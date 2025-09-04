import logging
import asyncio
import configparser
import requests
import pynvml
import time
import sys
import docker
import json
import websockets
import socket
from typing import Dict, Any, Set, Optional

from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient, ApiError
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionPayload,
    TransactionArgument,
)
from aptos_sdk.bcs import Serializer

# --- Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
AWS_METADATA_URL = "http://169.254.169.254/latest/meta-data/"
POLLING_INTERVAL_SECONDS = 15
PAYMENT_CLAIM_INTERVAL_SECONDS = 60
PYTORCH_IMAGE = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"
DEFAULT_BACKEND_WS_URL = "ws://127.0.0.1:8000/ws"


class HostAgent:
    def __init__(self, config: configparser.ConfigParser):
        self.rest_client = RestClient(config["aptos"]["node_url"])
        self.host_account = Account.load_key(config["aptos"]["private_key"])
        self.contract_address = config["aptos"]["contract_address"]
        self.backend_ws_url = config.get("aptos", "backend_ws_url", fallback=DEFAULT_BACKEND_WS_URL)

        self.price_per_second = int(config["host"]["price_per_second"])
        self.active_jobs: Set[int] = set()
        self.docker_client: Optional[docker.DockerClient] = None
        self.transaction_lock = asyncio.Lock()

        # Track running containers by job_id
        self.running_containers: Dict[int, Any] = {}

        logging.info(f"Host Agent loaded for account: {self.host_account.address()}")

    # ---------- Docker helpers ----------

    def _get_free_port(self) -> int:
        """Ask OS for a free TCP port to avoid collisions."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def _start_container(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Starts the PyTorch Docker container and returns connection details."""
        logging.info(f"🚀 Starting container for job ID: {job_id}...")
        try:
            if job_id in self.running_containers:
                logging.warning(f"Container for job {job_id} is already running.")
                return None

            port = self._get_free_port()
            token = f"unified-compute-token-{job_id}-{int(time.time()) % 10000}"

            jupyter_command = (
                "jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser "
                f"--allow-root --NotebookApp.token='{token}'"
            )

            # Try with runtime + device requests; fall back if daemon doesn't support runtime
            try:
                container = self.docker_client.containers.run(
                    PYTORCH_IMAGE,
                    command=jupyter_command,
                    runtime="nvidia",
                    detach=True,
                    ports={"8888/tcp": port},
                    device_requests=[docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])],
                )
            except Exception:
                container = self.docker_client.containers.run(
                    PYTORCH_IMAGE,
                    command=jupyter_command,
                    detach=True,
                    ports={"8888/tcp": port},
                    device_requests=[docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])],
                )

            self.running_containers[job_id] = container
            logging.info(f"✅ Container for job {job_id} started. Mapped host port: {port}")
            return {"port": port, "token": token}
        except Exception as e:
            logging.error(f"❌ Docker failed to start container for job {job_id}: {e}", exc_info=True)
            return None

    def _stop_container(self, job_id: int):
        """Stops and removes the Docker container for a given job_id."""
        logging.info(f"🛑 Stopping container for job ID: {job_id}...")
        if job_id not in self.running_containers:
            logging.warning(f"⚠️ No running container found for job {job_id} to stop.")
            return
        container = self.running_containers[job_id]
        try:
            container.stop()
            container.remove()
            logging.info(f"✅ Container for job {job_id} stopped and removed.")
        except docker.errors.NotFound:
            logging.warning(f"Container for job {job_id} was already removed.")
        except Exception as e:
            logging.error(f"❌ Failed to stop/remove container for job {job_id}: {e}", exc_info=True)
        finally:
            self.running_containers.pop(job_id, None)

    def ensure_docker(self):
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
            logging.info("✅ Docker is running and accessible.")
        except Exception:
            logging.error("❌ Critical Error: Docker is not running or not installed.")
            sys.exit(1)

    def prepare_base_image(self):
        logging.info(f"🐳 Checking for Docker image: {PYTORCH_IMAGE}...")
        try:
            assert self.docker_client is not None
            self.docker_client.images.get(PYTORCH_IMAGE)
            logging.info("   - Image already exists locally.")
        except docker.errors.ImageNotFound:
            logging.info("   - Image not found. Pulling from Docker Hub...")
            try:
                self.docker_client.images.pull(PYTORCH_IMAGE)
                logging.info("✅ Successfully pulled PyTorch base image.")
            except Exception as e:
                logging.error(f"❌ Critical Error: Failed to pull Docker image: {e}", exc_info=True)
                sys.exit(1)

    # ---------- Env/chain helpers ----------

    def detect_environment_and_specs(self) -> Dict[str, Any]:
        logging.info("🔎 Detecting environment and hardware specifications...")
        try:
            r = requests.get(f"{AWS_METADATA_URL}instance-type", timeout=1)
            if r.status_code == 200:
                return {"is_physical": False, "identifier": r.text}
        except requests.exceptions.RequestException:
            logging.info("   - Not an AWS environment. Assuming physical machine.")

        try:
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            gpu_model = pynvml.nvmlDeviceGetName(handle)
            logging.info(f"   - GPU Found: {gpu_model}")
            pynvml.nvmlShutdown()
            return {"is_physical": True, "identifier": gpu_model}
        except pynvml.NVMLError:
            logging.error("❌ Critical Error: Could not detect an NVIDIA GPU.")
            sys.exit(1)

    async def get_on_chain_listings(self) -> Dict[int, Any]:
        try:
            resource_type = f"{self.contract_address}::marketplace::ListingManager"
            response = await self.rest_client.account_resource(str(self.host_account.address()), resource_type)
            listings_data = response.get("data", {}).get("listings", [])
            return {int(l["id"]): l for l in listings_data}
        except Exception:
            return {}

    async def register_on_chain_if_needed(self, specs: Dict[str, Any]):
        if await self.get_on_chain_listings():
            logging.info("Host already has listing(s) on-chain. Skipping registration.")
            return

        logging.info(f"🔗 No listings found. Registering '{specs['identifier']}' on the blockchain...")

        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace",
                "list_machine",
                [],
                [
                    TransactionArgument(specs["is_physical"], Serializer.bool),
                    TransactionArgument(specs["identifier"], Serializer.str),
                    TransactionArgument(self.price_per_second, Serializer.u64),
                    # public key as bytes (vector<u8> on-chain)
                    TransactionArgument(self.host_account.public_key().to_bytes(), Serializer.to_bytes),
                ],
            )
        )
        await self.submit_transaction(payload, f"✅ Successfully listed '{specs['identifier']}' on-chain!")

    # ---------- Claim loop ----------

    async def claim_payment_for_job(self, job_id: int):
        """Periodically claims any newly accrued payment for a job."""
        while job_id in self.active_jobs:
            logging.info(f"Attempting to claim payment for active Job ID: {job_id}")
            try:
                raw = await self.rest_client.view(
                    function=f"{self.contract_address}::escrow::get_job",
                    type_arguments=[],
                    arguments=[str(job_id)],
                )
                parsed = json.loads(raw.decode("utf-8"))
                logging.info(f"DEBUG: Parsed response from get_job for Job ID {job_id}: {parsed}")
                if not parsed or not isinstance(parsed[0], dict):
                    logging.error("Unexpected data format from get_job.")
                    self.active_jobs.discard(job_id)
                    return

                job = parsed[0]
                is_active = bool(job.get("is_active", False))
                start_time = int(job["start_time"])
                max_end_time = int(job["max_end_time"])
                total_escrow_amount = int(job["total_escrow_amount"])
                claimed_amount = int(job["claimed_amount"])

                if not is_active:
                    logging.info(f"Job {job_id} is inactive; stopping claims.")
                    self.active_jobs.discard(job_id)
                    return

                duration = max_end_time - start_time
                if duration <= 0:
                    logging.warning(f"Job {job_id} has non-positive duration; stopping.")
                    self.active_jobs.discard(job_id)
                    return

                # Valid timestamp within [start_time, max_end_time]
                now = int(time.time())
                claim_timestamp = min(max(now, start_time), max_end_time)

                # Pre-check mirrors Move logic to avoid aborts
                price_per_second = total_escrow_amount // duration
                accrued = max(0, claim_timestamp - start_time) * price_per_second
                if accrued > total_escrow_amount:
                    accrued = total_escrow_amount

                if accrued <= claimed_amount:
                    if claim_timestamp >= max_end_time:
                        logging.info(f"Job {job_id}: fully claimed or at end; stopping claims.")
                        self.active_jobs.discard(job_id)
                        return
                    logging.info(f"Job {job_id}: nothing new to claim yet.")
                    await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)
                    continue

                # EXACT Move ABI: claim_payment(host: &signer, job_id: u64, claim_timestamp: u64)
                payload = TransactionPayload(
                    EntryFunction.natural(
                        f"{self.contract_address}::escrow",
                        "claim_payment",
                        [],
                        [
                            TransactionArgument(job_id, Serializer.u64),
                            TransactionArgument(claim_timestamp, Serializer.u64),
                        ],
                    )
                )
                await self.submit_transaction(payload, f"Successfully claimed payment for Job ID {job_id}")

            except ApiError as e:
                logging.error(f"API Error claiming payment for Job ID {job_id}: {e}", exc_info=True)
                self.active_jobs.discard(job_id)
                return
            except Exception:
                logging.error(f"Generic error claiming payment for Job ID {job_id}", exc_info=True)
                self.active_jobs.discard(job_id)
                return

            await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)

    # ---------- Main run & loops ----------

    async def run(self):
        logging.info("--- 🚀 Starting Unified Compute Host Agent ---")
        self.ensure_docker()
        specs = self.detect_environment_and_specs()
        self.prepare_base_image()
        await self.register_on_chain_if_needed(specs)

        logging.info("\n✅ Setup complete. Starting job polling and command listener...")

        polling_task = asyncio.create_task(self.poll_for_jobs())
        command_listener_task = asyncio.create_task(self.listen_for_commands())
        await asyncio.gather(polling_task, command_listener_task)

    async def poll_for_jobs(self):
        """Polls on-chain listings and starts claimers for active jobs."""
        while True:
            logging.info("Polling for new rentals...")
            try:
                on_chain_listings = await self.get_on_chain_listings()
                for _, listing in on_chain_listings.items():
                    if not listing.get("is_available", True):
                        job_id_vec = listing.get("active_job_id", {}).get("vec", [])
                        if job_id_vec:
                            # Elements may come as strings; ensure int
                            try:
                                job_id = int(job_id_vec[0])
                            except Exception:
                                logging.warning(f"Could not parse job_id from {job_id_vec}")
                                continue
                            if job_id not in self.active_jobs:
                                logging.info(f"🎉 New rental detected! Starting payment processor for Job ID: {job_id}")
                                self.active_jobs.add(job_id)
                                asyncio.create_task(self.claim_payment_for_job(job_id))
            except Exception as e:
                logging.error(f"An error occurred during polling loop: {e}", exc_info=True)

            await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    async def submit_transaction(self, payload: TransactionPayload, success_message: str):
        """Signs, submits and waits for a transaction; serialized to avoid seq# races."""
        async with self.transaction_lock:
            try:
                signed_txn = await self.rest_client.create_bcs_signed_transaction(self.host_account, payload)
                tx_hash = await self.rest_client.submit_bcs_transaction(signed_txn)
                await self.rest_client.wait_for_transaction(tx_hash)
                logging.info(f"{success_message} | Transaction: {tx_hash}")
            except Exception as e:
                logging.error(f"Transaction submission failed: {e}")
                raise

    # ---------- WebSocket command loop ----------

    async def listen_for_commands(self):
        """Connects to the backend WebSocket and listens for start/stop session commands."""
        addr_str = str(self.host_account.address())
        uri = f"{self.backend_ws_url}/{addr_str}"
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    logging.info(f"✅ WebSocket connected to backend at {uri}")
                    while True:
                        message_raw = await websocket.recv()
                        try:
                            command = json.loads(message_raw)
                        except json.JSONDecodeError:
                            logging.warning(f"Received non-JSON message: {message_raw!r}")
                            continue

                        logging.info(f"⬇️ Received command from backend: {command}")
                        action = command.get("action")
                        job_id = command.get("job_id")

                        # IMPORTANT: job_id can be 0; only skip if it's missing entirely
                        if job_id is None:
                            continue
                        if isinstance(job_id, str):
                            try:
                                job_id = int(job_id)
                            except ValueError:
                                logging.warning(f"Ignoring command with non-int job_id: {job_id!r}")
                                continue

                        if action == "start_session":
                            details = self._start_container(job_id)
                            response = {
                                "status": "session_ready" if details else "session_error",
                                "job_id": job_id,
                            }
                            if details:
                                response.update({"port": details["port"], "token": details["token"]})
                            await websocket.send(json.dumps(response))
                            logging.info(f"⬆️ Sent session details for job {job_id} to backend.")

                        elif action == "stop_session":
                            self._stop_container(job_id)
                            response = {"status": "session_stopped", "job_id": job_id}
                            await websocket.send(json.dumps(response))
                            logging.info(f"⬆️ Sent session stopped confirmation for job {job_id}.")

                        else:
                            logging.warning(f"Unknown action: {action}")

            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
                logging.warning(f"⚠️ WebSocket connection issue: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)
            except Exception as e:
                logging.error(f"Unexpected error in WebSocket listener: {e}", exc_info=True)
                await asyncio.sleep(10)


async def main():
    logging.info("Loading configuration from config.ini...")
    config = configparser.ConfigParser()
    try:
        config.read_file(open("config.ini"))
        _ = config["aptos"]["private_key"]
        _ = config["aptos"]["contract_address"]
        _ = config["aptos"]["node_url"]
        _ = config["host"]["price_per_second"]
    except (KeyError, FileNotFoundError) as e:
        logging.error(f"Configuration error in 'config.ini': {e}. Please ensure the file exists.")
        return

    agent = HostAgent(config)
    await agent.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nAgent shut down by user.")
