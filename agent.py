import logging
import asyncio
import configparser
import requests
import pynvml
import psutil
import time
import sys
import docker
import json
from typing import Dict, Any, Set

from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient, ApiError
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionPayload,
    TransactionArgument,
)
from aptos_sdk.bcs import Serializer

# --- Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
AWS_METADATA_URL = "http://169.254.169.254/latest/meta-data/"
POLLING_INTERVAL_SECONDS = 15
PAYMENT_CLAIM_INTERVAL_SECONDS = 60
PYTORCH_IMAGE = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"


class HostAgent:
    def __init__(self, config):
        self.rest_client = RestClient(config["aptos"]["node_url"])
        self.host_account = Account.load_key(config["aptos"]["private_key"])
        self.contract_address = config["aptos"]["contract_address"]
        self.price_per_second = int(config["host"]["price_per_second"])
        self.active_jobs: Set[int] = set()
        self.docker_client = None
        # Serialize submissions from this account to avoid mempool sequence races
        self.transaction_lock = asyncio.Lock()
        logging.info(f"Host Agent loaded for account: {self.host_account.address()}")

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
            self.docker_client.images.get(PYTORCH_IMAGE)
            logging.info("   - Image already exists locally.")
        except docker.errors.ImageNotFound:
            logging.info("   - Image not found. Pulling from Docker Hub...")
            try:
                self.docker_client.images.pull(PYTORCH_IMAGE)
                logging.info("✅ Successfully pulled PyTorch base image.")
            except Exception as e:
                logging.error(f"❌ Critical Error: Failed to pull Docker image: {e}")
                sys.exit(1)

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
            response = await self.rest_client.account_resource(
                str(self.host_account.address()), resource_type
            )
            listings_data = response.get("data", {}).get("listings", [])
            return {int(l["id"]): l for l in listings_data}
        except Exception:
            return {}

    async def register_on_chain_if_needed(self, specs: Dict[str, Any]):
        if await self.get_on_chain_listings():
            logging.info("Host already has listing(s) on-chain. Skipping registration.")
            return

        logging.info(
            f"🔗 No listings found. Registering '{specs['identifier']}' on the blockchain..."
        )

        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace",
                "list_machine",
                [],
                [
                    TransactionArgument(specs["is_physical"], Serializer.bool),
                    TransactionArgument(specs["identifier"], Serializer.str),
                    TransactionArgument(self.price_per_second, Serializer.u64),
                    # public key as bytes (matches vector<u8> on-chain)
                    TransactionArgument(
                        self.host_account.public_key().to_bytes(), Serializer.to_bytes
                    ),
                ],
            )
        )
        await self.submit_transaction(
            payload, f"✅ Successfully listed '{specs['identifier']}' on-chain!"
        )

    async def claim_payment_for_job(self, job_id: int):
        # Claim loop: re-fetch job each time so we use fresh on-chain values
        while job_id in self.active_jobs:
            logging.info(f"Attempting to claim payment for active Job ID: {job_id}")

            try:
                raw = await self.rest_client.view(
                    function=f"{self.contract_address}::escrow::get_job",
                    type_arguments=[],
                    arguments=[str(job_id)],
                )
                parsed = json.loads(raw.decode("utf-8"))
                logging.info(
                    f"DEBUG: Parsed response from get_job for Job ID {job_id}: {parsed}"
                )
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

                # Choose a valid claim timestamp within [start_time, max_end_time]
                now = int(time.time())
                claim_timestamp = min(max(now, start_time), max_end_time)

                # Optional pre-check mirrors Move logic to avoid no-op/abort submissions
                price_per_second = total_escrow_amount // duration
                accrued = max(0, claim_timestamp - start_time) * price_per_second
                if accrued > total_escrow_amount:
                    accrued = total_escrow_amount

                if accrued <= claimed_amount:
                    # Nothing new to claim yet; if we've reached the end, stop
                    if claim_timestamp >= max_end_time:
                        logging.info(
                            f"Job {job_id}: fully claimed or at end; stopping claims."
                        )
                        self.active_jobs.discard(job_id)
                        return
                    logging.info(f"Job {job_id}: nothing new to claim yet.")
                    await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)
                    continue

                # Build the payload with EXACTLY TWO args per Move ABI: (job_id, claim_timestamp)
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

                await self.submit_transaction(
                    payload, f"Successfully claimed payment for Job ID {job_id}"
                )

            except ApiError as e:
                logging.error(
                    f"API Error claiming payment for Job ID {job_id}: {e}",
                    exc_info=True,
                )
                # Stop trying on API errors; you can choose to retry if desired
                self.active_jobs.discard(job_id)
                return
            except Exception:
                logging.error(
                    f"Generic error claiming payment for Job ID {job_id}",
                    exc_info=True,
                )
                self.active_jobs.discard(job_id)
                return

            await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)

    async def run(self):
        logging.info("--- 🚀 Starting Unified Compute Host Agent ---")
        self.ensure_docker()
        specs = self.detect_environment_and_specs()
        self.prepare_base_image()
        await self.register_on_chain_if_needed(specs)

        logging.info("\n✅ Setup complete. Agent is now monitoring for jobs...")
        while True:
            logging.info("Polling for new rentals...")
            try:
                on_chain_listings = await self.get_on_chain_listings()
                for _, listing in on_chain_listings.items():
                    if not listing["is_available"]:
                        job_id_vec = listing.get("active_job_id", {}).get("vec", [])
                        if job_id_vec:
                            job_id = int(job_id_vec[0])
                            if job_id not in self.active_jobs:
                                logging.info(
                                    f"🎉 New rental detected! Starting payment processor for Job ID: {job_id}"
                                )
                                self.active_jobs.add(job_id)
                                asyncio.create_task(
                                    self.claim_payment_for_job(job_id)
                                )
            except Exception as e:
                logging.error(
                    f"An error occurred during polling loop: {e}", exc_info=True
                )
            await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    async def submit_transaction(
        self, payload: TransactionPayload, success_message: str
    ):
        # Serialize sign->submit->wait to avoid sequence collisions
        async with self.transaction_lock:
            try:
                signed_txn = await self.rest_client.create_bcs_signed_transaction(
                    self.host_account, payload
                )
                tx_hash = await self.rest_client.submit_bcs_transaction(signed_txn)
                await self.rest_client.wait_for_transaction(tx_hash)
                logging.info(f"{success_message} | Transaction: {tx_hash}")
            except Exception as e:
                logging.error(f"Transaction submission failed: {e}")
                raise


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
        logging.error(
            f"Configuration error in 'config.ini': {e}. Please ensure the file exists."
        )
        return

    agent = HostAgent(config)
    await agent.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nAgent shut down by user.")
