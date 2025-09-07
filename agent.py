from pathlib import Path
import asyncio
import configparser
import json
import logging
import os
import socket
import sys
import time
from typing import Any, Dict, Optional, Set

import shutil
import subprocess

import docker
import psutil  # REQUIRED: auto-detect CPU/RAM
import pynvml
import requests
import websockets
from pyngrok import conf, ngrok

from aptos_sdk.account import Account
from aptos_sdk.async_client import ApiError, RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionArgument,
    TransactionPayload,
)

# --- Logging / Constants ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

AWS_METADATA_URL = "http://169.254.169.254/latest/meta-data/"
POLLING_INTERVAL_SECONDS = 15
PAYMENT_CLAIM_INTERVAL_SECONDS = 60
STATS_INTERVAL_SECONDS = 5  # how often to push GPU stats

# Base image used for the notebook session
PYTORCH_IMAGE = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"

DEFAULT_BACKEND_WS_URL = "ws://127.0.0.1:8000/ws"


class HostAgent:
    def __init__(self, config: configparser.ConfigParser):
        # --- ngrok (secure tunnel) ---
        self.active_tunnels: Dict[int, Any] = {}
        self._configure_ngrok(config)
        self._ngrok_preflight()

        # --- Chain / backend ---
        self.rest_client = RestClient(config["aptos"]["node_url"])
        self.host_account = Account.load_key(config["aptos"]["private_key"])
        self.contract_address = config["aptos"]["contract_address"]
        self.backend_ws_url = config.get(
            "aptos", "backend_ws_url", fallback=DEFAULT_BACKEND_WS_URL
        )

        # --- Host settings ---
        self.price_per_second = int(config["host"]["price_per_second"])

        # --- Runtime state ---
        self.active_jobs: Set[int] = set()
        self.active_containers: Dict[int, str] = {}  # job_id -> container_id
        self.docker_client: Optional[docker.DockerClient] = None
        self.transaction_lock = asyncio.Lock()

        # NEW: keep handles to background monitoring tasks
        self.monitoring_tasks: Dict[int, asyncio.Task] = {}

        logging.info(f"Host Agent loaded for account: {self.host_account.address()}")

    # ---------- helpers ----------
    @staticmethod
    def _looks_like_pyngrok_shim(path_str: str) -> bool:
        """
        Detect the pyngrok console-script shim on Windows (e.g., venv\\Scripts\\ngrok.exe).
        We don't want to treat that as the real ngrok binary.
        """
        if os.name != "nt":
            return False
        p = Path(path_str)
        try:
            scripts = p.parent
            # Heuristic: in a Python venv/conda, Scripts contains python.exe
            return scripts.name.lower() == "scripts" and (
                (scripts / "python.exe").exists()
                or (scripts.parent / "python.exe").exists()
            )
        except Exception:
            return False

    # ---------- ngrok helpers ----------
    def _configure_ngrok(self, config: configparser.ConfigParser) -> None:
        """
        Robust ngrok resolution:
        - Use [ngrok] path if it exists
        - Else use a real ngrok found on PATH (ignore pyngrok shim)
        - Else on Windows, try WinGet install
        - Else fall back to pyngrok auto-download into a per-user folder (set TMP there)
        """
        # 1) Auth token
        auth_token = config.get(
            "ngrok", "auth_token", fallback=os.getenv("NGROK_AUTHTOKEN", "")
        )
        if not auth_token:
            logging.error("❌ 'auth_token' missing in [ngrok] (or NGROK_AUTHTOKEN).")
            sys.exit(1)
        conf.get_default().auth_token = auth_token

        # 2) Per-user folder for last-resort download
        if os.name == "nt":
            base = Path(os.getenv("LOCALAPPDATA") or Path.home())
            ngrok_dir = base / "UnifiedCompute" / "ngrok"
            bin_name = "ngrok.exe"
        else:
            ngrok_dir = Path.home() / ".unified_compute" / "ngrok"
            bin_name = "ngrok"
        ngrok_dir.mkdir(parents=True, exist_ok=True)
        per_user_bin = ngrok_dir / bin_name
        conf.get_default().config_path = str(ngrok_dir / "ngrok.yml")

        # 3) Prefer explicit path if it exists
        explicit_path = config.get(
            "ngrok", "path", fallback=os.getenv("NGROK_PATH", "")
        )
        if explicit_path and Path(explicit_path).exists():
            conf.get_default().ngrok_path = str(Path(explicit_path))
            logging.info(f"✅ Using preinstalled ngrok at: {explicit_path}")
            return

        # 4) Then PATH (but skip pyngrok shim)
        found = shutil.which("ngrok")
        if found and not self._looks_like_pyngrok_shim(found):
            conf.get_default().ngrok_path = found
            logging.info(f"✅ Found ngrok on PATH: {found}")
            return
        elif found:
            logging.info(f"ℹ️ Found pyngrok shim on PATH, ignoring: {found}")

        # 5) Windows: try WinGet
        if os.name == "nt" and shutil.which("winget"):
            try:
                logging.info("📦 Installing ngrok via WinGet (silent)...")
                subprocess.run(
                    [
                        "winget",
                        "install",
                        "--id",
                        "Ngrok.Ngrok",
                        "--silent",
                        "--accept-package-agreements",
                        "--accept-source-agreements",
                    ],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                post = shutil.which("ngrok")
                if post and not self._looks_like_pyngrok_shim(post):
                    conf.get_default().ngrok_path = post
                    logging.info(f"✅ WinGet installed ngrok: {post}")
                    return
                else:
                    logging.info(
                        "ℹ️ WinGet installed ngrok but PATH still points to shim; continuing..."
                    )
            except Exception as e:
                logging.warning(f"WinGet install failed or unavailable: {e}")

        # 6) Last resort: pyngrok downloader into per-user dir
        conf.get_default().ngrok_path = str(per_user_bin)

        # Only now do we force TMP/TEMP into a safe folder for the download/unzip step
        os.environ["TMP"] = str(ngrok_dir)
        os.environ["TEMP"] = str(ngrok_dir)
        os.environ["TMPDIR"] = str(ngrok_dir)

        logging.info(f"ℹ️ Falling back to pyngrok auto-download into: {per_user_bin}")

    def _ngrok_preflight(self) -> None:
        """Verify ngrok binary works; only install if no binary is present."""
        ngrok_bin = Path(conf.get_default().ngrok_path or "")
        try:
            if (
                ngrok_bin
                and ngrok_bin.exists()
                and not self._looks_like_pyngrok_shim(str(ngrok_bin))
            ):
                # Just verify it runs; don't trigger installer
                proc = subprocess.run(
                    [str(ngrok_bin), "version"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    shell=False,
                )
                if proc.returncode == 0:
                    logging.info(
                        f"✅ ngrok preflight OK: {proc.stdout.strip() or 'version OK'}"
                    )
                    return
                else:
                    logging.warning(
                        f"ngrok exists but 'version' failed (rc={proc.returncode}). "
                        f"stderr: {proc.stderr.strip()}"
                    )

            # If no real binary yet, try to install into the configured path
            from pyngrok import installer

            installer.install_ngrok(
                conf.get_default().ngrok_path
            )  # will download if needed
            logging.info("✅ ngrok installed via pyngrok.")

            # Re-verify
            proc = subprocess.run(
                [str(conf.get_default().ngrok_path), "version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=False,
            )
            if proc.returncode == 0:
                logging.info(f"✅ ngrok version: {proc.stdout.strip() or 'OK'}")
                return
            raise RuntimeError(f"ngrok installed but unusable: {proc.stderr.strip()}")

        except Exception as e:
            logging.error(f"❌ ngrok preflight failed: {e}", exc_info=True)
            logging.error(
                "If this is a locked-down network, set [ngrok] path to an existing ngrok.exe."
            )
            sys.exit(1)

    # ---------- Docker helpers ----------
    def ensure_docker(self) -> None:
        """Initialize docker client and verify daemon is reachable."""
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
            logging.info("✅ Docker is running and accessible.")
        except Exception:
            logging.error("❌ Critical Error: Docker is not running or not installed.")
            sys.exit(1)

    def prepare_base_image(self) -> None:
        """Pull the base image if not present."""
        if self.docker_client is None:
            raise RuntimeError("Docker client not initialized")
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
                logging.error(
                    f"❌ Critical Error: Failed to pull Docker image: {e}",
                    exc_info=True,
                )
                sys.exit(1)

    def _get_free_port(self) -> int:
        """Ask OS for a free TCP port to avoid collisions."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    # ---------- CPU/RAM + environment detection ----------
    def _detect_cpu_ram(self) -> tuple[int, int]:
        """
        Returns (cpu_cores, ram_gb) using psutil (required).
        Uses physical cores when available; falls back to logical.
        """
        # Cores: prefer physical, fallback to logical
        cores = psutil.cpu_count(logical=False) or psutil.cpu_count(logical=True)
        if not cores:
            cores = os.cpu_count() or 1
        # Memory total (bytes) -> GB
        vm = psutil.virtual_memory()
        ram_gb = int(round(vm.total / (1024**3)))
        return int(max(1, cores)), int(max(1, ram_gb))

    def detect_environment_and_specs(self) -> Dict[str, Any]:
        """Detect cloud vs physical, GPU identifier, plus CPU/RAM (via psutil)."""
        logging.info("🔎 Detecting environment and hardware specifications...")

        # Always include CPU/RAM using psutil
        cpu_cores, ram_gb = self._detect_cpu_ram()

        # Check AWS first
        try:
            r = requests.get(f"{AWS_METADATA_URL}instance-type", timeout=1)
            if r.status_code == 200:
                return {
                    "is_physical": False,
                    "identifier": r.text,
                    "cpu_cores": cpu_cores,
                    "ram_gb": ram_gb,
                }
        except requests.exceptions.RequestException:
            logging.info("   - Not an AWS environment. Assuming physical machine.")

        # Physical GPU info
        try:
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            gpu_model = pynvml.nvmlDeviceGetName(handle)
            try:
                gpu_model = gpu_model.decode("utf-8")  # NVML may return bytes
            except Exception:
                gpu_model = str(gpu_model)
            logging.info(f"   - GPU Found: {gpu_model}")
            pynvml.nvmlShutdown()
            return {
                "is_physical": True,
                "identifier": gpu_model,
                "cpu_cores": cpu_cores,
                "ram_gb": ram_gb,
            }
        except pynvml.NVMLError:
            logging.error("❌ Critical Error: Could not detect an NVIDIA GPU.")
            sys.exit(1)

    # ---------- Live stats monitoring ----------
    async def _monitor_and_report_stats(self, job_id: int, websocket):
        """
        Background task: periodically reads GPU stats and sends them to backend.
        """
        logging.info(f"📊 Starting stats monitoring for Job ID: {job_id}")
        try:
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)  # single-GPU assumption
            while job_id in self.active_containers:
                try:
                    mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    stats = {
                        "gpu_utilization_percent": int(util.gpu),
                        "memory_used_mb": int(mem.used // (1024**2)),
                        "memory_total_mb": int(mem.total // (1024**2)),
                    }
                except pynvml.NVMLError as e:
                    logging.error(f"NVML read error (job {job_id}): {e}")
                    stats = None

                try:
                    await websocket.send(
                        json.dumps(
                            {
                                "status": "stats_update",
                                "job_id": job_id,
                                "stats": stats,
                            }
                        )
                    )
                except websockets.exceptions.ConnectionClosed:
                    logging.warning(
                        f"WebSocket closed while monitoring job {job_id}. Stopping stats."
                    )
                    break

                await asyncio.sleep(STATS_INTERVAL_SECONDS)
        except pynvml.NVMLError as e:
            logging.error(f"NVML init error while monitoring job {job_id}: {e}")
        finally:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass
            # Ensure we clear our handle from the bookkeeping
            self.monitoring_tasks.pop(job_id, None)
            logging.info(f"⏹️ Stopped stats monitoring for Job ID: {job_id}")

    # ---------- Container lifecycle ----------
    def _start_container(self, job_id: int, websocket) -> Optional[Dict[str, Any]]:
        """Starts a Jupyter container and creates an ngrok tunnel to it."""
        logging.info(f"🚀 Starting container for job ID: {job_id}...")
        try:
            if job_id in self.active_containers:
                logging.warning(f"Container for job {job_id} is already running.")
                return None

            if self.docker_client is None:
                raise RuntimeError("Docker client not initialized")

            # 1) Choose free host port + generate a token
            host_port = self._get_free_port()
            token = f"unified-{job_id}-{int(time.time())%10000}"

            # 2) Command: ensure notebook exists, then launch it
            jupyter_command = (
                "bash -lc "
                '"python -m pip install --no-cache-dir --upgrade pip && '
                "python -m pip install --no-cache-dir notebook jupyterlab && "
                f"python -m notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='{token}'\""
            )

            # 3) Run the container (use GPU if present; fall back if runtime flag not supported)
            try:
                container = self.docker_client.containers.run(
                    PYTORCH_IMAGE,
                    command=jupyter_command,
                    runtime="nvidia",
                    detach=True,
                    ports={"8888/tcp": host_port},
                    device_requests=[
                        docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])
                    ],
                    name=f"job-{job_id}-nb",
                    remove=True,
                )
            except Exception:
                container = self.docker_client.containers.run(
                    PYTORCH_IMAGE,
                    command=jupyter_command,
                    detach=True,
                    ports={"8888/tcp": host_port},
                    device_requests=[
                        docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])
                    ],
                    name=f"job-{job_id}-nb",
                    remove=True,
                )

            self.active_containers[job_id] = container.id
            logging.info(
                f"✅ Container {container.id[:12]} running on host port {host_port}"
            )

            # 4) Create ngrok tunnel to the host port
            logging.info(f"Creating secure tunnel for port {host_port}...")
            tunnel = ngrok.connect(addr=host_port, proto="http")  # HTTPS by default
            self.active_tunnels[job_id] = tunnel
            public_url = tunnel.public_url
            logging.info(f"✅ Secure tunnel created for job {job_id}: {public_url}")

            # 5) Launch background GPU monitor
            task = asyncio.create_task(
                self._monitor_and_report_stats(job_id, websocket)
            )
            self.monitoring_tasks[job_id] = task

            return {"public_url": public_url, "token": token}

        except Exception as e:
            logging.error(
                f"❌ Failed to start container/tunnel for job {job_id}: {e}",
                exc_info=True,
            )
            self._stop_container(job_id)
            return None

    def _stop_container(self, job_id: int) -> None:
        """Stops the monitor, ngrok tunnel, and the container for a job if they exist."""
        # Cancel stats monitor first
        mon = self.monitoring_tasks.pop(job_id, None)
        if mon:
            mon.cancel()
            logging.info(f"📊 Canceled stats monitoring task for job {job_id}.")

        # Stop ngrok
        tunnel = self.active_tunnels.pop(job_id, None)
        if tunnel:
            try:
                logging.info(f"🛑 Disconnecting ngrok tunnel: {tunnel.public_url}")
                ngrok.disconnect(tunnel.public_url)
            except Exception as e:
                logging.warning(f"ngrok disconnect warning: {e}")

        # Then stop/remove container
        container_id = self.active_containers.pop(job_id, None)
        if container_id and self.docker_client:
            try:
                c = self.docker_client.containers.get(container_id)
                logging.info(f"🛑 Stopping container {container_id[:12]}")
                c.stop(timeout=5)
            except docker.errors.NotFound:
                logging.warning(
                    f"Container {container_id[:12]} not found (already removed)."
                )
            except Exception as e:
                logging.warning(f"Container stop warning: {e}")

    # ---------- Env/chain helpers ----------
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

    async def register_on_chain_if_needed(self, specs: Dict[str, Any]) -> None:
        """Register host machine if nothing is listed yet."""
        existing = await self.get_on_chain_listings()
        if existing:
            logging.info("Host already has listing(s) on-chain. Skipping registration.")
            return

        if specs["is_physical"]:
            function_name = "list_physical_machine"
            cpu_cores = int(specs.get("cpu_cores", 1))
            ram_gb = int(specs.get("ram_gb", 1))
            arguments = [
                TransactionArgument(specs["identifier"], Serializer.str),  # gpu_model
                TransactionArgument(cpu_cores, Serializer.u64),
                TransactionArgument(ram_gb, Serializer.u64),
                TransactionArgument(self.price_per_second, Serializer.u64),
                TransactionArgument(
                    self.host_account.public_key().to_bytes(), Serializer.to_bytes
                ),
            ]
        else:
            function_name = "list_cloud_machine"
            arguments = [
                TransactionArgument(
                    specs["identifier"], Serializer.str
                ),  # instance_type
                TransactionArgument(self.price_per_second, Serializer.u64),
                TransactionArgument(
                    self.host_account.public_key().to_bytes(), Serializer.to_bytes
                ),
            ]

        logging.info(
            f"🔗 Registering '{specs['identifier']}' on-chain using {function_name}..."
        )
        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace", function_name, [], arguments
            )
        )
        await self.submit_transaction(
            payload, f"✅ Successfully listed '{specs['identifier']}' on-chain!"
        )

    # ---------- Claim loop ----------
    async def claim_payment_for_job(self, job_id: int) -> None:
        """Periodically claim accrued payment for an active job until it ends."""
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
                    logging.warning(
                        f"Job {job_id} has non-positive duration; stopping."
                    )
                    self.active_jobs.discard(job_id)
                    return

                now = int(time.time())
                claim_timestamp = min(max(now, start_time), max_end_time)

                price_per_second = total_escrow_amount // duration
                accrued = max(0, claim_timestamp - start_time) * price_per_second
                if accrued > total_escrow_amount:
                    accrued = total_escrow_amount

                if accrued <= claimed_amount:
                    if claim_timestamp >= max_end_time:
                        logging.info(
                            f"Job {job_id}: fully claimed or at end; stopping claims."
                        )
                        self.active_jobs.discard(job_id)
                        return
                    logging.info(f"Job {job_id}: nothing new to claim yet.")
                    await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)
                    continue

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
                self.active_jobs.discard(job_id)
                return
            except Exception:
                logging.error(
                    f"Generic error claiming payment for Job ID {job_id}", exc_info=True
                )
                self.active_jobs.discard(job_id)
                return

            await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)

    # ---------- Orchestration ----------
    async def submit_transaction(
        self, payload: TransactionPayload, success_message: str
    ) -> None:
        """Sign, submit and wait for a transaction; serialize to avoid seq# races."""
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

    async def poll_for_jobs(self) -> None:
        """Polls on-chain listings and starts claimers for active jobs."""
        while True:
            logging.info("Polling for new rentals...")
            try:
                on_chain_listings = await self.get_on_chain_listings()
                for _, listing in on_chain_listings.items():
                    if not listing.get("is_available", True):
                        job_id_vec = listing.get("active_job_id", {}).get("vec", [])
                        if job_id_vec:
                            try:
                                job_id = int(job_id_vec[0])
                            except Exception:
                                logging.warning(
                                    f"Could not parse job_id from {job_id_vec}"
                                )
                                continue
                            if job_id not in self.active_jobs:
                                logging.info(
                                    f"🎉 New rental detected! Starting payment processor for Job ID: {job_id}"
                                )
                                self.active_jobs.add(job_id)
                                asyncio.create_task(self.claim_payment_for_job(job_id))
            except Exception as e:
                logging.error(
                    f"An error occurred during polling loop: {e}", exc_info=True
                )

            await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    async def listen_for_commands(self) -> None:
        """Connect to backend WS and handle start/stop session commands."""
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
                            logging.warning(
                                f"Received non-JSON message: {message_raw!r}"
                            )
                            continue

                        logging.info(f"⬇️ Received command from backend: {command}")
                        action = command.get("action")
                        job_id = command.get("job_id")

                        if job_id is None:
                            continue
                        if isinstance(job_id, str):
                            try:
                                job_id = int(job_id)
                            except ValueError:
                                logging.warning(
                                    f"Ignoring command with non-int job_id: {job_id!r}"
                                )
                                continue

                        if action == "start_session":
                            # pass websocket so monitor can stream stats
                            details = self._start_container(job_id, websocket)
                            resp = {
                                "status": (
                                    "session_ready" if details else "session_error"
                                ),
                                "job_id": job_id,
                            }
                            if details:
                                # Send secure ngrok URL instead of raw IP/port
                                resp.update(
                                    {
                                        "public_url": details["public_url"],
                                        "token": details["token"],
                                    }
                                )
                            await websocket.send(json.dumps(resp))
                            logging.info(
                                f"⬆️ Sent session details for job {job_id} to backend."
                            )

                        elif action == "stop_session":
                            self._stop_container(job_id)
                            resp = {"status": "session_stopped", "job_id": job_id}
                            await websocket.send(json.dumps(resp))
                            logging.info(
                                f"⬆️ Sent session stopped confirmation for job {job_id}."
                            )

                        else:
                            logging.warning(f"Unknown action: {action}")

            except (
                websockets.exceptions.ConnectionClosed,
                ConnectionRefusedError,
            ) as e:
                logging.warning(
                    f"⚠️ WebSocket connection issue: {e}. Retrying in 10 seconds..."
                )
                await asyncio.sleep(10)
            except Exception as e:
                logging.error(
                    f"Unexpected error in WebSocket listener: {e}", exc_info=True
                )
                await asyncio.sleep(10)

    async def run(self) -> None:
        logging.info("--- 🚀 Starting Unified Compute Host Agent ---")
        self.ensure_docker()
        specs = self.detect_environment_and_specs()
        self.prepare_base_image()
        await self.register_on_chain_if_needed(specs)

        logging.info(
            "\n✅ Setup complete. Starting job polling and command listener..."
        )

        polling_task = asyncio.create_task(self.poll_for_jobs())
        command_listener_task = asyncio.create_task(self.listen_for_commands())
        await asyncio.gather(polling_task, command_listener_task)


# ---------- Entrypoint ----------
async def main():
    logging.info("Loading configuration from config.ini...")
    config = configparser.ConfigParser()
    try:
        config.read_file(open("config.ini"))
        # Required keys sanity check
        _ = config["aptos"]["private_key"]
        _ = config["aptos"]["contract_address"]
        _ = config["aptos"]["node_url"]
        _ = config["host"]["price_per_second"]
        # ngrok token is validated inside HostAgent.__init__
    except (KeyError, FileNotFoundError) as e:
        logging.error(
            f"Configuration error in 'config.ini': {e}. Please ensure the file exists and has required keys."
        )
        return

        # Optional: fail fast if psutil missing (it’s required now)
    agent = HostAgent(config)
    await agent.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nAgent shut down by user.")
