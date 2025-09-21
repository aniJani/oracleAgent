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
import secrets
import re
from urllib.parse import quote

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
import atexit
import signal

STATE_FILE = "agent_state.json"
CONTAINER_NAME_PREFIX = "uc-job-"

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

# Default Cloudflared path for Windows (yours). Can be overridden by config or env.
DEFAULT_CLOUDFLARED_PATH = r"C:\Program Files (x86)\cloudflared\cloudflared.exe"

#####################################################################
# Key points:
# - Prefer Cloudflare Quick Tunnels. Use explicit path if provided.
# - Only fall back to ngrok if cloudflared isn't available anywhere.
# - Wait for Jupyter locally BEFORE exposing public tunnel.
# - Randomize Jupyter base_url; keep token-based auth.
# - Raise shm_size to 1g to reduce kernel crashes.
#####################################################################


def _with_basic_auth(url: str, user: str, pwd: str) -> str:
    """Embed basic-auth credentials into a URL (scheme://user:pwd@host/path)."""
    scheme_sep = "://"
    if scheme_sep not in url:
        return url
    scheme, rest = url.split(scheme_sep, 1)
    return f"{scheme}{scheme_sep}{quote(user)}:{quote(pwd)}@{rest}"


class HostAgent:
    def __init__(self, config: configparser.ConfigParser):
        # --- tunnel provider selection ---
        self.tunnel_provider = (
            config.get(
                "tunnel",
                "provider",
                fallback=os.getenv("TUNNEL_PROVIDER", "cloudflare"),
            )
            .strip()
            .lower()
        )

        # Resolve cloudflared path preference:
        explicit_cf = config.get(
            "tunnel",
            "cloudflared_path",
            fallback=os.getenv("CLOUDFLARED_PATH", DEFAULT_CLOUDFLARED_PATH),
        ).strip()
        self.cloudflared_path: Optional[str] = explicit_cf if explicit_cf else None
        if self.cloudflared_path and not Path(self.cloudflared_path).exists():
            # If explicit path missing, try PATH lookup
            found = shutil.which("cloudflared")
            if found:
                self.cloudflared_path = found
            else:
                self.cloudflared_path = None

        # Runtime tunnel registry (per job_id)
        # For cloudflare: {"provider":"cloudflare","proc":Popen,"public_url":str}
        # For ngrok:      {"provider":"ngrok","tunnel":NgrokTunnel,"public_url":str}
        self.active_tunnels: Dict[int, Dict[str, Any]] = {}

        # Configure ngrok only if chosen or needed as fallback later
        if self.tunnel_provider == "ngrok":
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

        # background monitoring tasks
        self.monitoring_tasks: Dict[int, asyncio.Task] = {}

        # track per-job session start timestamp (for final_session_duration)
        self.session_start_times: Dict[int, float] = {}  # job_id -> start_timestamp

        # track per-job token so we can re-announce on resume
        self._tokens: Dict[int, str] = {}

        # track per-job Jupyter base_url prefix (for resume)
        self._base_prefix: Dict[int, str] = {}

        logging.info(f"Host Agent loaded for account: {self.host_account.address()}")

        # graceful shutdown hooks
        atexit.register(self.shutdown)
        try:
            signal.signal(signal.SIGTERM, self._handle_signal)
            signal.signal(signal.SIGINT, self._handle_signal)
        except Exception:
            pass

        # Auto-fallback to ngrok if Cloudflare was requested but unavailable
        if self.tunnel_provider == "cloudflare" and not self._cloudflared_available():
            logging.warning("cloudflared not available; falling back to ngrok.")
            self.tunnel_provider = "ngrok"
            self._configure_ngrok(config)
            self._ngrok_preflight()
        elif self.tunnel_provider == "cloudflare":
            logging.info(f"✅ Using cloudflared at: {self.cloudflared_path}")

    # ---------- signal / shutdown ----------
    def _handle_signal(self, signum, frame):
        logging.warning(
            f"Received signal {getattr(signal, 'Signals', lambda x: x)(signum)}; shutting down…"
        )
        try:
            sys.exit(0)
        except SystemExit:
            pass

    def shutdown(self):
        """Best-effort cleanup. Safe to call multiple times."""
        logging.info("🛑 Graceful shutdown: closing tunnels, stopping containers.")
        # Stop monitors first
        for job_id, task in list(self.monitoring_tasks.items()):
            try:
                task.cancel()
            except Exception:
                pass
            self.monitoring_tasks.pop(job_id, None)

        # Close tunnels
        for job_id, t in list(self.active_tunnels.items()):
            try:
                if t.get("provider") == "cloudflare":
                    try:
                        proc = t.get("proc")
                        if proc and proc.poll() is None:
                            proc.terminate()
                    except Exception:
                        pass
                elif t.get("provider") == "ngrok":
                    try:
                        url = t.get("public_url")
                        if url:
                            ngrok.disconnect(url)
                    except Exception:
                        pass
            finally:
                self.active_tunnels.pop(job_id, None)

        # Stop containers
        for job_id in list(self.active_containers.keys()):
            try:
                self._stop_container(job_id)
            except Exception:
                pass

        # Final save (should be empty if we stopped all)
        self._save_state()
        logging.info("✅ Shutdown complete.")

    # ---------- helpers ----------
    @staticmethod
    def _looks_like_pyngrok_shim(path_str: str) -> bool:
        if os.name != "nt":
            return False
        p = Path(path_str)
        try:
            scripts = p.parent
            return scripts.name.lower() == "scripts" and (
                (scripts / "python.exe").exists()
                or (scripts.parent / "python.exe").exists()
            )
        except Exception:
            return False

    # Local readiness probe for Jupyter
    def _wait_for_jupyter(self, host_port: int, timeout: int = 240) -> bool:
        """
        Poll http://127.0.0.1:<host_port>/ until Jupyter responds (<500).
        Probe locally (not via tunnel) to avoid public RPM limits.
        """
        url = f"http://127.0.0.1:{host_port}/"
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = requests.get(url, timeout=2)
                if r.status_code < 500:
                    return True
            except requests.RequestException:
                pass
            time.sleep(1)
        return False

    # ---------- cloudflared helpers ----------
    def _cloudflared_available(self) -> bool:
        return bool(
            (self.cloudflared_path and Path(self.cloudflared_path).exists())
            or shutil.which("cloudflared")
        )

    def _start_cloudflared(self, host_port: int) -> tuple[str, subprocess.Popen]:
        """
        Start a Cloudflare quick tunnel to http://127.0.0.1:<host_port>.
        Returns (public_url, process). Raises on failure.
        """
        bin_path = (
            self.cloudflared_path
            if (self.cloudflared_path and Path(self.cloudflared_path).exists())
            else "cloudflared"
        )
        cmd = [
            bin_path,
            "tunnel",
            "--no-autoupdate",
            "--url",
            f"http://127.0.0.1:{host_port}",
            "--loglevel",
            "info",
        ]
        logging.info(
            f"Starting cloudflared: {cmd[0]} tunnel --url http://127.0.0.1:{host_port}"
        )

        creationflags = (
            subprocess.CREATE_NO_WINDOW
            if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW")
            else 0
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            creationflags=creationflags,
        )

        deadline = time.time() + 30
        url = None
        pattern = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com", re.I)

        # Read lines until the URL appears or we time out
        if proc.stdout:
            while time.time() < deadline and proc.poll() is None:
                line = proc.stdout.readline()
                if not line:
                    time.sleep(0.05)
                    continue
                m = pattern.search(line)
                if m:
                    url = m.group(0)
                    break

        if not url:
            try:
                proc.terminate()
            except Exception:
                pass
            raise RuntimeError("cloudflared did not output a public URL in time")

        return url, proc

    # ---------- ngrok helpers ----------
    def _configure_ngrok(self, config: configparser.ConfigParser) -> None:
        auth_token = config.get(
            "ngrok", "auth_token", fallback=os.getenv("NGROK_AUTHTOKEN", "")
        )
        if not auth_token:
            logging.error("❌ 'auth_token' missing in [ngrok] (or NGROK_AUTHTOKEN).")
            sys.exit(1)
        conf.get_default().auth_token = auth_token

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

        explicit_path = config.get(
            "ngrok", "path", fallback=os.getenv("NGROK_PATH", "")
        )
        if explicit_path and Path(explicit_path).exists():
            conf.get_default().ngrok_path = str(Path(explicit_path))
            logging.info(f"✅ Using preinstalled ngrok at: {explicit_path}")
            return

        found = shutil.which("ngrok")
        if found and not self._looks_like_pyngrok_shim(found):
            conf.get_default().ngrok_path = found
            logging.info(f"✅ Found ngrok on PATH: {found}")
            return
        elif found:
            logging.info(f"ℹ️ Found pyngrok shim on PATH, ignoring: {found}")

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

        conf.get_default().ngrok_path = str(per_user_bin)
        os.environ["TMP"] = str(ngrok_dir)
        os.environ["TEMP"] = str(ngrok_dir)
        os.environ["TMPDIR"] = str(ngrok_dir)
        logging.info(f"ℹ️ Falling back to pyngrok auto-download into: {per_user_bin}")

    def _ngrok_preflight(self) -> None:
        ngrok_bin = Path(conf.get_default().ngrok_path or "")
        try:
            if (
                ngrok_bin
                and ngrok_bin.exists()
                and not self._looks_like_pyngrok_shim(str(ngrok_bin))
            ):
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

            from pyngrok import installer

            installer.install_ngrok(conf.get_default().ngrok_path)
            logging.info("✅ ngrok installed via pyngrok.")

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
                "If this is a locked-down network, set [ngrok] path to an existing ngrok binary."
            )
            sys.exit(1)

    # ---------- Docker helpers ----------
    def ensure_docker(self) -> None:
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
            logging.info("✅ Docker is running and accessible.")
        except Exception:
            logging.error("❌ Critical Error: Docker is not running or not installed.")
            sys.exit(1)

    def prepare_base_image(self) -> None:
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def _kill_and_remove_by_name(self, name: str) -> None:
        """Stop and remove any container that already uses this name (prevents 409)."""
        if self.docker_client is None:
            return
        try:
            c = self.docker_client.containers.get(name)
        except docker.errors.NotFound:
            return
        try:
            try:
                c.stop(timeout=5)
            except Exception:
                pass
            c.remove(force=True)
            logging.info(f"🧹 Removed stale container '{name}'.")
        except Exception as e:
            logging.warning(f"Could not remove stale container '{name}': {e}")

    # ---------- CPU/RAM + environment detection ----------
    def _detect_cpu_ram(self) -> tuple[int, int]:
        cores = psutil.cpu_count(logical=False) or psutil.cpu_count(logical=True)
        if not cores:
            cores = os.cpu_count() or 1
        vm = psutil.virtual_memory()
        ram_gb = int(round(vm.total / (1024**3)))
        return int(max(1, cores)), int(max(1, ram_gb))

    def detect_environment_and_specs(self) -> Dict[str, Any]:
        logging.info("🔎 Detecting environment and hardware specifications...")

        cpu_cores, ram_gb = self._detect_cpu_ram()

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

        try:
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            gpu_model = pynvml.nvmlDeviceGetName(handle)
            try:
                gpu_model = gpu_model.decode("utf-8")
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
        logging.info(f"📊 Starting stats monitoring for Job ID: {job_id}")
        try:
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
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
            self.monitoring_tasks.pop(job_id, None)
            logging.info(f"⏹️ Stopped stats monitoring for Job ID: {job_id}")

    # ---------- Container lifecycle ----------
    def _start_container(self, job_id: int, websocket) -> Optional[Dict[str, Any]]:
        logging.info(f"🚀 Starting container for job ID: {job_id}...")
        try:
            if job_id in self.active_containers:
                logging.warning(f"Container for job {job_id} is already running.")
                return None

            if self.docker_client is None:
                raise RuntimeError("Docker client not initialized")

            host_port = self._get_free_port()
            token = f"unified-{job_id}-{int(time.time())%10000}"
            self._tokens[job_id] = token  # persist for resume

            # Per-job Jupyter base path (randomized)
            base_prefix = f"/{secrets.token_urlsafe(8)}/"
            self._base_prefix[job_id] = base_prefix

            # If using ngrok, generate per-job edge basic auth
            edge_user = "uc"
            edge_pass = secrets.token_urlsafe(24)

            # Launch Classic Notebook with token + base_url
            jupyter_command = (
                "bash -lc "
                '"python -m pip install --no-cache-dir --upgrade pip && '
                "python -m pip install --no-cache-dir notebook jupyter_server ipykernel && "
                f"python -m notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root "
                f"--NotebookApp.base_url='{base_prefix}' "
                f"--NotebookApp.token='{token}'\""
            )

            name = f"{CONTAINER_NAME_PREFIX}{job_id}"
            # Pre-clean any stale container with this name to avoid 409 Conflict
            self._kill_and_remove_by_name(name)

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
                    name=name,
                    remove=True,  # auto-remove on exit
                    labels={"uc.job_id": str(job_id)},
                    shm_size="1g",  # prevents first-kernel OOM/crash
                )
            except Exception:
                # fallback w/o runtime flag for hosts without nvidia runtime
                container = self.docker_client.containers.run(
                    PYTORCH_IMAGE,
                    command=jupyter_command,
                    detach=True,
                    ports={"8888/tcp": host_port},
                    device_requests=[
                        docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])
                    ],
                    name=name,
                    remove=True,
                    labels={"uc.job_id": str(job_id)},
                    shm_size="1g",
                )

            self.active_containers[job_id] = container.id
            self.session_start_times[job_id] = time.time()
            logging.info(
                f"✅ Container {container.id[:12]} running on host port {host_port}"
            )

            # Save state ASAP
            self._save_state()

            # Wait for Jupyter to actually be up before exposing
            logging.info(
                f"⏳ Waiting for Jupyter to come up on http://127.0.0.1:{host_port} ..."
            )
            if not self._wait_for_jupyter(host_port, timeout=240):
                logging.warning(
                    f"Jupyter on port {host_port} did not become ready within timeout. "
                    f"Not creating tunnel; stopping container so the client can retry later."
                )
                self._stop_container(job_id)
                return None

            logging.info(f"✅ Jupyter is up on {host_port}. Creating tunnel…")

            # Create public tunnel based on provider
            if self.tunnel_provider == "cloudflare":
                public_base, proc = self._start_cloudflared(host_port)
                public_url = public_base.rstrip("/") + base_prefix
                self.active_tunnels[job_id] = {
                    "provider": "cloudflare",
                    "proc": proc,
                    "public_url": public_url,
                }
            else:
                tunnel = ngrok.connect(
                    addr=host_port,
                    proto="http",
                    auth=f"{edge_user}:{edge_pass}",
                    inspect=False,
                )
                public_url = tunnel.public_url.rstrip("/") + base_prefix
                public_url = _with_basic_auth(public_url, edge_user, edge_pass)
                self.active_tunnels[job_id] = {
                    "provider": "ngrok",
                    "tunnel": tunnel,
                    "public_url": public_url,
                }

            logging.info(
                f"✅ Secure tunnel created for job {job_id}: {self.active_tunnels[job_id]['public_url']}"
            )

            # Launch background GPU monitor
            task = asyncio.create_task(
                self._monitor_and_report_stats(job_id, websocket)
            )
            self.monitoring_tasks[job_id] = task

            # Save state again (optional)
            self._save_state()

            return {"public_url": public_url, "token": token}

        except Exception as e:
            logging.error(
                f"❌ Failed to start container/tunnel for job {job_id}: {e}",
                exc_info=True,
            )
            self._stop_container(job_id)
            return None

    def _stop_container(self, job_id: int) -> None:
        # Cancel stats monitor
        mon = self.monitoring_tasks.pop(job_id, None)
        if mon:
            mon.cancel()
            logging.info(f"📊 Canceled stats monitoring task for job {job_id}.")

        # Close tunnel
        t = self.active_tunnels.pop(job_id, None)
        if t:
            try:
                if t.get("provider") == "cloudflare":
                    logging.info("🛑 Terminating cloudflared tunnel process.")
                    try:
                        proc = t.get("proc")
                        if proc and proc.poll() is None:
                            proc.terminate()
                    except Exception as e:
                        logging.warning(f"cloudflared terminate warning: {e}")
                elif t.get("provider") == "ngrok":
                    url = t.get("public_url")
                    if url:
                        logging.info(f"🛑 Disconnecting ngrok tunnel: {url}")
                        try:
                            ngrok.disconnect(url)
                        except Exception as e:
                            logging.warning(f"ngrok disconnect warning: {e}")
            except Exception as e:
                logging.warning(f"Tunnel cleanup warning: {e}")

        # Stop and remove container
        container_id = self.active_containers.pop(job_id, None)
        if container_id and self.docker_client:
            try:
                c = self.docker_client.containers.get(container_id)
            except docker.errors.NotFound:
                c = None
            if c:
                try:
                    logging.info(f"🛑 Stopping container {container_id[:12]}")
                    c.stop(timeout=5)
                except Exception as e:
                    logging.warning(f"Container stop warning: {e}")
                try:
                    c.remove(force=True)
                    logging.info(f"🧹 Removed container {container_id[:12]}")
                except docker.errors.NotFound:
                    pass
                except Exception as e:
                    logging.warning(f"Container remove warning: {e}")

        # Clear recorded start time, token, and base prefix
        self.session_start_times.pop(job_id, None)
        self._tokens.pop(job_id, None)
        self._base_prefix.pop(job_id, None)

        # Persist new state
        self._save_state()

    # ---------- state helpers ----------
    def _state_snapshot(self) -> dict:
        """Builds a JSON-serializable snapshot of live sessions."""
        sessions = {}
        for job_id, container_id in self.active_containers.items():
            sessions[str(job_id)] = {
                "container_id": container_id,
                "container_name": f"{CONTAINER_NAME_PREFIX}{job_id}",
                "host_port": self._host_port_for_container(container_id) or None,
                "token": self._tokens.get(job_id),
                "session_start_time": self.session_start_times.get(job_id),
                "base_prefix": self._base_prefix.get(job_id),
            }
        return {"sessions": sessions}

    def _save_state(self) -> None:
        """Writes current state to disk."""
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(self._state_snapshot(), f)
            logging.info(f"💾 State saved to {STATE_FILE}")
        except Exception as e:
            logging.error(f"Failed to save state: {e}", exc_info=True)

    def _load_state(self) -> dict:
        if not os.path.exists(STATE_FILE):
            logging.info("💾 No previous state file. Starting fresh.")
            return {"sessions": {}}
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            logging.info(f"💾 Loaded state from {STATE_FILE}")
            return data or {"sessions": {}}
        except Exception as e:
            logging.error(f"Failed to read state file: {e}", exc_info=True)
            return {"sessions": {}}

    def _host_port_for_container(self, container_id: str) -> Optional[int]:
        try:
            c = self.docker_client.containers.get(container_id)
            c.reload()  # ensure attrs are fresh
            ports = (c.attrs or {}).get("NetworkSettings", {}).get("Ports", {})
            bindings = ports.get("8888/tcp") or []
            if bindings:
                return int(bindings[0].get("HostPort"))
        except Exception:
            pass
        return None

    def _load_and_prepare_resumes(self) -> None:
        """
        Load state file and populate self._resumable_sessions with sessions we can
        re-attach to (container exists). We will finalize resume after WS connects.
        """
        self._resumable_sessions: Dict[int, dict] = {}

        state = self._load_state()
        sessions = state.get("sessions", {})

        for job_id_str, s in sessions.items():
            try:
                job_id = int(job_id_str)
            except ValueError:
                continue

            container_id = s.get("container_id")
            container_name = (
                s.get("container_name") or f"{CONTAINER_NAME_PREFIX}{job_id}"
            )
            token = s.get("token")
            start_ts = s.get("session_start_time")
            base_prefix = s.get("base_prefix") or "/"

            # Prefer look-up by id; fallback to name
            container = None
            try:
                if container_id:
                    container = self.docker_client.containers.get(container_id)
            except Exception:
                container = None

            if container is None:
                try:
                    container = self.docker_client.containers.get(container_name)
                except Exception:
                    container = None

            if not container:
                logging.info(
                    f"⚠️ No live container found for job {job_id}; skipping resume."
                )
                continue

            host_port = self._host_port_for_container(container.id)
            if not host_port:
                logging.info(
                    f"⚠️ Could not detect host port for job {job_id}; skipping resume."
                )
                continue

            # Track as active and mark to finish after WS connects
            self.active_containers[job_id] = container.id
            if token:
                self._tokens[job_id] = token
            if start_ts:
                self.session_start_times[job_id] = start_ts
            if base_prefix:
                self._base_prefix[job_id] = base_prefix

            self._resumable_sessions[job_id] = {
                "container_id": container.id,
                "host_port": host_port,
                "token": token,
                "session_start_time": start_ts,
                "base_prefix": base_prefix,
            }
            logging.info(
                f"🔁 Will resume session for job {job_id} on port {host_port}."
            )

    async def _resume_all_sessions_over_ws(self, websocket):
        """
        Called right after a WS connection is established.
        Recreate tunnels, restart monitors, and re-emit session_ready.
        """
        for job_id, info in list(self._resumable_sessions.items()):
            host_port = info["host_port"]
            token = info.get("token")
            base_prefix = (
                self._base_prefix.get(job_id) or info.get("base_prefix") or "/"
            )
            try:
                # Make sure Jupyter is responding before exposing/resuming
                if not self._wait_for_jupyter(host_port, timeout=60):
                    logging.warning(
                        f"[resume] Jupyter on port {host_port} not ready; skipping tunnel for job {job_id}."
                    )
                    continue

                # Recreate tunnel
                if self.tunnel_provider == "cloudflare":
                    public_base, proc = self._start_cloudflared(host_port)
                    public_url = public_base.rstrip("/") + base_prefix
                    self.active_tunnels[job_id] = {
                        "provider": "cloudflare",
                        "proc": proc,
                        "public_url": public_url,
                    }
                else:
                    edge_user = "uc"
                    edge_pass = secrets.token_urlsafe(24)
                    tunnel = ngrok.connect(
                        addr=host_port,
                        proto="http",
                        auth=f"{edge_user}:{edge_pass}",
                        inspect=False,
                    )
                    public_url = tunnel.public_url.rstrip("/") + base_prefix
                    public_url = _with_basic_auth(public_url, edge_user, edge_pass)
                    self.active_tunnels[job_id] = {
                        "provider": "ngrok",
                        "tunnel": tunnel,
                        "public_url": public_url,
                    }

                # restart monitor
                task = asyncio.create_task(
                    self._monitor_and_report_stats(job_id, websocket)
                )
                self.monitoring_tasks[job_id] = task

                # re-emit session_ready so backend fills SESSION_CACHE
                msg = {
                    "status": "session_ready",
                    "job_id": job_id,
                    "public_url": public_url,
                    "token": token,
                }
                await websocket.send(json.dumps(msg))
                logging.info(
                    f"🔁 Re-announced session_ready for job {job_id} ({public_url})"
                )

                # remove from resumables
                self._resumable_sessions.pop(job_id, None)

                # Persist (optional, no public_url persisted)
                self._save_state()

            except Exception as e:
                logging.error(f"Failed to resume job {job_id}: {e}", exc_info=True)

    # ---------- Env/chain helpers ----------
    async def get_on_chain_listings(self) -> Dict[int, Any]:
        try:
            resource_type = f"{self.contract_address}::marketplace::ListingManager}}"
            response = await self.rest_client.account_resource(
                str(self.host_account.address()), resource_type
            )
            listings_data = response.get("data", {}).get("listings", [])
            return {int(l["id"]): l for l in listings_data}
        except Exception:
            return {}

    async def register_on_chain_if_needed(self, specs: Dict[str, Any]) -> None:
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
                    self._stop_container(job_id)
                    return

                duration = max_end_time - start_time
                if duration <= 0:
                    logging.warning(
                        f"Job {job_id} has non-positive duration; stopping."
                    )
                    self.active_jobs.discard(job_id)
                    self._stop_container(job_id)
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
                        self._stop_container(job_id)
                        self.active_jobs.discard(job_id)
                        return
                    logging.info(f"Job {job_id}: nothing new to claim yet.")
                    await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)
                    continue

                # Compute final_session_duration for the final claim, else 0
                final_session_duration = 0
                if claim_timestamp >= max_end_time:
                    start_ts = self.session_start_times.get(job_id)
                    if start_ts:
                        final_session_duration = int(time.time() - start_ts)
                    else:
                        final_session_duration = int(max_end_time - start_time)

                # Prefer 3-arg ABI (Week 9+). If chain is older, retry with 2-arg ABI.
                try:
                    payload = TransactionPayload(
                        EntryFunction.natural(
                            f"{self.contract_address}::escrow",
                            "claim_payment",
                            [],
                            [
                                TransactionArgument(job_id, Serializer.u64),
                                TransactionArgument(claim_timestamp, Serializer.u64),
                                TransactionArgument(
                                    final_session_duration, Serializer.u64
                                ),
                            ],
                        )
                    )
                    await self.submit_transaction(
                        payload, f"Successfully claimed payment for Job ID {job_id}"
                    )
                except Exception as e:
                    if "NUMBER_OF_ARGUMENTS_MISMATCH" in str(e):
                        logging.warning(
                            "Chain indicates older claim_payment ABI; retrying with 2 arguments."
                        )
                        payload2 = TransactionPayload(
                            EntryFunction.natural(
                                f"{self.contract_address}::escrow",
                                "claim_payment",
                                [],
                                [
                                    TransactionArgument(job_id, Serializer.u64),
                                    TransactionArgument(
                                        claim_timestamp, Serializer.u64
                                    ),
                                ],
                            )
                        )
                        await self.submit_transaction(
                            payload2,
                            f"Successfully claimed payment (2-arg ABI) for Job ID {job_id}",
                        )
                    else:
                        raise

                if claim_timestamp >= max_end_time:
                    logging.info(
                        f"Job {job_id}: final claim submitted; stopping session."
                    )
                    self._stop_container(job_id)
                    self.active_jobs.discard(job_id)
                    return

            except ApiError as e:
                logging.error(
                    f"API Error claiming payment for Job ID {job_id}: {e}",
                    exc_info=True,
                )
                self._stop_container(job_id)
                self.active_jobs.discard(job_id)
                return
            except Exception:
                logging.error(
                    f"Generic error claiming payment for Job ID {job_id}", exc_info=True
                )
                self._stop_container(job_id)
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
        addr_str = str(self.host_account.address())
        uri = f"{self.backend_ws_url}/{addr_str}"
        backoff = 3
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    logging.info(f"✅ WebSocket connected to backend at {uri}")
                    # On each (re)connect, re-announce any resumable sessions
                    await self._resume_all_sessions_over_ws(websocket)
                    backoff = 3  # reset backoff

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
                            details = self._start_container(job_id, websocket)
                            resp = {
                                "status": (
                                    "session_ready" if details else "session_error"
                                ),
                                "job_id": job_id,
                            }
                            if details:
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
                websockets.exceptions.ConnectionClosedError,
                ConnectionRefusedError,
                OSError,
            ) as e:
                logging.warning(
                    f"⚠️ WebSocket connection issue: {e}. Retrying in {backoff} seconds..."
                )
            except Exception as e:
                logging.error(
                    f"Unexpected error in WebSocket listener: {e}", exc_info=True
                )

            # jittered exponential backoff
            await asyncio.sleep(backoff)
            backoff = min(30, int(backoff * 1.7) + 1)

    async def run(self) -> None:
        logging.info("--- 🚀 Starting Unified Compute Host Agent ---")
        self.ensure_docker()

        # Load persisted sessions and mark them resumable BEFORE anything else
        self._load_and_prepare_resumes()

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
        _ = config["aptos"]["private_key"]
        _ = config["aptos"]["contract_address"]
        _ = config["aptos"]["node_url"]
        _ = config["host"]["price_per_second"]
        # Optional: [tunnel] provider=cloudflare|ngrok
        # Optional: [tunnel] cloudflared_path=C:\Program Files (x86)\cloudflared\cloudflared.exe
        # Optional: [ngrok]  auth_token=...
    except (KeyError, FileNotFoundError) as e:
        logging.error(
            f"Configuration error in 'config.ini': {e}. Please ensure the file exists and has required keys."
        )
        return

    agent = HostAgent(config)
    await agent.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nAgent shut down by user.")
