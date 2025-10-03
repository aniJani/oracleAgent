import os
import sys
import json
import time
import asyncio
import logging
import configparser
import contextlib
import subprocess
import shlex
import socket
from pathlib import Path
from typing import Optional, Dict

# ---- Aptos SDK ----
from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionPayload,
    TransactionArgument,
)

# ---- Hardware detection ----
import pynvml
import psutil

# ---- WebSocket client ----
import websockets

# ---- Constants ----
try:
    from constants import (
        DEFAULT_BACKEND_WS_URL,
        DEFAULT_CLOUDFLARED_PATH,
        PYTORCH_IMAGE,
        CONTAINER_NAME_PREFIX,
    )
except Exception:
    DEFAULT_BACKEND_WS_URL = "ws://127.0.0.1:5000/ws"
    DEFAULT_CLOUDFLARED_PATH = r"C:\Program Files (x86)\cloudflared\cloudflared.exe"
    PYTORCH_IMAGE = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"
    CONTAINER_NAME_PREFIX = "uc-job-"

log = logging.getLogger("host-agent")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


# ---------------------------- utilities ----------------------------
def _free_tcp_port(start: int = 7800, end: int = 7999) -> int:
    for port in range(start, end + 1):
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise RuntimeError("No free port found in range 7800-7999")


def _wait_http_ready(url: str, timeout: float = 60.0) -> bool:
    import urllib.request
    import urllib.error

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:  # nosec B310
                if 200 <= r.status < 500:
                    return True
        except Exception:
            time.sleep(1.0)
    return False


# ---------------------------- runtime managers ----------------------------
class DockerRuntime:
    def __init__(self):
        self.bin = os.environ.get("DOCKER_BIN", "docker")

    def _run(self, cmd: str) -> subprocess.CompletedProcess:
        log.info("[docker] %s", cmd)
        return subprocess.run(
            shlex.split(cmd), capture_output=True, text=True, check=False
        )

    def ensure_image(self, image: str) -> None:
        # pull if not present
        r = self._run(f"{self.bin} image inspect {image}")
        if r.returncode != 0:
            log.info(f"Pulling Docker image: {image}")
            r = self._run(f"{self.bin} pull {image}")
            if r.returncode != 0:
                raise RuntimeError(f"Docker pull failed: {r.stderr.strip()}")

    def start_jupyter(self, job_id: int, image: str, host_port: int, token: str) -> str:
        """
        Start a classic Jupyter Notebook server inside the container, expose host_port.
        Returns container_id.
        """
        name = f"{CONTAINER_NAME_PREFIX}{job_id}"
        # Stop/remove any old
        self._run(f"{self.bin} rm -f {name}")

        # --- Classic Notebook bootstrap (no Lab) ---
        bootstrap = " && ".join(
            [
                "python -m pip install --no-cache-dir --upgrade pip",
                # install classic notebook if absent
                "python - <<'PY'\n"
                "import importlib.util, sys\n"
                "sys.exit(0 if importlib.util.find_spec('notebook') else 1)\n"
                "PY\n"
                " || python -m pip install --no-cache-dir 'notebook==6.5.*'",
                # launch notebook (no token/password)
                "python -m notebook --ip=0.0.0.0 --port=8888 --no-browser "
                "--NotebookApp.token='' --NotebookApp.password='' "
                "--NotebookApp.allow_origin='*' --NotebookApp.disable_check_xsrf=True",
                "--allow-root"
            ]
        )

        cmd = (
            f"{self.bin} run -d --gpus all --name {name} "
            f"-p {host_port}:8888 "
            f"{image} "
            f"bash -lc {shlex.quote(bootstrap)}"
        )
        r = self._run(cmd)
        if r.returncode != 0:
            raise RuntimeError(f"Docker run failed: {r.stderr.strip()}")
        container_id = r.stdout.strip()
        if not container_id:
            raise RuntimeError("Docker run returned empty container ID")
        log.info(f"Container started: {container_id} ({name}) on :{host_port}")
        return container_id

    def stop(self, job_id: int) -> None:
        name = f"{CONTAINER_NAME_PREFIX}{job_id}"
        self._run(f"{self.bin} rm -f {name}")


class TunnelRuntime:
    def __init__(self, bin_path: Optional[str] = None):
        self.bin = bin_path or DEFAULT_CLOUDFLARED_PATH
        self.proc_by_job: Dict[int, subprocess.Popen] = {}
        self.url_by_job: Dict[int, str] = {}

    def start(self, job_id: int, local_port: int, timeout: float = 30.0) -> str:
        """
        Start cloudflared and parse the public URL from stdout.
        Returns the public URL.
        """
        if not Path(self.bin).exists():
            raise RuntimeError(f"cloudflared not found at {self.bin}")

        self.stop(job_id)

        cmd = f'"{self.bin}" tunnel --url http://127.0.0.1:{local_port}'
        log.info(f"[tunnel] {cmd}")
        proc = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
        self.proc_by_job[job_id] = proc

        public_url: Optional[str] = None
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = proc.stdout.readline() if proc.stdout else ""
            if not line:
                time.sleep(0.2)
                continue
            log.info(f"[tunnel] {line.strip()}")
            if "trycloudflare.com" in line:
                for token in line.split():
                    if "http://" in token or "https://" in token:
                        public_url = token.strip()
                        break
            if public_url:
                break
            if proc.poll() is not None:
                break

        if not public_url:
            self.stop(job_id)
            raise RuntimeError("Failed to obtain Cloudflare tunnel URL")

        self.url_by_job[job_id] = public_url
        log.info(f"Tunnel ready: {public_url}")
        return public_url

    def stop(self, job_id: int) -> None:
        proc = self.proc_by_job.pop(job_id, None)
        if proc and proc.poll() is None:
            with contextlib.suppress(Exception):
                proc.terminate()
                proc.wait(timeout=5)
            if proc.poll() is None:
                with contextlib.suppress(Exception):
                    proc.kill()
        self.url_by_job.pop(job_id, None)


# ---------------------------- main Agent ----------------------------
class HostAgent:
    """
    Host/Oracle agent:
      - One-time on-chain registration (run main.py --register)
      - Set availability ONLINE/OFFLINE on-chain as it starts/stops
      - Maintain a WebSocket connection to backend at ws://.../ws/{host_address}
      - Handle start/stop_session over WS (by job_id)
    """

    def __init__(self, config: configparser.ConfigParser):
        # ---- Config ----
        self.config = config
        self.contract_address = config["aptos"]["contract_address"]
        self.node_url = config["aptos"]["node_url"]
        self.price_per_second = int(config["host"]["price_per_second"])

        # ---- Chain client & account ----
        self._rest = RestClient(self.node_url)
        self._acct = Account.load_key(config["aptos"]["private_key"])

        # ---- Backend WS base (env overrides constant) ----
        self.ws_base = os.getenv("BACKEND_WS_URL", DEFAULT_BACKEND_WS_URL)
        log.info(f"BACKEND_WS_URL = {self.ws_base}")
        log.info(f"Host address    = {self._acct.address()}")

        # ---- Runtimes ----
        self.docker = DockerRuntime()
        self.tunnel = TunnelRuntime()

        # ---- Session state by job_id ----
        self.sessions: Dict[int, Dict] = {}

    # -------------------------------------------------------------------------
    # Registration (direct on-chain)
    # -------------------------------------------------------------------------
    def _detect_specs(self) -> dict:
        """Detect GPU/CPU/RAM for registration."""
        specs = {}
        pynvml.nvmlInit()
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            name = pynvml.nvmlDeviceGetName(handle)
            if isinstance(name, bytes):
                name = name.decode("utf-8")
            specs["gpu_model"] = str(name)
        finally:
            pynvml.nvmlShutdown()
        specs["cpu_cores"] = int(psutil.cpu_count(logical=True))
        specs["ram_gb"] = int(round(psutil.virtual_memory().total / (1024**3)))
        return specs

    async def _already_registered(self) -> bool:
        try:
            await self._rest.account_resource(
                str(self._acct.address()),
                f"{self.contract_address}::marketplace::Listing",
            )
            return True
        except Exception:
            return False

    async def _submit_tx(self, payload: TransactionPayload, ok_msg: str) -> str:
        signed = await self._rest.create_bcs_signed_transaction(self._acct, payload)
        tx_hash = await self._rest.submit_bcs_transaction(signed)
        await self._rest.wait_for_transaction(tx_hash)
        log.info(f"{ok_msg} | tx={tx_hash}")
        return tx_hash

    async def register_device_if_needed(self) -> dict:
        log.info("--- Host Registration requested ---")

        if await self._already_registered():
            msg = "Host already registered on-chain; nothing to do."
            log.info(msg)
            return {"status": "ok", "message": msg, "tx_hash": None}

        specs = self._detect_specs()
        log.info(
            f"Detected specs: GPU={specs['gpu_model']}, "
            f"CPU cores={specs['cpu_cores']}, RAM={specs['ram_gb']} GB"
        )

        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace",
                "register_host_machine",
                [],
                [
                    TransactionArgument(specs["gpu_model"], Serializer.str),
                    TransactionArgument(specs["cpu_cores"], Serializer.u64),
                    TransactionArgument(specs["ram_gb"], Serializer.u64),
                    TransactionArgument(self.price_per_second, Serializer.u64),
                    TransactionArgument(
                        self._acct.public_key().to_bytes(), Serializer.to_bytes
                    ),
                ],
            )
        )
        try:
            txh = await self._submit_tx(payload, "✅ Successfully registered machine")
            return {"status": "ok", "message": "Device registered", "tx_hash": txh}
        except Exception as e:
            err = f"❌ Registration failed: {e}"
            log.error(err)
            return {"status": "error", "message": err, "tx_hash": None}

    # -------------------------------------------------------------------------
    # Availability control
    # -------------------------------------------------------------------------
    async def _set_availability(self, is_available: bool):
        status_str = "ONLINE" if is_available else "OFFLINE"
        log.info(f"Attempting to set availability to {status_str} on-chain...")
        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace",
                "set_availability",
                [],
                [TransactionArgument(bool(is_available), Serializer.bool)],
            )
        )
        await self._submit_tx(payload, f"✅ Machine is now {status_str}")

    def shutdown(self):
        log.info("--- Graceful shutdown ---")
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._set_availability(False))
        finally:
            loop.close()
        for job_id in list(self.sessions.keys()):
            with contextlib.suppress(Exception):
                self._stop_session_sync(job_id)
        log.info("✅ Shutdown complete.")

    # -------------------------------------------------------------------------
    # Session lifecycle (by job_id)
    # -------------------------------------------------------------------------
    def _start_session_sync(self, job_id: int, image: Optional[str] = None) -> Dict:
        image = image or PYTORCH_IMAGE
        self.docker.ensure_image(image)

        port = _free_tcp_port()
        token = os.environ.get("JUPYTER_TOKEN", f"token-{job_id}-{int(time.time())}")

        container_id = self.docker.start_jupyter(
            job_id=job_id, image=image, host_port=port, token=token
        )

        if not _wait_http_ready(f"http://127.0.0.1:{port}", timeout=180):
            self.docker.stop(job_id)
            raise RuntimeError("Jupyter did not become ready in time")

        public_url = self.tunnel.start(job_id=job_id, local_port=port, timeout=45)

        session = {
            "job_id": job_id,
            "container_id": container_id,
            "local_port": port,
            "public_url": public_url,
            "token": token,
            "image": image,
            "started_at": int(time.time()),
        }
        self.sessions[job_id] = session
        return session

    def _stop_session_sync(self, job_id: int) -> None:
        self.tunnel.stop(job_id)
        self.docker.stop(job_id)
        self.sessions.pop(job_id, None)

    async def _handle_start_session(self, ws, msg: dict):
        job_id = msg.get("job_id")
        image = msg.get("image") or PYTORCH_IMAGE
        if not isinstance(job_id, int):
            await ws.send(
                json.dumps(
                    {"status": "error", "error": "invalid_job_id", "detail": str(job_id)}
                )
            )
            return
        if job_id in self.sessions:
            s = self.sessions[job_id]
            await ws.send(
                json.dumps(
                    {
                        "status": "session_ready",
                        "job_id": job_id,
                        "public_url": s["public_url"],
                        "token": s["token"],
                    }
                )
            )
            return

        try:
            s = self._start_session_sync(job_id=job_id, image=image)
            await ws.send(
                json.dumps(
                    {
                        "status": "session_ready",
                        "job_id": job_id,
                        "public_url": s["public_url"],
                        "token": s["token"],
                    }
                )
            )
        except Exception as e:
            log.error(f"start_session failed for job {job_id}: {e}")
            await ws.send(
                json.dumps(
                    {
                        "status": "error",
                        "job_id": job_id,
                        "error": "start_failed",
                        "detail": str(e),
                    }
                )
            )

    async def _handle_stop_session(self, ws, msg: dict):
        job_id = msg.get("job_id")
        if not isinstance(job_id, int):
            await ws.send(
                json.dumps(
                    {"status": "error", "error": "invalid_job_id", "detail": str(job_id)}
                )
            )
            return
        try:
            self._stop_session_sync(job_id)
            await ws.send(json.dumps({"status": "session_stopped", "job_id": job_id}))
        except Exception as e:
            log.error(f"stop_session failed for job {job_id}: {e}")
            await ws.send(
                json.dumps(
                    {
                        "status": "error",
                        "job_id": job_id,
                        "error": "stop_failed",
                        "detail": str(e),
                    }
                )
            )

    # -------------------------------------------------------------------------
    # Backend WebSocket loop
    # -------------------------------------------------------------------------
    async def _ws_loop(self):
        host_addr = str(self._acct.address())
        url = f"{self.ws_base}/{host_addr}"
        log.info(f"Connecting WS → {url}")
        while True:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=6,
                    close_timeout=4,
                    max_size=None,
                ) as ws:
                    log.info(f"WS connected → {url}")
                    await ws.send(json.dumps({"kind": "hello", "addr": host_addr}))

                    while True:
                        raw = await ws.recv()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            log.warning(f"WS non-JSON message: {raw!r}")
                            continue

                        action = msg.get("action") or msg.get("kind")

                        if action == "hello":
                            log.info("WS hello ack")
                            continue
                        if action == "start_session":
                            await self._handle_start_session(ws, msg)
                            continue
                        if action == "stop_session":
                            await self._handle_stop_session(ws, msg)
                            continue

                        log.warning(f"WS unknown action: {action} | msg={msg}")

            except asyncio.CancelledError:
                log.info("WS loop cancelled; exiting.")
                break
            except Exception as e:
                log.warning(
                    f"WS disconnected/failed handshake ({e}); retrying in 3s..."
                )
                await asyncio.sleep(3)

    # -------------------------------------------------------------------------
    # Main run loop
    # -------------------------------------------------------------------------
    async def run(self):
        log.info("--- 🚀 Starting Host Agent ---")
        try:
            await self._set_availability(True)
        except Exception as e:
            log.error(f"Failed to set ONLINE: {e}")
            sys.exit(1)

        ws_task = asyncio.create_task(self._ws_loop())
        try:
            while True:
                await asyncio.sleep(2)
                log.info("Heartbeat: agent alive")
        finally:
            ws_task.cancel()
            with contextlib.suppress(Exception):
                await ws_task
