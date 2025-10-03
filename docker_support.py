import os
import shlex
import subprocess
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

log = logging.getLogger("host-agent.docker")

# ---- Constants (import if available; otherwise sane defaults) ----
try:
    from constants import PYTORCH_IMAGE as DEFAULT_IMAGE
except Exception:
    DEFAULT_IMAGE = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"

try:
    from constants import CONTAINER_NAME_PREFIX as NAME_PREFIX
except Exception:
    NAME_PREFIX = "uc-job-"

SESSIONS_ROOT = Path(os.getenv("SESSIONS_ROOT", "./sessions")).resolve()
SESSIONS_ROOT.mkdir(parents=True, exist_ok=True)

DOCKER_BIN = os.environ.get("DOCKER_BIN", "docker")


class DockerRuntime:
    """Helper to launch JupyterLab inside a GPU-enabled Docker container."""

    def __init__(self, image: Optional[str] = None):
        self.image = image or DEFAULT_IMAGE
        self.bin = DOCKER_BIN

    # ------------------------------- utils -------------------------------

    def _run(self, args: List[str]) -> subprocess.CompletedProcess:
        """Run a docker CLI command and return the CompletedProcess (no raise)."""
        log.info("[docker] %s", " ".join(shlex.quote(a) for a in args))
        return subprocess.run(args, capture_output=True, text=True, check=False)

    def _container_name(self, job_id: Union[int, str]) -> str:
        return f"{NAME_PREFIX}{job_id}"

    # ------------------------------- public -------------------------------

    def ensure_image(self, image: Optional[str] = None) -> None:
        """
        Make sure the base image exists locally (pull if missing).
        Accepts optional `image` override for compatibility with callers that pass it.
        """
        img = image or self.image
        r = self._run([self.bin, "image", "inspect", img])
        if r.returncode != 0:
            log.info(f"Pulling Docker image: {img}")
            r = self._run([self.bin, "pull", img])
            if r.returncode != 0:
                raise RuntimeError(f"Docker pull failed: {r.stderr.strip()}")

    def start_jupyter(
        self,
        job_id: int,
        host_port: int,
        token: str = "",
        mounts: Optional[Iterable[Tuple[str, str]]] = None,
        envs: Optional[Dict[str, str]] = None,
        workdir: str = "/workspace",
        image: Optional[str] = None,
    ) -> str:
        """
        Start a containerized JupyterLab on container port 8888 mapped to host_port.

        - Installs jupyterlab if missing (idempotent).
        - Launches via `python -m jupyter lab ...` (avoids PATH reliance on `jupyter`).
        - Returns the container ID.
        """
        img = image or self.image
        name = self._container_name(job_id)

        # Stop any previous container with the same name (idempotent)
        self._run([self.bin, "rm", "-f", name])

        # Per-job workspace
        job_dir = (SESSIONS_ROOT / str(job_id)).resolve()
        job_dir.mkdir(parents=True, exist_ok=True)

        # Bootstrap sequence executed inside the container
        bootstrap = " && ".join(
            [
                # Upgrade pip (safe & quick)
                "python -m pip install --no-cache-dir --upgrade pip",
                # Install jupyterlab only if absent (idempotent)
                "python - <<'PY'\n"
                "import importlib.util, sys\n"
                "sys.exit(0 if importlib.util.find_spec('jupyterlab') else 1)\n"
                "PY\n"
                " || python -m pip install --no-cache-dir jupyterlab",
                # Run JupyterLab via module to avoid PATH issues
                (
                    "python -m jupyter lab "
                    "--ip=0.0.0.0 --port=8888 --no-browser "
                    f"--NotebookApp.token='{token}' --NotebookApp.password=''"
                ),
            ]
        )

        # Build docker run command
        cmd: List[str] = [
            self.bin,
            "run",
            "-d",
            "--gpus",
            "all",
            "--name",
            name,
            "-p",
            f"{host_port}:8888",
            "-w",
            workdir,
            "-v",
            f"{str(job_dir)}:{workdir}",
            # image + command
            img,
            "bash",
            "-lc",
            bootstrap,
        ]

        # Extra mounts
        if mounts:
            # Insert before image so they apply correctly
            insert_at = len(cmd) - 3
            for host_path, container_path in mounts:
                cmd[insert_at:insert_at] = [
                    "-v",
                    f"{Path(host_path).resolve()}:{container_path}",
                ]
                insert_at += 2

        # Env vars
        if envs:
            insert_at = len(cmd) - 3
            for k, v in envs.items():
                cmd[insert_at:insert_at] = ["-e", f"{k}={v}"]
                insert_at += 2

        # Ensure image and run
        self.ensure_image(img)
        r = self._run(cmd)
        if r.returncode != 0:
            raise RuntimeError(f"Docker run failed: {r.stderr.strip()}")

        cid = r.stdout.strip()
        if not cid:
            raise RuntimeError("Docker run returned empty container ID")

        log.info(f"Container started: {cid} ({name}) http://127.0.0.1:{host_port}")
        return cid

    def stop(self, job_id: int) -> None:
        """Stop and remove the job container (idempotent)."""
        name = self._container_name(job_id)
        self._run([self.bin, "rm", "-f", name])
