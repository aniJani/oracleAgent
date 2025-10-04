# docker_runtime.py

import os
import shlex
import subprocess
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

log = logging.getLogger("host-agent.docker")

# ---- Constants (import if available; otherwise use sane defaults) ----
try:
    from constants import PYTORCH_IMAGE as _IMG
except Exception:
    _IMG = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"

try:
    from constants import CONTAINER_NAME_PREFIX as _PREFIX
except Exception:
    _PREFIX = "uc-job-"

from config import SESSIONS_DIR as SESSIONS_ROOT

DOCKER_BIN = os.environ.get("DOCKER_BIN", "docker")


class DockerRuntime:
    """Minimal helper to run classic Jupyter Notebook sessions inside a container."""

    def __init__(self, image: Optional[str] = None):
        self.image = image or _IMG
        self.bin = DOCKER_BIN

    def _run(self, args: List[str]) -> subprocess.CompletedProcess:
        log.info("[docker] %s", " ".join(shlex.quote(a) for a in args))
        return subprocess.run(args, capture_output=True, text=True, check=False)

    def ensure_image(self) -> None:
        r = self._run([self.bin, "image", "inspect", self.image])
        if r.returncode != 0:
            log.info(f"Pulling Docker image: {self.image}")
            r = self._run([self.bin, "pull", self.image])
            if r.returncode != 0:
                raise RuntimeError(f"Docker pull failed: {r.stderr.strip()}")

    def _container_name(self, job_id: Union[int, str]) -> str:
        return f"{_PREFIX}{job_id}"

    def start_jupyter(
        self,
        job_id: int,
        host_port: int,
        token: str = "",
        mounts: Optional[Iterable[Tuple[str, str]]] = None,
        envs: Optional[Dict[str, str]] = None,
        workdir: str = "/workspace",
    ) -> str:
        """
        Start a containerized classic Jupyter Notebook on port 8888 mapped to host_port.
        Returns the container ID.
        """
        name = self._container_name(job_id)

        # Stop any previous container
        self._run([self.bin, "rm", "-f", name])

        # Per-job workspace
        job_dir = (SESSIONS_ROOT / str(job_id)).resolve()
        job_dir.mkdir(parents=True, exist_ok=True)

        # --- FIX: Robust bootstrap command using a standard 'if' statement ---
        # This is a much safer way to conditionally install a package in bash,
        # avoiding the syntax errors caused by the previous multi-line '||' chain.
        bootstrap = (
            "python -m pip install --no-cache-dir --upgrade pip && "
            "if ! python -c \"import importlib.util; exit(0 if importlib.util.find_spec('notebook') else 1)\"; then "
            "    echo 'Jupyter Notebook not found, installing notebook==6.5.*...'; "
            "    python -m pip install --no-cache-dir 'notebook==6.5.*'; "
            "fi && "
            "python -m notebook --ip=0.0.0.0 --port=8888 --no-browser "
            "--NotebookApp.token='' --NotebookApp.password='' "
            "--NotebookApp.allow_origin='*' --NotebookApp.disable_check_xsrf=True --allow-root"
        )

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
        ]

        if mounts:
            for host_path, container_path in mounts:
                cmd += ["-v", f"{Path(host_path).resolve()}:{container_path}"]

        if envs:
            for k, v in envs.items():
                cmd += ["-e", f"{k}={v}"]

        # FIX: Use 'bash -c' to execute the command string directly
        cmd += [self.image, "bash", "-c", bootstrap]

        self.ensure_image()
        r = self._run(cmd)
        if r.returncode != 0:
            # Provide more context on failure
            log.error(f"Docker run command failed. Stderr:\n{r.stderr.strip()}")
            raise RuntimeError(f"Docker run failed: {r.stderr.strip()}")

        cid = r.stdout.strip()
        if not cid:
            raise RuntimeError("Docker run returned empty container ID")

        log.info(f"Container started: {cid} ({name}) http://127.0.0.1:{host_port}")
        return cid

    def stop(self, job_id: int) -> None:
        name = self._container_name(job_id)
        self._run([self.bin, "rm", "-f", name])