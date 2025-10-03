import os
import subprocess
from pathlib import Path

# If you already centralize this constant, import from your existing constants module instead
PYTORCH_IMAGE = os.getenv(
    "PYTORCH_IMAGE", "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"
)

SESSIONS_ROOT = Path(os.getenv("SESSIONS_ROOT", "./sessions")).resolve()
SESSIONS_ROOT.mkdir(parents=True, exist_ok=True)


def build_jupyter_cmd(host_port: int, token: str = "") -> list[str]:
    """
    Start Jupyter Lab inside the container. We *install* jupyterlab first because the
    PyTorch runtime image does not ship it. We then run via python -m jupyter to avoid PATH issues.
    """
    bootstrap = " && ".join(
        [
            # make sure pip exists and is up to date
            "python -m pip install --no-cache-dir --upgrade pip",
            # install jupyterlab only if missing (this is idempotent and fast on cache)
            'python -c "import importlib.util, sys; '
            "sys.exit(0 if importlib.util.find_spec('jupyterlab') else 1)\" || "
            "python -m pip install --no-cache-dir jupyterlab",
            # run jupyter on 0.0.0.0:8888, no browser, empty token
            f"python -m jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token='{token}' --NotebookApp.password=''",
        ]
    )
    return ["bash", "-lc", bootstrap]


def build_run_cmd(
    job_id: str | int,
    host_port: int,
    mounts: list[tuple[str, str]] | None = None,
    envs: dict[str, str] | None = None,
) -> list[str]:
    """
    Compose the 'docker run' command that launches the Jupyter Lab session.
    - Maps host_port -> container 8888
    - Mounts a per-job workspace at /workspace
    """
    name = f"uc-job-{job_id}"
    job_dir = (SESSIONS_ROOT / str(job_id)).resolve()
    job_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "docker",
        "run",
        "--rm",
        "--gpus",
        "all",
        "-p",
        f"{host_port}:8888",
        "--name",
        name,
        "-w",
        "/workspace",
        "-v",
        f"{job_dir}:/workspace",
    ]

    # Additional mounts
    if mounts:
        for host_path, container_path in mounts:
            cmd += ["-v", f"{Path(host_path).resolve()}:{container_path}"]

    # Environment variables
    if envs:
        for k, v in envs.items():
            cmd += ["-e", f"{k}={v}"]

    # Image + command
    cmd += [PYTORCH_IMAGE]
    cmd += build_jupyter_cmd(host_port=host_port, token="")

    return cmd


def start_session_container(job_id: str | int, host_port: int) -> subprocess.Popen:
    """
    Launch the container and return the Popen handle. Caller can read stdout/stderr.
    """
    cmd = build_run_cmd(job_id=job_id, host_port=host_port)
    try:
        # Unbuffered text mode so logs stream to backend logger
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
    except Exception as e:
        raise RuntimeError(f"Docker run failed: {e}")
