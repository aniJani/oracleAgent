import logging
import socket
import docker
from typing import Optional

from constants import PYTORCH_IMAGE

logger = logging.getLogger(__name__)


class DockerManager:
    def __init__(self):
        self.client: Optional[docker.DockerClient] = None

    def ensure(self):
        try:
            self.client = docker.from_env()
            self.client.ping()
            logger.info("✅ Docker is running and accessible.")
        except Exception:
            logger.error("❌ Critical Error: Docker is not running or not installed.")
            raise SystemExit(1)

    def prepare_image(self):
        if self.client is None:
            raise RuntimeError("Docker client not initialized")
        logger.info(f"🐳 Checking for Docker image: {PYTORCH_IMAGE}...")
        try:
            self.client.images.get(PYTORCH_IMAGE)
            logger.info("   - Image already exists locally.")
        except docker.errors.ImageNotFound:
            logger.info("   - Image not found. Pulling from Docker Hub...")
            try:
                self.client.images.pull(PYTORCH_IMAGE)
                logger.info("✅ Successfully pulled PyTorch base image.")
            except Exception as e:
                logger.error(f"❌ Critical Error: Failed to pull Docker image: {e}", exc_info=True)
                raise SystemExit(1)

    @staticmethod
    def get_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]
