import logging
import os
from pathlib import Path

# Logging configuration (kept identical to original)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

AWS_METADATA_URL = "http://169.254.169.254/latest/meta-data/"
POLLING_INTERVAL_SECONDS = 15
PAYMENT_CLAIM_INTERVAL_SECONDS = 60
STATS_INTERVAL_SECONDS = 5
PYTORCH_IMAGE = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"
DEFAULT_BACKEND_WS_URL = "ws://127.0.0.1:8000/ws"
DEFAULT_CLOUDFLARED_PATH = r"C:\\Program Files (x86)\\cloudflared\\cloudflared.exe"
STATE_FILE = "agent_state.json"
CONTAINER_NAME_PREFIX = "uc-job-"
