import json
import logging
import os
from typing import Dict, Optional
from pathlib import Path

from constants import STATE_FILE, CONTAINER_NAME_PREFIX

logger = logging.getLogger(__name__)


class StateManager:
    def __init__(self, docker_client_ref_getter):
        self._docker_client_ref_getter = docker_client_ref_getter

    def host_port_for_container(self, container_id: str) -> Optional[int]:
        client = self._docker_client_ref_getter()
        if not client:
            return None
        try:
            c = client.containers.get(container_id)
            c.reload()
            ports = (c.attrs or {}).get("NetworkSettings", {}).get("Ports", {})
            bindings = ports.get("8888/tcp") or []
            if bindings:
                return int(bindings[0].get("HostPort"))
        except Exception:
            pass
        return None

    def load(self) -> dict:
        if not os.path.exists(STATE_FILE):
            logger.info("💾 No previous state file. Starting fresh.")
            return {"sessions": {}}
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"💾 Loaded state from {STATE_FILE}")
            return data or {"sessions": {}}
        except Exception as e:
            logger.error(f"Failed to read state file: {e}", exc_info=True)
            return {"sessions": {}}

    def snapshot(self, active_containers, tokens, session_start_times, base_prefix) -> dict:
        sessions = {}
        for job_id, container_id in active_containers.items():
            sessions[str(job_id)] = {
                "container_id": container_id,
                "container_name": f"{CONTAINER_NAME_PREFIX}{job_id}",
                "host_port": self.host_port_for_container(container_id) or None,
                "token": tokens.get(job_id),
                "session_start_time": session_start_times.get(job_id),
                "base_prefix": base_prefix.get(job_id),
            }
        return {"sessions": sessions}

    def save(self, snapshot: dict) -> None:
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(snapshot, f)
            logger.info(f"💾 State saved to {STATE_FILE}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}", exc_info=True)
