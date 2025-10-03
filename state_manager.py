import json
from pathlib import Path
from typing import Dict, Any

try:
    from constants import STATE_FILE
except ImportError:
    STATE_FILE = Path.cwd() / "agent_state.json"


class StateManager:
    """
    Simple JSON-based state store used by the agent and GUI.
    Schema:
    {
      "sessions": {
        "<job_id_str>": {
          "status": "...",
          "started_at": 0,
          ...
        }
      },
      "host": {
        "registered": bool,
        "identifier": str,
        "registered_at": int
      }
    }
    """

    def __init__(self, docker_client_ref_getter=None):
        self._docker_client_ref_getter = docker_client_ref_getter
        self._state_path = Path(STATE_FILE)

    # ---------- Core R/W ----------
    def load(self) -> Dict[str, Any]:
        if not self._state_path.exists():
            return {"sessions": {}, "host": {}}
        try:
            return json.loads(self._state_path.read_text(encoding="utf-8")) or {
                "sessions": {},
                "host": {},
            }
        except Exception:
            return {"sessions": {}, "host": {}}

    def save(self, state: Dict[str, Any]) -> None:
        # Ensure minimal keys always present
        state.setdefault("sessions", {})
        state.setdefault("host", {})
        self._state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    # ---------- Convenience ----------
    def set_host_registered(self, identifier: str, ts: int) -> None:
        st = self.load()
        st.setdefault("host", {})
        st["host"]["registered"] = True
        st["host"]["identifier"] = identifier
        st["host"]["registered_at"] = ts
        self.save(st)

    def get_host_registered(self) -> bool:
        st = self.load()
        return bool(st.get("host", {}).get("registered"))
