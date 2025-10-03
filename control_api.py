import threading
import time
import uuid
from typing import Any, Dict, Optional

try:
    from fastapi import FastAPI, HTTPException
except Exception as e:  # pragma: no cover - optional dependency error surfaced by agent
    raise


def create_app(agent) -> "FastAPI":
    """Create a FastAPI app bound to a HostAgent instance.

    Endpoints:
    - GET /health -> {status: ok}
    - GET /sessions -> current sessions from state file
    - POST /start/{job_id}
    - POST /stop/{job_id}
    """
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/sessions")
    def sessions():
        """List current sessions with enhanced info for one-per-user policy"""
        try:
            state = agent.state_manager.load()
            sessions_data = state.get("sessions", {})

            # Enhance with renter info if available in agent memory
            enhanced_sessions = {}
            for job_id_str, session_info in sessions_data.items():
                try:
                    job_id = int(job_id_str)
                    renter = agent._job_renter.get(job_id, "Unknown")
                    enhanced_info = dict(session_info)
                    enhanced_info["renter"] = renter
                    enhanced_sessions[job_id_str] = enhanced_info
                except ValueError:
                    enhanced_sessions[job_id_str] = session_info

            return enhanced_sessions
        except Exception as e:
            return {"error": str(e)}

    def _request_and_wait(
        command: Dict[str, Any], timeout: float = 30.0
    ) -> Dict[str, Any]:
        # Correlate result through agent's condition and results map
        corr = str(uuid.uuid4())
        command["correlation_id"] = corr
        agent.enqueue_local_command(command)
        # Wait for response up to timeout
        deadline = time.time() + timeout
        with agent._result_cond:  # type: ignore[attr-defined]
            while time.time() < deadline:
                if corr in agent._local_results:  # type: ignore[attr-defined]
                    return agent._local_results.pop(corr)  # type: ignore[attr-defined]
                remaining = max(0.0, deadline - time.time())
                agent._result_cond.wait(timeout=remaining)  # type: ignore[attr-defined]
        # Timeout
        return {"status": "accepted", "detail": "processing in background"}

    @app.post("/start/{job_id}")
    def start(job_id: int):
        try:
            result = _request_and_wait({"action": "start_session", "job_id": job_id})
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/stop/{job_id}")
    def stop(job_id: int):
        try:
            result = _request_and_wait({"action": "stop_session", "job_id": job_id})
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return app
