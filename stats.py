# stats.py

import asyncio
import json
import logging
import pynvml

try:
    from constants import STATS_INTERVAL_SECONDS
except ImportError:
    STATS_INTERVAL_SECONDS = 5

logger = logging.getLogger(__name__)


class StatsMonitor:
    def __init__(self):
        pass

    async def monitor_gpu(self, job_id: int, nvml_handle, active_containers, websocket, monitoring_tasks):
        if not nvml_handle:
            logger.error(f"NVML handle not provided for job {job_id}; cannot monitor stats.")
            monitoring_tasks.pop(job_id, None)
            return

        logger.info(f"📊 Starting stats monitoring for Job ID: {job_id}")
        try:
            while job_id in active_containers:
                try:
                    # Use the provided 'nvml_handle' directly
                    mem = pynvml.nvmlDeviceGetMemoryInfo(nvml_handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(nvml_handle)
                    stats = {
                        "gpu_utilization_percent": int(util.gpu),
                        "memory_used_mb": int(mem.used // (1024**2)),
                        "memory_total_mb": int(mem.total // (1024**2)),
                    }
                except pynvml.NVMLError as e:
                    logger.error(f"NVML read error (job {job_id}): {e}. Stopping monitor.")
                    break # Stop monitoring if the GPU can't be read
                
                try:
                    await websocket.send(
                        json.dumps({"status": "stats_update", "job_id": job_id, "stats": stats})
                    )
                except Exception:
                    logger.warning(f"WebSocket closed while monitoring job {job_id}. Stopping stats.")
                    break
                
                await asyncio.sleep(STATS_INTERVAL_SECONDS)
    
        finally:
            monitoring_tasks.pop(job_id, None)
            logger.info(f"⏹️ Stopped stats monitoring for Job ID: {job_id}")