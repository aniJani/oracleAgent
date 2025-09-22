import asyncio
import json
import logging
import pynvml

from constants import STATS_INTERVAL_SECONDS

logger = logging.getLogger(__name__)


class StatsMonitor:
    def __init__(self):
        pass

    async def monitor_gpu(self, job_id: int, active_containers, websocket, monitoring_tasks):
        logger.info(f"📊 Starting stats monitoring for Job ID: {job_id}")
        try:
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            while job_id in active_containers:
                try:
                    mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    stats = {
                        "gpu_utilization_percent": int(util.gpu),
                        "memory_used_mb": int(mem.used // (1024**2)),
                        "memory_total_mb": int(mem.total // (1024**2)),
                    }
                except pynvml.NVMLError as e:
                    logger.error(f"NVML read error (job {job_id}): {e}")
                    stats = None
                try:
                    await websocket.send(
                        json.dumps({"status": "stats_update", "job_id": job_id, "stats": stats})
                    )
                except Exception:
                    logger.warning(f"WebSocket closed while monitoring job {job_id}. Stopping stats.")
                    break
                await asyncio.sleep(STATS_INTERVAL_SECONDS)
        except pynvml.NVMLError as e:
            logger.error(f"NVML init error while monitoring job {job_id}: {e}")
        finally:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass
            monitoring_tasks.pop(job_id, None)
            logger.info(f"⏹️ Stopped stats monitoring for Job ID: {job_id}")
