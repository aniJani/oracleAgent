# agent.py
import os
import sys
import json
import time
import asyncio
import logging
import configparser
from pathlib import Path
import contextlib

# ---- Aptos SDK ----
from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionPayload,
    TransactionArgument,
)

# ---- Hardware detection ----
import pynvml
import psutil

# ---- WebSocket client ----
import websockets

# ---- Constants ----
try:
    # Your constants.py should define DEFAULT_BACKEND_WS_URL (we override via env if set)
    from constants import DEFAULT_BACKEND_WS_URL
except Exception:
    # Fallback if constants.py is not available; adjust if needed
    DEFAULT_BACKEND_WS_URL = "ws://127.0.0.1:8000/ws"

log = logging.getLogger("host-agent")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


class HostAgent:
    """
    Host/Oracle agent:
      - One-time on-chain registration (run main.py --register)
      - Set availability ONLINE/OFFLINE on-chain as it starts/stops
      - Maintain a WebSocket connection to backend at ws://.../ws/{host_address}
    """

    def __init__(self, config: configparser.ConfigParser):
        # ---- Config ----
        self.config = config
        self.contract_address = config["aptos"]["contract_address"]
        self.node_url = config["aptos"]["node_url"]
        self.price_per_second = int(config["host"]["price_per_second"])

        # ---- Chain client & account ----
        self._rest = RestClient(self.node_url)
        self._acct = Account.load_key(config["aptos"]["private_key"])

        # ---- Backend WS base (env overrides constant) ----
        self.ws_base = os.getenv("BACKEND_WS_URL", DEFAULT_BACKEND_WS_URL)

    # -------------------------------------------------------------------------
    # Registration (direct on-chain)
    # -------------------------------------------------------------------------
    def _detect_specs(self) -> dict:
        """Detect GPU/CPU/RAM for registration."""
        specs = {}
        # GPU
        pynvml.nvmlInit()
        try:
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            name = pynvml.nvmlDeviceGetName(handle)
            if isinstance(name, bytes):
                name = name.decode("utf-8")
            specs["gpu_model"] = str(name)
        finally:
            pynvml.nvmlShutdown()
        # CPU & RAM
        specs["cpu_cores"] = int(psutil.cpu_count(logical=True))
        specs["ram_gb"] = int(round(psutil.virtual_memory().total / (1024**3)))
        return specs

    async def _already_registered(self) -> bool:
        """Returns True if this host already has a Listing resource on-chain."""
        try:
            await self._rest.account_resource(
                str(self._acct.address()),
                f"{self.contract_address}::marketplace::Listing",
            )
            return True
        except Exception:
            return False

    async def _submit_tx(self, payload: TransactionPayload, ok_msg: str) -> str:
        signed = await self._rest.create_bcs_signed_transaction(self._acct, payload)
        tx_hash = await self._rest.submit_bcs_transaction(signed)
        await self._rest.wait_for_transaction(tx_hash)
        log.info(f"{ok_msg} | tx={tx_hash}")
        return tx_hash

    async def register_device_if_needed(self) -> dict:
        """
        One-time on-chain registration. Safe to call multiple times:
        if already registered, no-op.
        """
        log.info("--- Host Registration requested ---")

        if await self._already_registered():
            msg = "Host already registered on-chain; nothing to do."
            log.info(msg)
            return {"status": "ok", "message": msg, "tx_hash": None}

        specs = self._detect_specs()
        log.info(
            f"Detected specs: GPU={specs['gpu_model']}, "
            f"CPU cores={specs['cpu_cores']}, RAM={specs['ram_gb']} GB"
        )

        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace",
                "register_host_machine",
                [],
                [
                    TransactionArgument(specs["gpu_model"], Serializer.str),
                    TransactionArgument(specs["cpu_cores"], Serializer.u64),
                    TransactionArgument(specs["ram_gb"], Serializer.u64),
                    TransactionArgument(self.price_per_second, Serializer.u64),
                    TransactionArgument(
                        self._acct.public_key().to_bytes(), Serializer.to_bytes
                    ),
                ],
            )
        )
        try:
            txh = await self._submit_tx(payload, "✅ Successfully registered machine")
            return {"status": "ok", "message": "Device registered", "tx_hash": txh}
        except Exception as e:
            err = f"❌ Registration failed: {e}"
            log.error(err)
            return {"status": "error", "message": err, "tx_hash": None}

    # -------------------------------------------------------------------------
    # Availability control
    # -------------------------------------------------------------------------
    async def _set_availability(self, is_available: bool):
        """Set on-chain availability; OFFLINE during shutdown, ONLINE on startup."""
        status_str = "ONLINE" if is_available else "OFFLINE"
        log.info(f"Attempting to set availability to {status_str} on-chain...")
        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace",
                "set_availability",
                [],
                [TransactionArgument(bool(is_available), Serializer.bool)],
            )
        )
        await self._submit_tx(payload, f"✅ Machine is now {status_str}")

    def shutdown(self):
        """Graceful shutdown → OFFLINE on-chain."""
        log.info("--- Graceful shutdown ---")
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._set_availability(False))
        finally:
            loop.close()
        log.info("✅ Shutdown complete.")

    # -------------------------------------------------------------------------
    # Backend WebSocket loop
    # -------------------------------------------------------------------------
    async def _ws_loop(self):
        """Maintain a persistent WS connection: ws://.../ws/{host_address}."""
        host_addr = str(self._acct.address())
        url = f"{self.ws_base}/{host_addr}"
        log.info(f"Connecting WS → {url}")  # <-- print the exact URL
        while True:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=6,  # <-- avoid long hangs
                    close_timeout=4,
                    max_size=None,
                ) as ws:
                    log.info(f"WS connected → {url}")
                    await ws.send(json.dumps({"kind": "hello", "addr": host_addr}))
                    while True:
                        raw = await ws.recv()
                        log.info(f"WS received: {raw}")

            except asyncio.CancelledError:
                log.info("WS loop cancelled; exiting.")
                break
            except Exception as e:
                log.warning(
                    f"WS disconnected/failed handshake ({e}); retrying in 3s..."
                )
                await asyncio.sleep(3)

    # -------------------------------------------------------------------------
    # Main run loop
    # -------------------------------------------------------------------------
    async def run(self):
        log.info("--- 🚀 Starting Host Agent ---")
        # Bring ONLINE (will fail if not registered)
        try:
            await self._set_availability(True)
        except Exception as e:
            log.error(f"Failed to set ONLINE: {e}")
            sys.exit(1)

        # Start backend WS client
        ws_task = asyncio.create_task(self._ws_loop())

        # Your normal job polling/command loops can be added here.
        try:
            while True:
                await asyncio.sleep(2)
                log.info("Heartbeat: agent alive")
        finally:
            ws_task.cancel()
            with contextlib.suppress(Exception):
                await ws_task
