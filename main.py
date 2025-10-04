# main.py (Final Version)

import argparse
import asyncio
import configparser
import logging
import os
from pathlib import Path
import sys
import time
from typing import Dict

from agent import HostAgent
import agent
from config import load_all_settings as load_config
from dotenv import load_dotenv

load_dotenv()

async def _run_agent_async():
    cfg = load_config()
    agent = HostAgent(cfg)
    try:
        await agent.run()
    finally:
        agent.shutdown()

async def _register_once() -> Dict:
    # (This function is already correct)
    try:
        cfg = load_config()
        agent = HostAgent(cfg)
        result = await agent.register_device_if_needed()
    except Exception as e:
        result = {"status": "error", "message": f"An unexpected error occurred: {e}"}
    if result["status"] == "ok":
        msg = result["message"]; tx = result.get("tx_hash")
        print(f"REGISTER_OK tx={tx}" if tx else f"REGISTER_OK {msg}")
    else:
        print(f"REGISTER_ERR {result['message']}")
    return result

def run_agent_main():
    """Main entry point for the agent loop."""
    try:
        asyncio.run(_run_agent_async())
    except Exception as e:
        logging.critical(f"Agent loop failed with a critical error: {e}", exc_info=True)
        # In a packaged app, it's better to log than to exit silently
        time.sleep(10) # Keep console open for a moment to see the error

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s") 
    parser = argparse.ArgumentParser(description="Unified Compute Host Agent")
    parser.add_argument("--register", action="store_true", help="Register this device on-chain and exit")
    parser.add_argument("--run-agent", action="store_true", help="Run the agent loop (for internal use by the GUI)")
    args = parser.parse_args()

    if args.register:
        asyncio.run(_register_once())
    elif args.run-agent:
        run_agent_main()
    else:
        # Default action if no flags are given is to run the agent
        run_agent_main()