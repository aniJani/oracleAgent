import argparse
import asyncio
import configparser
import logging
import sys
from typing import Dict

from agent import HostAgent

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


def load_config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    try:
        with open("config.ini", "r", encoding="utf-8") as f:
            cfg.read_file(f)
        # Validate required keys
        _ = cfg["aptos"]["private_key"]
        _ = cfg["aptos"]["contract_address"]
        _ = cfg["aptos"]["node_url"]
        _ = cfg["host"]["price_per_second"]
        return cfg
    except (KeyError, FileNotFoundError) as e:
        logging.error(f"Configuration error in 'config.ini': {e}")
        sys.exit(1)


async def _run_agent():
    cfg = load_config()
    agent = HostAgent(cfg)
    try:
        await agent.run()
    finally:
        agent.shutdown()


async def _register_once() -> Dict:
    """
    Performs the on-chain registration. Returns a result dictionary.
    This version does NOT exit, making it safe to call from other modules.
    """
    cfg = load_config()
    agent = HostAgent(cfg)
    result = await agent.register_device_if_needed()
    
    # Print a single-line outcome for CLI backward compatibility
    if result["status"] == "ok":
        msg = result["message"]
        tx = result.get("tx_hash")
        if tx:
            print(f"REGISTER_OK tx={tx}")
        else:
            print(f"REGISTER_OK {msg}")
    else:
        print(f"REGISTER_ERR {result['message']}")
        
    return result


def main():
    parser = argparse.ArgumentParser(description="Unified Compute Host Agent")
    parser.add_argument(
        "--register", action="store_true", help="Register this device on-chain and exit"
    )
    args = parser.parse_args()

    if args.register:
        asyncio.run(_register_once())
    else:
        asyncio.run(_run_agent())


if __name__ == "__main__":
    main()
