import asyncio
import configparser
import logging

from agent import HostAgent

async def main():
    logging.info("Loading configuration from config.ini...")
    config = configparser.ConfigParser()
    try:
        config.read_file(open("config.ini"))
        _ = config["aptos"]["private_key"]
        _ = config["aptos"]["contract_address"]
        _ = config["aptos"]["node_url"]
        _ = config["host"]["price_per_second"]
    except (KeyError, FileNotFoundError) as e:
        logging.error(
            f"Configuration error in 'config.ini': {e}. Please ensure the file exists and has required keys."
        )
        return
    agent = HostAgent(config)
    await agent.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nAgent shut down by user.")
