import logging
import asyncio
import configparser
import requests
import pynvml
import psutil  # We will now use this for CPU and RAM

from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionPayload,
    TransactionArgument,
)
from aptos_sdk.bcs import Serializer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

AWS_METADATA_URL = "http://169.254.169.254/latest/meta-data/"  # correct link-local IP


def get_aws_metadata():
    """Try to fetch minimal EC2 instance metadata."""
    try:
        r = requests.get(f"{AWS_METADATA_URL}instance-type", timeout=1)
        if r.status_code == 200:
            logging.info("AWS environment detected.")
            return {"instance_type": r.text}
    except requests.exceptions.RequestException:
        logging.info("Not an AWS environment.")
    return None


def get_physical_metadata():
    """
    Gather full hardware specifications from a physical (bare-metal/laptop) machine.
    """
    logging.info("Physical environment detected. Gathering full hardware specs...")
    gpu_model = "N/A"
    try:
        pynvml.nvmlInit()
        handle = pynvml.nvmlDeviceGetHandleByIndex(0)
        name = pynvml.nvmlDeviceGetName(handle)
        gpu_model = (
            name.decode("utf-8") if isinstance(name, (bytes, bytearray)) else str(name)
        )
        logging.info(f"Found GPU: {gpu_model}")
    except pynvml.NVMLError:
        logging.warning("Could not detect an NVIDIA GPU.")
    finally:
        try:
            pynvml.nvmlShutdown()
        except Exception:
            pass

    # --- UPGRADED LOGIC: Get CPU and RAM specs ---
    cpu_cores = psutil.cpu_count(logical=True)
    ram_gb = round(psutil.virtual_memory().total / (1024**3))
    logging.info(f"Detected CPU Cores: {cpu_cores}, RAM: {ram_gb} GB")

    return {
        "gpu_model": gpu_model,
        "cpu_cores": cpu_cores,
        "ram_gb": ram_gb,
    }


async def main():
    """Main function to run the oracle agent."""
    # 1) Load configuration (no changes here)
    logging.info("Loading configuration...")
    config = configparser.ConfigParser()
    config.read("config.ini")
    try:
        private_key_hex = config["aptos"]["private_key"]
        contract_address = config["aptos"]["contract_address"]
        node_url = config["aptos"]["node_url"]
        price_per_second = int(config["host"]["price_per_second"])
    except KeyError as e:
        logging.error(f"Config file 'config.ini' is missing a key: {e}")
        return

    # 2) Setup Aptos client & account (no changes here)
    try:
        rest_client = RestClient(node_url)
        logging.info(f"Connected to Aptos node: {node_url}")
        host_account = Account.load_key(private_key_hex)
        logging.info(f"Agent loaded for host account: {host_account.address()}")
    except Exception:
        logging.error("Failed to initialize Aptos account or client.", exc_info=True)
        return

    # 3) Detect environment and build the correct payload for the V2 contract
    aws_info = get_aws_metadata()

    if aws_info:
        # --- PATH FOR CLOUD MACHINES ---
        instance_type = aws_info["instance_type"]
        logging.info(f"Building payload for V2 cloud machine: {instance_type}")

        entry_func = EntryFunction.natural(
            f"{contract_address}::marketplace",
            "list_cloud_machine",  # <-- CALLS THE NEW, DEDICATED CLOUD FUNCTION
            [],
            [
                TransactionArgument(instance_type, Serializer.str),
                TransactionArgument(price_per_second, Serializer.u64),
            ],
        )
        log_message = f"Submitting list_cloud_machine for '{instance_type}'..."

    else:
        # --- PATH FOR PHYSICAL MACHINES ---
        physical_info = get_physical_metadata()
        logging.info(f"Building payload for V2 physical machine: {physical_info}")

        entry_func = EntryFunction.natural(
            f"{contract_address}::marketplace",
            "list_physical_machine",  # <-- CALLS THE NEW, DEDICATED PHYSICAL FUNCTION
            [],
            [
                TransactionArgument(physical_info["gpu_model"], Serializer.str),
                TransactionArgument(physical_info["cpu_cores"], Serializer.u64),
                TransactionArgument(physical_info["ram_gb"], Serializer.u64),
                TransactionArgument(price_per_second, Serializer.u64),
            ],
        )
        log_message = (
            f"Submitting list_physical_machine for '{physical_info['gpu_model']}'..."
        )

    payload = TransactionPayload(entry_func)

    # 5) Sign, submit, wait (no changes here)
    try:
        logging.info(log_message)
        signed_txn = await rest_client.create_bcs_signed_transaction(
            host_account, payload
        )
        tx_hash = await rest_client.submit_bcs_transaction(signed_txn)
        await rest_client.wait_for_transaction(tx_hash)
        logging.info(f"Successfully listed machine! Transaction hash: {tx_hash}")
    except Exception:
        logging.error("Transaction failed.", exc_info=True)
    finally:
        await rest_client.close()


if __name__ == "__main__":
    asyncio.run(main())
