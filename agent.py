import logging
import asyncio
import configparser
import requests
import pynvml
import psutil

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

AWS_METADATA_URL = "http://169.254.169.254/latest/meta-data/"


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

    # 3) Detect environment
    aws_info = get_aws_metadata()

    # 4) Prepare the arguments for our single `list_machine` function
    function_arguments = []

    if aws_info:
        instance_type = aws_info["instance_type"]
        log_message = f"Submitting list_machine for cloud instance '{instance_type}'..."

        function_arguments = [
            TransactionArgument(False, Serializer.bool),  # is_physical = false
            TransactionArgument(instance_type, Serializer.str),
            TransactionArgument(price_per_second, Serializer.u64),
        ]
    else:
        physical_info = get_physical_metadata()
        log_message = f"Submitting list_machine for physical machine '{physical_info['gpu_model']}'..."

        # Arguments for `list_machine(is_physical: bool, gpu_or_instance_type: string, ...)`
        function_arguments = [
            TransactionArgument(True, Serializer.bool),  # is_physical = true
            TransactionArgument(physical_info["gpu_model"], Serializer.str),
            TransactionArgument(price_per_second, Serializer.u64),
        ]

    # Add the host's public key as the final argument for BOTH paths
    # (assuming your final contract requires it for signature verification)
    pub_key_bytes = host_account.public_key().to_bytes()
    function_arguments.append(TransactionArgument(pub_key_bytes, Serializer.to_bytes))

    # 5) Construct the final EntryFunction and Payload
    entry_func = EntryFunction.natural(
        f"{contract_address}::marketplace",
        "list_machine",  # <-- CALLS THE CORRECT, SINGLE FUNCTION
        [],
        function_arguments,
    )
    payload = TransactionPayload(entry_func)

    # 6) Sign, submit, wait
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
