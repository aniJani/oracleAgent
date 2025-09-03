import logging
import asyncio
import configparser
import requests
import pynvml
import psutil
import time
import sys
import docker
from typing import Dict, Any, Set

from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionPayload,
    TransactionArgument,
)
from aptos_sdk.bcs import Serializer

# --- Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
AWS_METADATA_URL = "http://169.254.169.254/latest/meta-data/"
POLLING_INTERVAL_SECONDS = 15
PAYMENT_CLAIM_INTERVAL_SECONDS = 60
# Define the base Docker image required for jobs
PYTORCH_IMAGE = "pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime"


class HostAgent:
    def __init__(self, config):
        self.rest_client = RestClient(config["aptos"]["node_url"])
        self.host_account = Account.load_key(config["aptos"]["private_key"])
        self.contract_address = config["aptos"]["contract_address"]
        self.price_per_second = int(config["host"]["price_per_second"])
        self.active_jobs: Set[int] = set()
        self.docker_client = None  # Will be initialized
        logging.info(f"Host Agent loaded for account: {self.host_account.address()}")

    def ensure_docker(self):
        """Checks if Docker is running and initializes the client."""
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
            logging.info("✅ Docker is running and accessible.")
        except Exception:
            logging.error("❌ Critical Error: Docker is not running or not installed.")
            logging.error("Please install Docker and ensure the Docker daemon is active.")
            sys.exit(1)

    def prepare_base_image(self):
        """Pulls the specified PyTorch Docker image if not present."""
        logging.info(f"🐳 Checking for Docker image: {PYTORCH_IMAGE}...")
        try:
            self.docker_client.images.get(PYTORCH_IMAGE)
            logging.info("   - Image already exists locally.")
        except docker.errors.ImageNotFound:
            logging.info(f"   - Image not found. Pulling from Docker Hub (this may take several minutes)...")
            try:
                self.docker_client.images.pull(PYTORCH_IMAGE)
                logging.info("✅ Successfully pulled PyTorch base image.")
            except Exception as e:
                logging.error(f"❌ Critical Error: Failed to pull Docker image: {e}")
                sys.exit(1)

    def detect_environment_and_specs(self) -> Dict[str, Any]:
        """Detects environment (AWS/Physical) and gathers hardware specs."""
        logging.info("🔎 Detecting environment and hardware specifications...")
        try:
            r = requests.get(f"{AWS_METADATA_URL}instance-type", timeout=1)
            if r.status_code == 200:
                instance_type = r.text
                logging.info(f"   - Cloud Environment: AWS | Instance Type: {instance_type}")
                return {"is_physical": False, "identifier": instance_type}
        except requests.exceptions.RequestException:
            logging.info("   - Not an AWS environment. Assuming physical machine.")

        gpu_model = "N/A"
        try:
            pynvml.nvmlInit()
            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            gpu_model = pynvml.nvmlDeviceGetName(handle)
            logging.info(f"   - GPU Found: {gpu_model}")
        except pynvml.NVMLError:
            logging.error("❌ Critical Error: Could not detect an NVIDIA GPU.")
            sys.exit(1)
        finally:
            try:
                pynvml.nvmlShutdown()
            except pynvml.NVMLError:
                pass
        
        return {"is_physical": True, "identifier": gpu_model}

    async def get_on_chain_listings(self) -> Dict[int, Any]:
        """Fetches all listings for this host directly from its account resource."""
        try:
            resource_type = f"{self.contract_address}::marketplace::ListingManager"
            response = await self.rest_client.account_resource(
                str(self.host_account.address()), resource_type
            )
            listings_data = response.get("data", {}).get("listings", [])
            return {int(l['id']): l for l in listings_data}
        except Exception:
            # This can happen if the host has never listed before, which is fine
            return {}

    async def register_on_chain_if_needed(self, specs: Dict[str, Any]):
        """Lists the machine on-chain if no listings already exist."""
        listings = await self.get_on_chain_listings()
        if listings:
            logging.info(f"Host already has {len(listings)} listing(s) on-chain. Skipping registration.")
            return

        logging.info(f"🔗 No listings found. Registering '{specs['identifier']}' on the blockchain...")
        
        host_public_key_bytes = self.host_account.public_key().to_bytes()

        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace",
                "list_machine",
                [],
                [
                    TransactionArgument(specs['is_physical'], Serializer.bool),
                    TransactionArgument(specs['identifier'], Serializer.str),
                    TransactionArgument(self.price_per_second, Serializer.u64),
                    TransactionArgument(host_public_key_bytes, Serializer.to_bytes),
                ],
            )
        )
        await self.submit_transaction(payload, f"✅ Successfully listed '{specs['identifier']}' on-chain!")

    async def claim_payment_for_job(self, job_id: int):
        """Periodically calls the claim_payment function for an active job."""
        while job_id in self.active_jobs:
            logging.info(f"Attempting to claim payment for active Job ID: {job_id}")
            try:
                claim_timestamp = int(time.time())
                
                serializer = Serializer()
                serializer.u64(job_id)
                serializer.u64(claim_timestamp)
                message_to_sign = serializer.to_bytes()

                signature = self.host_account.sign(message_to_sign)
                signature_bytes = signature.to_bytes()

                payload = TransactionPayload(
                    EntryFunction.natural(
                        f"{self.contract_address}::escrow",
                        "claim_payment",
                        [],
                        [
                            TransactionArgument(job_id, Serializer.u64),
                            TransactionArgument(claim_timestamp, Serializer.u64),
                            TransactionArgument(signature, Serializer.ed25519_signature), # Using the correct type
                        ],
                    )
                )
                await self.submit_transaction(payload, f"Successfully claimed payment for Job ID {job_id}")

            except Exception as e:
                logging.error(f"Failed to claim payment for Job ID {job_id}. The job may have ended. Stopping claims.", exc_info=True)
                self.active_jobs.discard(job_id)
                return
            
            await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)

    async def run(self):
        """Main setup and monitoring loop of the host agent."""
        logging.info("--- 🚀 Starting Unified Compute Host Agent ---")
        
        # --- 1. Initial Setup Sequence ---
        self.ensure_docker()
        specs = self.detect_environment_and_specs()
        self.prepare_base_image()
        await self.register_on_chain_if_needed(specs)

        logging.info("\n✅ Setup complete. Agent is now monitoring for jobs...")

        # --- 2. Main Polling Loop ---
        while True:
            logging.info("Polling for new rentals...")
            try:
                on_chain_listings = await self.get_on_chain_listings()

                for listing_id, listing in on_chain_listings.items():
                    # Check if listing is rented
                    if not listing['is_available']:
                        job_id_vec = listing.get('active_job_id', {}).get('vec', [])
                        if job_id_vec:
                            job_id = int(job_id_vec[0])
                            # If this is a new job we haven't seen, start the payment loop
                            if job_id not in self.active_jobs:
                                logging.info(f"🎉 New rental detected! Starting payment processor for Job ID: {job_id}")
                                self.active_jobs.add(job_id)
                                asyncio.create_task(self.claim_payment_for_job(job_id))
            except Exception as e:
                logging.error(f"An error occurred during the polling loop: {e}", exc_info=True)

            await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    async def submit_transaction(self, payload: TransactionPayload, success_message: str):
        """Helper to sign, submit, and wait for a transaction."""
        try:
            signed_txn = await self.rest_client.create_bcs_signed_transaction(
                self.host_account, payload
            )
            tx_hash = await self.rest_client.submit_bcs_transaction(signed_txn)
            await self.rest_client.wait_for_transaction(tx_hash)
            logging.info(f"{success_message} | Transaction: {tx_hash}")
        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            raise

async def main():
    """Main function to configure and run the agent."""
    logging.info("Loading configuration from config.ini...")
    config = configparser.ConfigParser()
    try:
        config.read_file(open("config.ini"))
        _ = config["aptos"]["private_key"]
        _ = config["aptos"]["contract_address"]
        _ = config["aptos"]["node_url"]
        _ = config["host"]["price_per_second"]
    except (KeyError, FileNotFoundError) as e:
        logging.error(f"Configuration error in 'config.ini': {e}. Please ensure the file exists and is correctly formatted.")
        return

    agent = HostAgent(config)
    await agent.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nAgent shut down by user.")