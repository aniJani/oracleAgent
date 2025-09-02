import logging
import asyncio
import configparser
import requests
import pynvml
import psutil
import time
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
POLLING_INTERVAL_SECONDS = 15  # Check for new jobs every 15 seconds
PAYMENT_CLAIM_INTERVAL_SECONDS = 60  # Claim payment for active jobs every 60 seconds

# --- Metadata Gathering Functions (from your script) ---

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
    
    # --- THE FIX #1: This part was incorrectly removed and is now restored. ---
    cpu_cores = psutil.cpu_count(logical=True)
    ram_gb = round(psutil.virtual_memory().total / (1024**3))
    logging.info(f"Detected CPU Cores: {cpu_cores}, RAM: {ram_gb} GB")

    return {
        "gpu_model": gpu_model,
        "cpu_cores": cpu_cores,
        "ram_gb": ram_gb,
    }

# --- Oracle Agent Class ---

class OracleAgent:
    def __init__(self, config):
        self.rest_client = RestClient(config["aptos"]["node_url"])
        self.host_account = Account.load_key(config["aptos"]["private_key"])
        self.contract_address = config["aptos"]["contract_address"]
        self.price_per_second = int(config["host"]["price_per_second"])
        self.active_jobs: Set[int] = set()
        logging.info(f"Agent loaded for host account: {self.host_account.address()}")

    async def get_on_chain_listings(self) -> Dict[int, Any]:
        """Fetches all listings for this host from the blockchain."""
        try:
            payload = {
                "function": f"{self.contract_address}::marketplace::get_listings_by_host",
                "type_arguments": [],
                "arguments": [str(self.host_account.address())],
            }
            listings_data = (await self.rest_client.view(payload=payload))[0]
            return {int(l['id']): l for l in listings_data}
        except Exception:
            return {}

    async def list_machine_if_needed(self):
        """Checks if the host has any listings and creates one if not."""
        listings = await self.get_on_chain_listings()
        if listings:
            logging.info(f"Host already has {len(listings)} listing(s) on-chain. Monitoring...")
            return

        logging.info("No listings found for host. Creating a default listing...")
        
        # --- THE FIX #2: Correctly call the metadata functions and build the right transaction. ---
        aws_info = get_aws_metadata()
        function_name = ""
        function_arguments = []
        
        if aws_info:
            function_name = "list_cloud_machine"
            instance_type = aws_info["instance_type"]
            logging.info(f"Submitting list_cloud_machine for '{instance_type}'...")
            function_arguments = [
                TransactionArgument(instance_type, Serializer.str),
                TransactionArgument(self.price_per_second, Serializer.u64),
            ]
        else:
            function_name = "list_physical_machine"
            # Call the complete get_physical_metadata function
            physical_info = get_physical_metadata()
            logging.info(f"Submitting list_physical_machine for '{physical_info['gpu_model']}'...")
            function_arguments = [
                TransactionArgument(physical_info["gpu_model"], Serializer.str),
                TransactionArgument(physical_info["cpu_cores"], Serializer.u64),
                TransactionArgument(physical_info["ram_gb"], Serializer.u64),
                TransactionArgument(self.price_per_second, Serializer.u64),
            ]
        
        # Add the public key, which is the final argument for both functions
        pub_key_bytes = self.host_account.public_key().to_bytes()
        function_arguments.append(TransactionArgument(pub_key_bytes, Serializer.to_bytes))

        # Construct and submit the transaction with the correct function name and arguments
        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace", function_name, [], function_arguments
            )
        )
        await self.submit_transaction(payload, "Successfully listed machine!")

    async def claim_payment_for_job(self, job_id: int, renter_address: str):
        """Periodically calls the claim_payment function for an active job."""
        while job_id in self.active_jobs:
            logging.info(f"Attempting to claim payment for active Job ID: {job_id}")
            try:
                claim_timestamp = int(time.time())
                
                # 1. Serialize message: (job_id, claim_timestamp) as required by the contract
                serializer = Serializer()
                serializer.u64(job_id)
                serializer.u64(claim_timestamp)
                message = serializer.to_bytes()

                # 2. Sign the BCS-serialized message
                signature_bytes = self.host_account.sign(message).to_bytes()

                # 3. Construct payload for `claim_payment`
                # Note: The on-chain `Signature` struct is just a wrapper around bytes.
                # The SDK handles this conversion for us.
                payload = TransactionPayload(
                    EntryFunction.natural(
                        f"{self.contract_address}::escrow",
                        "claim_payment",
                        [],
                        [
                            TransactionArgument(job_id, Serializer.u64),
                            TransactionArgument(renter_address, Serializer.to_address),
                            TransactionArgument(claim_timestamp, Serializer.u64),
                            TransactionArgument(signature_bytes, Serializer.to_bytes),
                        ],
                    )
                )
                await self.submit_transaction(payload, f"Successfully claimed payment for Job ID {job_id}")

            except Exception as e:
                logging.error(f"Failed to claim payment for Job ID {job_id}, job might be over. Stopping claims.", exc_info=True)
                self.active_jobs.discard(job_id) # Stop trying if it fails
                return # Exit the loop for this job
            
            await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)

    async def run(self):
        """Main monitoring loop of the oracle agent."""
        # Initial check to list a machine if the host has none
        await self.list_machine_if_needed()

        while True:
            logging.info("Polling for new rentals...")
            on_chain_listings = await self.get_on_chain_listings()

            for listing_id, listing in on_chain_listings.items():
                if not listing['is_available']:
                    job_id_vec = listing.get('active_job_id', {}).get('vec', [])
                    if job_id_vec:
                        job_id = int(job_id_vec[0])
                        if job_id not in self.active_jobs:
                            logging.info(f"New rental detected! Starting job processor for Job ID: {job_id}")
                            self.active_jobs.add(job_id)
                            
                            # To get the renter's address, we need to query the job itself.
                            # This requires a view function `get_job(job_id)` on the escrow contract.
                            # For the demo, we will assume such a function exists.
                            # You would need to add this to escrow.move:
                            # #[view] public fun get_job(job_id: u64): Job acquires EscrowVault { ... }
                            job_details_payload = {
                                "function": f"{self.contract_address}::escrow::get_job",
                                "type_arguments": [], "arguments": [str(job_id)]
                            }
                            try:
                                job_data = (await self.rest_client.view(job_details_payload))[0]
                                renter_address = job_data['renter_address']
                                asyncio.create_task(self.claim_payment_for_job(job_id, renter_address))
                            except Exception:
                                logging.error(f"Could not fetch details for job {job_id} to start payment claims.")
                                self.active_jobs.discard(job_id)
            
            await asyncio.sleep(POLLING_INTERVAL_SECONDS)

    async def submit_transaction(self, payload: TransactionPayload, success_message: str):
        """Helper to sign, submit, and wait for a transaction."""
        try:
            signed_txn = await self.rest_client.create_bcs_signed_transaction(
                self.host_account, payload
            )
            tx_hash = await self.rest_client.submit_bcs_transaction(signed_txn)
            await self.rest_client.wait_for_transaction(tx_hash)
            logging.info(f"{success_message} Transaction hash: {tx_hash}")
        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            raise # Re-raise the exception to be handled by the caller

async def main():
    """Main function to configure and run the oracle agent."""
    logging.info("Loading configuration...")
    config = configparser.ConfigParser()
    try:
        config.read_file(open("config.ini"))
        # Validate that all necessary keys are present
        _ = config["aptos"]["private_key"]
        _ = config["aptos"]["contract_address"]
        _ = config["aptos"]["node_url"]
        _ = config["host"]["price_per_second"]
    except (KeyError, FileNotFoundError) as e:
        logging.error(f"Configuration error in 'config.ini': {e}. Please ensure the file exists and is correctly formatted.")
        return

    agent = OracleAgent(config)
    await agent.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Agent shut down by user.")