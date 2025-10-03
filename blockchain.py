import logging
import time
import json
from typing import Dict, Any

from aptos_sdk.transactions import (
    EntryFunction,
    TransactionArgument,
    TransactionPayload,
)
from aptos_sdk.bcs import Serializer
from aptos_sdk.async_client import ApiError

from constants import PAYMENT_CLAIM_INTERVAL_SECONDS

logger = logging.getLogger(__name__)


class ChainClient:
    def __init__(
        self,
        rest_client,
        host_account,
        contract_address,
        price_per_second,
        transaction_lock,
    ):
        self.rest_client = rest_client
        self.host_account = host_account
        self.contract_address = contract_address
        self.price_per_second = price_per_second
        self.transaction_lock = transaction_lock
        self.active_jobs = None  # set externally
        self.stop_container_callback = None  # external injection
        self.session_start_times = None  # external injection

    async def get_on_chain_listings(self) -> Dict[int, Any]:
        try:
            # Fix typo
            resource_type = f"{self.contract_address}::marketplace::ListingManager"
            response = await self.rest_client.account_resource(
                str(self.host_account.address()), resource_type
            )
            listings_data = response.get("data", {}).get("listings", [])
            result = {int(l["id"]): l for l in listings_data}
            logger.info(f"📋 Found {len(result)} existing listing(s) for this host.")
            return result
        except ApiError as e:
            if "resource not found" in str(e).lower() or e.status_code == 404:
                logger.info("📋 No ListingManager resource found (no listings yet).")
                return {}
            logger.error(f"API error fetching listings: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching listings: {e}", exc_info=True)
            raise

    async def register_on_chain_if_needed(self, specs: Dict[str, Any]) -> None:
        try:
            existing = await self.get_on_chain_listings()
        except Exception as e:
            logger.error(f"Cannot verify existing listings: {e}")
            logger.warning("⚠️ Skipping registration to avoid duplicate listing errors.")
            return

        if existing:
            logger.info(
                f"✅ Host already has {len(existing)} listing(s) on-chain. Skipping registration."
            )
            return

        if specs["is_physical"]:
            function_name = "list_physical_machine"
            cpu_cores = int(specs.get("cpu_cores", 1))
            ram_gb = int(specs.get("ram_gb", 1))
            arguments = [
                TransactionArgument(specs["identifier"], Serializer.str),
                TransactionArgument(cpu_cores, Serializer.u64),
                TransactionArgument(ram_gb, Serializer.u64),
                TransactionArgument(self.price_per_second, Serializer.u64),
                TransactionArgument(
                    self.host_account.public_key().to_bytes(), Serializer.to_bytes
                ),
            ]
        else:
            function_name = "list_cloud_machine"
            arguments = [
                TransactionArgument(specs["identifier"], Serializer.str),
                TransactionArgument(self.price_per_second, Serializer.u64),
                TransactionArgument(
                    self.host_account.public_key().to_bytes(), Serializer.to_bytes
                ),
            ]
        logger.info(
            f"🔗 Registering '{specs['identifier']}' on-chain using {function_name}..."
        )
        payload = TransactionPayload(
            EntryFunction.natural(
                f"{self.contract_address}::marketplace", function_name, [], arguments
            )
        )
        await self.submit_transaction(
            payload, f"✅ Successfully listed '{specs['identifier']}' on-chain!"
        )

    async def claim_payment_for_job(self, job_id: int):
        while job_id in self.active_jobs:
            logger.info(f"Attempting to claim payment for active Job ID: {job_id}")
            try:
                raw = await self.rest_client.view(
                    function=f"{self.contract_address}::escrow::get_job",
                    type_arguments=[],
                    arguments=[str(job_id)],
                )
                parsed = json.loads(raw.decode("utf-8"))
                logger.info(
                    f"DEBUG: Parsed response from get_job for Job ID {job_id}: {parsed}"
                )
                if not parsed or not isinstance(parsed[0], dict):
                    logger.error("Unexpected data format from get_job.")
                    self.active_jobs.discard(job_id)
                    return
                job = parsed[0]
                is_active = bool(job.get("is_active", False))
                start_time = int(job["start_time"])
                max_end_time = int(job["max_end_time"])
                total_escrow_amount = int(job["total_escrow_amount"])
                claimed_amount = int(job["claimed_amount"])
                if not is_active:
                    logger.info(f"Job {job_id} is inactive; stopping claims.")
                    self.active_jobs.discard(job_id)
                    self.stop_container_callback(job_id)
                    return
                duration = max_end_time - start_time
                if duration <= 0:
                    logger.warning(f"Job {job_id} has non-positive duration; stopping.")
                    self.active_jobs.discard(job_id)
                    self.stop_container_callback(job_id)
                    return
                now = int(time.time())
                claim_timestamp = min(max(now, start_time), max_end_time)
                price_per_second = total_escrow_amount // duration
                accrued = max(0, claim_timestamp - start_time) * price_per_second
                if accrued > total_escrow_amount:
                    accrued = total_escrow_amount
                if accrued <= claimed_amount:
                    if claim_timestamp >= max_end_time:
                        logger.info(
                            f"Job {job_id}: fully claimed or at end; stopping claims."
                        )
                        self.stop_container_callback(job_id)
                        self.active_jobs.discard(job_id)
                        return
                    logger.info(f"Job {job_id}: nothing new to claim yet.")
                    await asyncio.sleep(
                        PAYMENT_CLAIM_INTERVAL_SECONDS
                    )  # noqa: F821 (asyncio imported in agent)
                    continue
                final_session_duration = 0
                if claim_timestamp >= max_end_time:
                    start_ts = (
                        self.session_start_times.get(job_id)
                        if self.session_start_times
                        else None
                    )
                    if start_ts:
                        final_session_duration = int(time.time() - start_ts)
                    else:
                        final_session_duration = int(max_end_time - start_time)
                try:
                    payload = TransactionPayload(
                        EntryFunction.natural(
                            f"{self.contract_address}::escrow",
                            "claim_payment",
                            [],
                            [
                                TransactionArgument(job_id, Serializer.u64),
                                TransactionArgument(claim_timestamp, Serializer.u64),
                                TransactionArgument(
                                    final_session_duration, Serializer.u64
                                ),
                            ],
                        )
                    )
                    await self.submit_transaction(
                        payload, f"Successfully claimed payment for Job ID {job_id}"
                    )
                except Exception as e:
                    if "NUMBER_OF_ARGUMENTS_MISMATCH" in str(e):
                        logger.warning(
                            "Older claim_payment ABI; retrying with 2 arguments."
                        )
                        payload2 = TransactionPayload(
                            EntryFunction.natural(
                                f"{self.contract_address}::escrow",
                                "claim_payment",
                                [],
                                [
                                    TransactionArgument(job_id, Serializer.u64),
                                    TransactionArgument(
                                        claim_timestamp, Serializer.u64
                                    ),
                                ],
                            )
                        )
                        await self.submit_transaction(
                            payload2,
                            f"Successfully claimed payment (2-arg ABI) for Job ID {job_id}",
                        )
                    else:
                        raise
                if claim_timestamp >= max_end_time:
                    logger.info(
                        f"Job {job_id}: final claim submitted; stopping session."
                    )
                    self.stop_container_callback(job_id)
                    self.active_jobs.discard(job_id)
                    return
            except ApiError as e:
                logger.error(
                    f"API Error claiming payment for Job ID {job_id}: {e}",
                    exc_info=True,
                )
                self.stop_container_callback(job_id)
                self.active_jobs.discard(job_id)
                return
            except Exception:
                logger.error(
                    f"Generic error claiming payment for Job ID {job_id}", exc_info=True
                )
                self.stop_container_callback(job_id)
                self.active_jobs.discard(job_id)
                return
            await asyncio.sleep(PAYMENT_CLAIM_INTERVAL_SECONDS)  # noqa: F821

    async def submit_transaction(
        self, payload: TransactionPayload, success_message: str
    ):
        async with self.transaction_lock:
            try:
                signed_txn = await self.rest_client.create_bcs_signed_transaction(
                    self.host_account, payload
                )
                tx_hash = await self.rest_client.submit_bcs_transaction(signed_txn)
                await self.rest_client.wait_for_transaction(tx_hash)
                logger.info(f"{success_message} | Transaction: {tx_hash}")
            except Exception as e:
                logger.error(f"Transaction submission failed: {e}")
                raise
