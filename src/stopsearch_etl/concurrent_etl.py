from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from .api import PoliceApiClient, ApiError
from .etl_service import EtlService
from .backfill_service import BackfillResult


class ConcurrentEtlService:
    """Run ETL for many months at the same time"""

    def __init__(self, api_client: PoliceApiClient, etl_service: EtlService, max_workers: int = 3):
        """
        Initialize concurrent ETL service.

        Args:
            api_client: used to get which months exist
            etl_service: does the work for each month
            max_workers: how many threads to run at once
        """
        self.api_client = api_client
        self.etl_service = etl_service
        self.max_workers = max_workers

    def backfill_force_concurrent(self, force: str) -> BackfillResult:
        """
        Backfill all months for a force using threads.

        Returns:
            BackfillResult with summary of the operation
        """
        result = BackfillResult(
            force=force,
            total_records=0,
            months_processed=0,
            months_failed=0
        )

        try:
            # get list of months to process
            available_months = self.api_client.get_available_months(force)

            if not available_months:
                return result

            print(f"Processing {len(available_months)} months for {force} with {self.max_workers} workers")
            # TODO: use logging instead of print

            # start work in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # submit all ETL tasks
                future_to_month = {
                    executor.submit(self._process_month, force, month): month
                    for month in available_months
                }

                # Collect results as they complete
                for future in as_completed(future_to_month):
                    month = future_to_month[future]
                    try:
                        records_saved = future.result()
                        result.total_records += records_saved
                        result.months_processed += 1
                        print(f"Completed {force} {month}: {records_saved} records")

                    except Exception as e:
                        result.months_failed += 1
                        print(f"Failed {force} {month}: {e}")
                        # TODO: remember which month failed so we can retry later

        except ApiError as e:
            print(f"Failed to get available months for {force}: {e}")
            # TODO: decide if we should re-raise here

        return result

    def _process_month(self, force: str, month: str) -> int:
        """
        Run ETL for one month.

        Returns:
            Number of records processed
        """
        return self.etl_service.extract_transform_load(force, month)