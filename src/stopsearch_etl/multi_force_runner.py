from dataclasses import dataclass, field
from typing import List

from .backfill_service import BackfillService


@dataclass
class MultiForceRunSummary:
    """Summary of a multi force backfill run"""
    total_records: int = 0
    total_months_processed: int = 0
    total_months_failed: int = 0
    forces_completed: int = 0
    forces_failed: int = 0
    failed_forces: List[str] = field(default_factory=list)


class MultiForceRunner:
    """Orchestrates backfill operations across multiple police forces"""

    def __init__(self, backfill_service: BackfillService):
        self.backfill_service = backfill_service

    def run_backfill(self, forces: List[str]) -> MultiForceRunSummary:
        """
        Run backfill for multiple forces in list

        Args:
            forces: List of police force ids
        """
        summary = MultiForceRunSummary()

        if not forces:
            return summary

        print(f"Starting backfill for {len(forces)} forces...")

        for force in forces:
            try:
                print(f"Processing force: {force}")

                result = self.backfill_service.backfill_force(force)

                # roll up numbers from the force result
                summary.total_records += result.total_records
                summary.total_months_processed += result.months_processed
                summary.total_months_failed += result.months_failed
                summary.forces_completed += 1

                print(f"Completed {force}: {result.total_records} records from {result.months_processed} months")
                # TODO: progress callback / hook for UI

            except Exception as e:
                summary.forces_failed += 1
                summary.failed_forces.append(force)
                print(f"Failed to process force {force}: {e}")
                # TODO: consider fail-fast flag to stop on first error
                # Continue with other forces

        print(f"Backfill complete: {summary.total_records} total records, {summary.forces_completed}/{len(forces)} forces successful")

        return summary