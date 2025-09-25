import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any
from datetime import datetime

# Set up structured logging
logger = logging.getLogger(__name__)


@dataclass
class EtlMetrics:
    """Collect ETL metrics and log them"""
    total_records_ingested: int = 0
    total_records_deduplicated: int = 0
    total_batches_processed: int = 0
    total_batches_failed: int = 0
    failed_batches: List[Dict[str, Any]] = field(default_factory=list)
    # TODO: if this grows large, cap the list size or stream to storage


class MetricsCollector:
    """Collects and tracks ETL operation metrics with structured logging."""

    def __init__(self):
        self.metrics = EtlMetrics()
        # TODO: thread-safety: add a lock if called from multiple threads

    def record_successful_batch(self, force: str, month: str, records_ingested: int, records_deduplicated: int) -> None:
        """Record a successful batch processing operation."""
        self.metrics.total_records_ingested += records_ingested
        self.metrics.total_records_deduplicated += records_deduplicated
        self.metrics.total_batches_processed += 1

        # structured log (extra adds fields to the record)
        # TODO: consider JSON formatter so these are easy to parse
        logger.info(
            "Batch completed successfully",
            extra={
                "force": force,
                "month": month,
                "records_ingested": records_ingested,
                "records_deduplicated": records_deduplicated,
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    def record_failed_batch(self, force: str, month: str, error_message: str) -> None:
        """Record a failed batch processing operation."""
        self.metrics.total_batches_failed += 1

        failure_info = {
            "batch_id": f"{force}-{month}",
            "force": force,
            "month": month,
            "error": error_message,
            "timestamp": datetime.utcnow().isoformat()
        }

        self.metrics.failed_batches.append(failure_info)

        # Structured logging
        logger.error(
            "Batch processing failed",
            extra=failure_info
        )

    def get_current_metrics(self) -> EtlMetrics:
        """Return current totals"""
        return self.metrics

    def reset(self) -> None:
        """Reset all counters"""
        self.metrics = EtlMetrics()

    def log_summary(self) -> None:
        """log summary of current metrics."""
        logger.info(
            "ETL operation summary",
            extra={
                "total_records_ingested": self.metrics.total_records_ingested,
                "total_records_deduplicated": self.metrics.total_records_deduplicated,
                "total_batches_processed": self.metrics.total_batches_processed,
                "total_batches_failed": self.metrics.total_batches_failed,
                "success_rate": self._calculate_success_rate(),
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    def _calculate_success_rate(self) -> float:
        """Return success ratio"""
        total_batches = self.metrics.total_batches_processed + self.metrics.total_batches_failed
        if total_batches == 0:
            return 0.0
        # TODO: if you want percent, multiply by 100.0 in the caller/formatter
        return self.metrics.total_batches_processed / total_batches