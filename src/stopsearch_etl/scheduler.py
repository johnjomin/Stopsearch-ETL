import logging
from datetime import time
from typing import List, Optional

from apscheduler.schedulers.background import BackgroundScheduler

from .multi_force_runner import MultiForceRunner, MultiForceRunSummary

logger = logging.getLogger(__name__)


class EtlScheduler:
    """Run ETL every day at a set time"""

    def __init__(self, multi_force_runner: MultiForceRunner, forces: List[str],
                 schedule_time: time = time(2, 0)):
        """
        Initialize the ETL scheduler.

        Args:
            multi_force_runner: runs ETL for many forces
            forces: which forces to process
            schedule_time: daily time to run (default 02:00)
        """
        self.multi_force_runner = multi_force_runner
        self.forces = forces
        self.schedule_time = schedule_time
        self.scheduler: Optional[BackgroundScheduler] = None

    def start(self) -> None:
        """Start background scheduler"""
        if self.scheduler is not None:
            logger.warning("Scheduler is already running")
            return

        self.scheduler = BackgroundScheduler()

        # schedule a daily job
        # TODO: set timezone explicitly if needed (e.g., Europe/London)
        self.scheduler.add_job(
            func=self._run_etl_job,
            trigger='cron',
            hour=self.schedule_time.hour,
            minute=self.schedule_time.minute,
            id='daily_etl_job',
            name='Daily Stop & Search ETL'
        )

        self.scheduler.start()
        logger.info(f"ETL scheduler started, will run daily at {self.schedule_time}")

    def stop(self) -> None:
        """Stop the scheduler"""
        if self.scheduler is not None:
            self.scheduler.shutdown()
            self.scheduler = None
            logger.info("ETL scheduler stopped")

    def run_once(self) -> MultiForceRunSummary:
        """
        Run ETL now for all forces

        Returns:
            MultiForceRunSummary with results of the operation
        """
        logger.info(f"Starting immediate ETL run for forces: {self.forces}")

        try:
            result = self.multi_force_runner.run_backfill(self.forces)
            logger.info(f"ETL run completed: {result.total_records} records, "
                       f"{result.forces_completed}/{len(self.forces)} forces successful")
            return result
        except Exception as e:
            logger.error(f"ETL run failed: {e}")
            raise

    def _run_etl_job(self) -> None:
        """Called by the scheduler"""
        try:
            logger.info("Scheduled ETL job starting")
            result = self.run_once()
            logger.info("Scheduled ETL job completed successfully")
        except Exception as e:
            logger.error(f"Scheduled ETL job failed: {e}")
            # do not re-raise in scheduler context