import argparse
import sys
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import Config
from .http_client import HttpPoliceApiClient
from .sqlite_repository import SqliteStopSearchRepository, Base
from .etl_service import EtlService
from .backfill_service import BackfillService
from .multi_force_runner import MultiForceRunner
from .scheduler import EtlScheduler
from .metrics import MetricsCollector


def create_parser() -> argparse.ArgumentParser:
    """Build CLI parser"""
    # TODO: add --log-level and --db flags to override config
    # TODO: validate --since (YYYY-MM) if provided
    parser = argparse.ArgumentParser(
        prog='stopsearch-etl',
        description='UK Police Stop & Search ETL Tool'
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # backfill
    backfill_parser = subparsers.add_parser('backfill', help='Backfill data for specific forces')
    backfill_parser.add_argument('--force', nargs='+', required=True,
                                help='Police force identifiers (e.g., metropolitan, avon-and-somerset)')
    backfill_parser.add_argument('--since', type=str,
                                help='Start from specific month (YYYY-MM format)')

    # run once
    run_once_parser = subparsers.add_parser('run-once', help='Run ETL once for all configured forces')

    # schedule
    schedule_parser = subparsers.add_parser('schedule', help='Start scheduled ETL service')
    schedule_parser.add_argument('--time', type=str, default='02:00',
                                help='Daily run time in HH:MM format (default: 02:00)')
    # TODO: add timezone option if needed

    return parser


def setup_application():
    """Wire up app parts (config, DB, clients, services)"""

    # configuration
    config = Config()

    # logging
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # database
    engine = create_engine(config.database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # components
    api_client = HttpPoliceApiClient()
    repository = SqliteStopSearchRepository(session)
    metrics_collector = MetricsCollector()
    etl_service = EtlService(api_client, repository, metrics_collector)
    backfill_service = BackfillService(api_client, etl_service)
    multi_force_runner = MultiForceRunner(backfill_service)
    scheduler = EtlScheduler(multi_force_runner, config.forces)

    return api_client, repository, backfill_service, multi_force_runner, scheduler


def handle_backfill_command(args, backfill_service):
    """Run backfill for one or more forces"""
    # TODO: use logger instead of print
    forces = args.force

    print(f"Starting backfill for forces: {forces}")

    for force in forces:
        print(f"Processing force: {force}")
        try:
            result = backfill_service.backfill_force(force)
            print(f"Completed {force}: {result.total_records} records from "
                  f"{result.months_processed} months")

            if result.months_failed > 0:
                print(f"Warning: {result.months_failed} months failed for {force}")

        except Exception as e:
            print(f"Failed to process {force}: {e}")
            continue

    print("Backfill complete")


def handle_run_once_command(args, scheduler):
    """Run ETL one time for all configured forces"""
    print("Running ETL for all configured forces...")

    try:
        result = scheduler.run_once()
        print(f"ETL completed: {result.total_records} total records, "
              f"{result.forces_completed}/{result.forces_completed + result.forces_failed} forces successful")

        if result.forces_failed > 0:
            print(f"Warning: {result.forces_failed} forces failed")
            for failure in result.failed_forces:
                print(f"  - {failure}")

    except Exception as e:
        print(f"ETL run failed: {e}")
        sys.exit(1)


def handle_schedule_command(args, scheduler):
    """Start daily scheduler"""
    print(f"Starting scheduled ETL service (daily at {args.time})...")

    try:
        scheduler.start()
        print("Scheduler started. Press Ctrl+C to stop.")

        # keep process alive
        import signal
        signal.pause()

    except KeyboardInterrupt:
        print("Stopping scheduler...")
        scheduler.stop()
        print("Scheduler stopped")
    except Exception as e:
        print(f"Failed to start scheduler: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point"""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # build app
    api_client, repository, backfill_service, multi_force_runner, scheduler = setup_application()

    # route
    if args.command == 'backfill':
        handle_backfill_command(args, backfill_service)
    elif args.command == 'run-once':
        handle_run_once_command(args, scheduler)
    elif args.command == 'schedule':
        handle_schedule_command(args, scheduler)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == '__main__':
    main()