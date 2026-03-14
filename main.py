import logging
import argparse
import sys
from ingestion.ingester import Ingester
from ingestion.salary_ingester import SalaryIngester
from ingestion.roster_ingester import RosterIngester
import pandas as pd


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="MLS Analytics Engine")
    parser.add_argument("--debug", action="store_true", help="enable debug logging")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("MLS Salary Performance")
    logger.info("Getting Salary Data For 2025")
    ingester: Ingester = SalaryIngester()
    ingester.run()
    ingester: Ingester = RosterIngester()
    ingester.run()


if __name__ == "__main__":
    sys.exit(main())
