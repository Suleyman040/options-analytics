"""
Reference data batch job: copy Deribit's option list into Postgres.

    python -m options_analytics.reference              # run once
    python -m options_analytics.reference --every 3600 # run hourly, forever

New options get inserted, changed ones updated, and options Deribit no longer
lists (they expired) are marked inactive rather than deleted, so history in
ClickHouse can still be joined to them.
"""

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from options_analytics.config import Settings, get_settings
from options_analytics.deribit import DeribitRest
from options_analytics.logs import setup_logging
from options_analytics.migrate import retry
from options_analytics.postgres_store import PostgresStore

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RefreshSummary:
    seen: int
    inserted: int
    updated: int
    deactivated: int


def refresh(rest: DeribitRest, pg: PostgresStore, currency: str) -> RefreshSummary:
    started_at = datetime.now(UTC)
    instruments = rest.get_instruments(currency)
    if not instruments:
        # Never deactivate the whole chain because of one bad API response.
        raise RuntimeError(f"Deribit returned no {currency} options")
    result = pg.upsert_instruments(instruments)
    deactivated = pg.deactivate_missing(currency, [i.instrument_name for i in instruments])
    pg.record_refresh(currency, started_at, len(instruments), result, deactivated)
    summary = RefreshSummary(len(instruments), result.inserted, result.updated, deactivated)
    log.info(
        "reference refresh %s: %d seen, %d inserted, %d updated, %d deactivated",
        currency,
        summary.seen,
        summary.inserted,
        summary.updated,
        summary.deactivated,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--every", type=int, default=0, help="repeat every N seconds (0 = run once)")
    args = parser.parse_args()

    settings: Settings = get_settings()
    setup_logging(settings.log_level)
    rest = DeribitRest(settings.deribit_rest_url)
    pg = retry(lambda: PostgresStore(settings.postgres_dsn), "Postgres")
    try:
        while True:
            try:
                refresh(rest, pg, settings.currency)
            except Exception:
                if not args.every:
                    raise
                log.exception("reference refresh failed, will retry")
            if not args.every:
                break
            time.sleep(args.every)
    finally:
        rest.close()
        pg.close()


if __name__ == "__main__":
    main()
