from __future__ import annotations

import os
import time
from loguru import logger

from .flows import daily_transform_and_forecast


def main() -> None:
    interval = int(os.getenv("SCHEDULE_INTERVAL_SECONDS", "3600"))
    logger.info("Scheduler starting, interval={}s", interval)
    while True:
        try:
            logger.info("Running daily_transform_and_forecast flow")
            res = daily_transform_and_forecast()
            logger.info("Flow result: {}", res)
        except Exception:
            logger.exception("Flow run failed")
        time.sleep(interval)


if __name__ == "__main__":
    main()
