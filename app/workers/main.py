"""Standalone worker daemon entrypoint: python -m app.workers.main."""

import asyncio
import logging
import signal
import sys

from app.workers.consumer_runner import WorkerSupervisor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("app.workers.main")


def main() -> None:
    supervisor = WorkerSupervisor()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _sig_handler():
        logger.info("Received termination signal; stopping worker daemon...")
        supervisor.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(supervisor.run())
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user.")
    finally:
        loop.close()
        logger.info("Worker exited.")


if __name__ == "__main__":
    main()
