from __future__ import annotations

import logging

from data_sync_service.service import foo

logger = logging.getLogger(__name__)


def run_foo_job() -> None:
    result = foo()
    logger.info("Foo job executed: %s", result)
