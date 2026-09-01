"""Shared application logging for Spark jobs.

Spark's JVM logs are configured separately through ``log4j2.properties``. This
module is for messages emitted by Python job code.
"""

from __future__ import annotations

import logging
import os
import sys

DEFAULT_LOG_LEVEL = "INFO"
LOGGER_NAMESPACE = "blockchain.spark"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a configured logger for a Spark job or shared module.

    Set ``LOG_LEVEL`` (for example, ``DEBUG`` or ``WARNING``) to control
    verbosity. Configuration is applied once per logger.
    """
    logger_name = LOGGER_NAMESPACE if name is None else f"{LOGGER_NAMESPACE}.{name}"
    logger = logging.getLogger(logger_name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False

    configured_level = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    logger.setLevel(getattr(logging, configured_level, logging.INFO))
    return logger
