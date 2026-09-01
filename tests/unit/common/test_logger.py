import logging
import os
import unittest
from unittest.mock import patch

from common.logger import LOGGER_NAMESPACE, get_logger


class GetLoggerTests(unittest.TestCase):
    logger_name = f"{LOGGER_NAMESPACE}.bronze.ingest_blocks"

    def setUp(self) -> None:
        logging.getLogger(self.logger_name).handlers.clear()

    def tearDown(self) -> None:
        logging.getLogger(self.logger_name).handlers.clear()

    def test_get_logger_is_namespaced_and_idempotent(self) -> None:
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}):
            logger = get_logger("bronze.ingest_blocks")
            same_logger = get_logger("bronze.ingest_blocks")

        self.assertIs(logger, same_logger)
        self.assertEqual(logger.name, f"{LOGGER_NAMESPACE}.bronze.ingest_blocks")
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertEqual(len(logger.handlers), 1)

    def test_get_logger_uses_info_for_invalid_log_level(self) -> None:
        with patch.dict(os.environ, {"LOG_LEVEL": "invalid"}):
            logger = get_logger("test.invalid_level")

        self.assertEqual(logger.level, logging.INFO)


if __name__ == "__main__":
    unittest.main()
