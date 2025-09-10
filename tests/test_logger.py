import os
import logging
import tempfile
import shutil
import pytest
from src.logger import logger_setup

def test_logger_setup_returns_logger():
    logger = logger_setup("test.log")
    assert isinstance(logger, logging.Logger)

def test_logger_setup_creates_log_file():
    log_filename = "test_file.log"
    logger = logger_setup(log_filename)
    log_dir = os.path.join(os.path.dirname(__file__), "../src/logs")
    log_file_path = os.path.join(log_dir, log_filename)
    logger.info("Test message")
    assert os.path.exists(log_file_path)

def test_logger_setup_writes_to_file(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_filename = "temp_test.log"
    log_file_path = log_dir / log_filename
    with pytest.MonkeyPatch.context() as m:
        m.setattr(os.path, "dirname", lambda _: str(tmp_path))
        logger = logger_setup(log_filename)
        logger.info("Hello world!")
        logger.handlers[0].flush()
        with open(log_file_path) as f:
            content = f.read()
        assert "Hello world!" in content

def test_logger_setup_no_duplicate_handlers():
    log_filename = "dup_test.log"
    logger1 = logger_setup(log_filename)
    logger2 = logger_setup(log_filename)
    handler_types = [type(h) for h in logger2.handlers]
    assert handler_types.count(logging.FileHandler) <= 1