# Utility functions, including the Baostock login context manager and logging setup
import baostock as bs
import os
import sys
import logging
from contextlib import contextmanager, redirect_stdout
import threading

from .data_source_interface import LoginError

# --- Logging Setup ---
def setup_logging(level=logging.INFO):
    """Configures basic logging for the application."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Optionally silence logs from dependencies if they are too verbose
    # logging.getLogger("mcp").setLevel(logging.WARNING)

# Get a logger instance for this module (optional, but good practice)
logger = logging.getLogger(__name__)

# --- Baostock Context Manager ---
_BAOSTOCK_LOCK = threading.RLock()


@contextmanager
def baostock_login_context():
    """Serialize Baostock's global socket and keep login output off MCP stdout."""
    # Baostock owns a process-global session, so concurrent login/query/logout
    # cycles must not disconnect each other's queries.
    with _BAOSTOCK_LOCK:
        with redirect_stdout(sys.stderr):
            result = bs.login()
        if result.error_code != "0":
            raise LoginError(f"Baostock login failed: {result.error_msg}")
        try:
            yield
        finally:
            with redirect_stdout(sys.stderr):
                bs.logout()
