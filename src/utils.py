# Utility functions, including the Baostock login context manager and logging setup
import baostock as bs
import os
import sys
import logging
from contextlib import contextmanager
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
@contextmanager
def baostock_login_context():
    """Context manager to handle Baostock login and logout, suppressing stdout messages."""
    # Redirect stdout to suppress login/logout messages
    original_stdout_fd = sys.stdout.fileno()
    saved_stdout_fd = os.dup(original_stdout_fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)

    os.dup2(devnull_fd, original_stdout_fd)
    os.close(devnull_fd)

    logger.debug("Attempting Baostock login...")
    lg = bs.login()
    logger.debug(f"Login result: code={lg.error_code}, msg={lg.error_msg}")

    # Restore stdout
    os.dup2(saved_stdout_fd, original_stdout_fd)
    os.close(saved_stdout_fd)

    if lg.error_code != '0':
        # Log error before raising
        logger.error(f"Baostock login failed: {lg.error_msg}")
        raise LoginError(f"Baostock login failed: {lg.error_msg}")

    logger.info("Baostock login successful.")
    try:
        yield  # API calls happen here
    finally:
        # Redirect stdout again for logout
        original_stdout_fd = sys.stdout.fileno()
        saved_stdout_fd = os.dup(original_stdout_fd)
        devnull_fd = os.open(os.devnull, os.O_WRONLY)

        os.dup2(devnull_fd, original_stdout_fd)
        os.close(devnull_fd)

        logger.debug("Attempting Baostock logout...")
        bs.logout()
        logger.debug("Logout completed.")

        # Restore stdout
        os.dup2(saved_stdout_fd, original_stdout_fd)
        os.close(saved_stdout_fd)
        logger.info("Baostock logout successful.")

# You can add other utility functions or classes here if needed
