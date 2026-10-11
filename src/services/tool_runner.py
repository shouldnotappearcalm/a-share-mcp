"""Common error handling for MCP tools."""
import logging
import json
import os
import queue
import threading
from functools import wraps
from inspect import signature
from src.skill_compat.data_runtime import SourcePool

_TOOL_POOL = SourcePool(workers=8, backlog=32)
from typing import Callable

from src.data_source_interface import NoDataFoundError, LoginError, DataSourceError

logger = logging.getLogger(__name__)


def run_tool_with_handling(action: Callable[[], str], context: str, format: str = "markdown") -> str:
    """
    Executes a callable and normalizes exceptions to user-friendly strings.

    Args:
        action: Callable returning a string (typically formatted output).
        context: Short description for logs.
    """
    try:
        return action()
    except NoDataFoundError as e:
        logger.warning(f"{context}: No data found: {e}")
        if format == "json":
            return json.dumps({"status": "empty", "message": str(e), "data": []}, ensure_ascii=False)
        return f"Error: {e}"
    except LoginError as e:
        logger.error(f"{context}: Login error: {e}")
        if format == "json":
            return json.dumps({"status": "error", "error": str(e), "data": []}, ensure_ascii=False)
        return f"Error: Could not connect to data source. {e}"
    except DataSourceError as e:
        logger.error(f"{context}: Data source error: {e}")
        if format == "json":
            return json.dumps({"status": "error", "error": str(e), "data": []}, ensure_ascii=False)
        return f"Error: An error occurred while fetching data. {e}"
    except ValueError as e:
        logger.warning(f"{context}: Validation error: {e}")
        if format == "json":
            return json.dumps({"status": "error", "error": str(e), "data": []}, ensure_ascii=False)
        return f"Error: Invalid input parameter. {e}"
    except Exception as e:  # Catch-all
        logger.exception(f"{context}: Unexpected error: {e}")
        if format == "json":
            return json.dumps({"status": "error", "error": str(e), "data": []}, ensure_ascii=False)
        return f"Error: An unexpected error occurred: {e}"


def json_tool_output(fn):
    """Keep the requested JSON format even when a use case returns an error string."""
    @wraps(fn)
    def wrapped(*args, **kwargs):
        timeout=max(0.01,min(60,float(os.environ.get("A_SHARE_TOOL_TIMEOUT","35"))))
        bound = signature(fn).bind_partial(*args, **kwargs)
        bound.apply_defaults()
        requested_format = bound.arguments.get("format", "markdown")
        try:
            result = _TOOL_POOL.call(lambda: fn(*args, **kwargs), timeout)
        except TimeoutError:
            message=f"data source timeout after {timeout}s"
            return json.dumps({"status":"error","error":message,"data":[]}) if requested_format=="json" else message
        except Exception as exc:
            message=f"{type(exc).__name__}: {exc}"
            return json.dumps({"status":"error","error":message,"data":[]},ensure_ascii=False) if requested_format=="json" else message
        if requested_format == "json" and isinstance(result, str):
            try:
                json.loads(result)
            except ValueError:
                return json.dumps({"status": "error", "error": result, "data": []}, ensure_ascii=False)
        return result
    return wrapped
