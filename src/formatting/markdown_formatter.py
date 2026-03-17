"""
Markdown formatting utilities for A-Share MCP Server.
"""
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)

# Configuration: Max rows to display in string outputs to protect context length
MAX_MARKDOWN_ROWS = 250


def format_df_to_markdown(df: pd.DataFrame, max_rows: int = None) -> str:
    """Formats a Pandas DataFrame to a Markdown string with row truncation.

    Args:
        df: The DataFrame to format
        max_rows: Maximum rows to include in output. Defaults to MAX_MARKDOWN_ROWS if None.

    Returns:
        A markdown formatted string representation of the DataFrame
    """
    if df is None or df.empty:
        logger.warning("Attempted to format an empty DataFrame to Markdown.")
        return "(No data available to display)"

    if max_rows is None:
        max_rows = MAX_MARKDOWN_ROWS

    original_rows = df.shape[0]
    rows_to_show = min(original_rows, max_rows)
    df_display = df.head(rows_to_show)

    truncated = original_rows > rows_to_show

    try:
        markdown_table = df_display.to_markdown(index=False)
    except Exception as e:
        logger.error("Error converting DataFrame to Markdown: %s", e, exc_info=True)
        return "Error: Could not format data into Markdown table."

    if truncated:
        notes = f"rows truncated to {rows_to_show} from {original_rows}"
        return f"Note: Data truncated ({notes}).\n\n{markdown_table}"
    return markdown_table


def format_table_output(
    df: pd.DataFrame,
    format: str = "markdown",
    max_rows: int | None = None,
    meta: dict | None = None,
) -> str:
    """Formats a DataFrame into the requested string format with optional meta.

    Args:
        df: Data to format.
        format: 'markdown' | 'json' | 'csv'. Defaults to 'markdown'.
        max_rows: Optional max rows to include (defaults depend on formatters).
        meta: Optional metadata dict to include (prepended for markdown, embedded for json).

    Returns:
        A string suitable for tool responses.
    """
    fmt = (format or "markdown").lower()

    # Normalize row cap
    if max_rows is None:
        max_rows = MAX_MARKDOWN_ROWS if fmt == "markdown" else MAX_MARKDOWN_ROWS

    total_rows = 0 if df is None else int(df.shape[0])
    rows_to_show = 0 if df is None else min(total_rows, max_rows)
    truncated = total_rows > rows_to_show
    df_display = df.head(rows_to_show) if df is not None else pd.DataFrame()

    if fmt == "markdown":
        header = ""
        if meta:
            # Render a compact meta header
            lines = ["Meta:"]
            for k, v in meta.items():
                lines.append(f"- {k}: {v}")
            header = "\n".join(lines) + "\n\n"
        return header + format_df_to_markdown(df_display, max_rows=max_rows)

    if fmt == "csv":
        try:
            return df_display.to_csv(index=False)
        except Exception as e:
            logger.error("Error converting DataFrame to CSV: %s", e, exc_info=True)
            return "Error: Could not format data into CSV."

    if fmt == "json":
        try:
            # Convert DataFrame to dict and handle Timestamp serialization
            df_copy = df_display.copy()
            for col in df_copy.columns:
                if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                    df_copy[col] = df_copy[col].astype(str)
            
            payload = {
                "data": [] if df_copy is None else df_copy.to_dict(orient="records"),
                "meta": {
                    **(meta or {}),
                    "total_rows": total_rows,
                    "returned_rows": rows_to_show,
                    "truncated": truncated,
                    "columns": [] if df_copy is None else list(df_copy.columns),
                },
            }
            return json.dumps(payload, ensure_ascii=False)
        except Exception as e:
            logger.error("Error converting DataFrame to JSON: %s", e, exc_info=True)
            return "Error: Could not format data into JSON."

    # Fallback to markdown if unknown format
    logger.warning("Unknown format '%s', falling back to markdown", fmt)
    return format_df_to_markdown(df_display, max_rows=max_rows)
