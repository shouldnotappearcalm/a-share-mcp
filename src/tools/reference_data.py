"""Raw-data facades synchronized with the canonical a-share-data skill."""
import json
from src.services.tool_runner import json_tool_output
from src.skill_compat.fetch_stock_concepts import query_stock_concepts
from src.skill_compat.fetch_corporate_actions import query_corporate_actions
from src.skill_compat.fetch_market_details import query_market_pool, query_lhb_seats


def _require_json(format):
    if format != "json":
        raise ValueError("Raw data tools support format=json")


def _dump(payload):
    return json.dumps(payload, ensure_ascii=False, allow_nan=False)


def register_reference_data_tools(app):
    @app.tool()
    @json_tool_output
    def get_stock_concepts(code: str, source: str = "all", max_pages: int = 5, max_seconds: float = 20, no_cache: bool = False, format: str = "json") -> str:
        """Company concepts and provider-written reasons. THS includes all displayed regular/emerging/other categories; Eastmoney is IS_PRECISE=1 only. Preserve taxonomy, raw records and unknown evidence dates. source=all/ths/eastmoney; max_pages=1..20; max_seconds=1..30. No semantic filtering or cross-source deduplication."""
        _require_json(format)
        return _dump(query_stock_concepts(code, source, max_pages, max_seconds, no_cache))

    @app.tool()
    @json_tool_output
    def get_corporate_actions(kind: str = "unlock", code: str = "", start: str = "", end: str = "", days: int = 30, direction: str = "all", limit: int = 500, max_pages: int = 10, max_seconds: float = 25, no_cache: bool = False, format: str = "json") -> str:
        """Raw unlock schedules, repurchase progress or disclosed holder changes. kind=unlock/repurchase/holder_changes. Dates use FREE_DATE/UPDATEDATE/NOTICE_DATE respectively, inclusive. Without code require bounded start/end (up to366 days), except unlock defaults to next30 calendar days including today. days=1..366; direction=all/increase/decrease (holders only); limit=1..5000; max_pages=1..50; max_seconds=1..30. Partial pagination is explicit. Planned and actual amounts remain separate; holder changes do not establish complete future reduction plans."""
        _require_json(format)
        return _dump(query_corporate_actions(kind, code, start, end, days, direction, limit, max_pages, max_seconds, no_cache))

    @app.tool()
    @json_tool_output
    def get_market_pool_details(kind: str = "up", date: str = "", source: str = "auto", market: str = "all", limit: int = 500, max_pages: int = 10, max_seconds: float = 20, allow_date_fallback: bool = False, allow_partial_scope: bool = False, no_cache: bool = False, format: str = "json") -> str:
        """Raw limit-up/broken-board pool with seal times, open counts, funds and provider fields. kind=up/broken; source=auto/eastmoney/ths; market=all/shsz. Omitted date allows latest returned snapshot, but explicit mismatched dates require allow_date_fallback. THS excludes Beijing; all-market fallback requires allow_partial_scope and returns partial. limit=1..5000; max_pages=1..50; max_seconds=1..30. Always preserve actual trade_date and source attempts; no snapshot does not prove no limit-ups."""
        _require_json(format)
        return _dump(query_market_pool(kind, date, source, market, limit, max_pages, max_seconds, allow_date_fallback, allow_partial_scope, no_cache))

    @app.tool()
    @json_tool_output
    def get_lhb_seats(code: str, date: str, max_pages: int = 10, max_seconds: float = 20, no_cache: bool = False, format: str = "json") -> str:
        """Dragon-Tiger buy/sell seats for an exact stock/date using current BUY/SELL protocol. Preserve raw BUY/SELL/NET (CNY), percentage ratios, EXPLANATION and TRADE_ID; no cross-reason seat deduplication. Each side has pagination/error diagnostics. max_pages=1..50; max_seconds=1..30."""
        _require_json(format)
        return _dump(query_lhb_seats(code, date, max_pages, max_seconds, no_cache))
