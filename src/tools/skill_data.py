"""MCP facades for the canonical a-share-data skill data functions."""
import json
from types import SimpleNamespace
from src.skill_compat import history, danginvest
from src.formatting.markdown_formatter import format_table_output
from src.services.tool_runner import json_tool_output


def register_skill_data_tools(app):
    @app.tool()
    @json_tool_output
    def get_price_history(code: str, start: str = "", end: str = "", frequency: str = "1d", count: int = 120, format: str = "json", adjust: str = "raw", no_cache: bool = False) -> str:
        """Same history route/fields as a-share-data fetch_history: Tencent, Sina and Eastmoney with explicit adjustment support.

        Explicit raw/qfq/hfq contract matches the skill; unsupported sources are skipped.
        Legacy get_historical_k_data retains Baostock's explicit adjustment semantics.
        """
        if frequency not in ("1d","1w","1M") or not 1<=count<=2000:
            raise ValueError("frequency=1d/1w/1M; count=1..2000")
        norm=history.normalize_code(code)
        size=history._estimate_fetch_count(start,end,frequency,count)
        df,source=history._fetch_kline_with_fallback(history._build_session(),norm,frequency,size,start=start or None,end=end or None,retry=2,adjust=adjust,no_cache=no_cache)
        if df.empty:raise ValueError("source_unavailable: skill history sources")
        result=history._format_kline_output(df,norm,count,start or None,end or None)
        return format_table_output(result,format=format,max_rows=count,meta={"source":source,"compatibility":"a-share-data/fetch_history","adjust_requested":adjust,"adjust_actual":adjust,"timezone":"Asia/Shanghai","source_errors":df.attrs.get("source_errors",{}),"cache":df.attrs.get("cache",{})})

    @app.tool()
    @json_tool_output
    def get_financials(code: str, kind: str = "financials", start: str = "", end: str = "", year: str = "", quarter: int = 0, count: int = 120, format: str = "json", as_of: str = "") -> str:
        """Same current Eastmoney/Sina indicators and period filters as a-share-data fetch_history.

        kind=financials/profit/growth/balance/cashflow/dupont. Cashflow/DuPont are related indicators,
        not a full statement or a verified decomposition. Legacy Baostock tools remain available.
        """
        if kind not in ("financials","profit","growth","balance","cashflow","dupont") or not 1<=count<=500 or quarter not in (0,1,2,3,4):
            raise ValueError("invalid kind/count/quarter")
        norm=history.normalize_code(code)
        df=history._financials_from_akshare(norm) if kind=="financials" else history._financial_single_from_akshare(norm,kind)
        args=SimpleNamespace(start=start,end=end,year=year,quarter=quarter,count=count,as_of=as_of)
        df=history._filter_financial_periods(df,args)
        return format_table_output(df,format=format,max_rows=count,meta={"compatibility":"a-share-data/fetch_history","code":norm,"kind":kind,"as_of":as_of or None,"pit_guarantee":"announcement_date_filter_only","errors":df.attrs.get("errors",[])})

    @app.tool()
    @json_tool_output
    def get_industry_info(code: str, with_boards: bool = False, format: str = "json") -> str:
        """Same Eastmoney company/industry query as a-share-data fetch_history --industry.

        Legacy get_stock_industry uses Baostock's CSRC taxonomy; standards are not interchangeable.
        """
        if format != "json":raise ValueError("This raw data facade supports format=json")
        args=SimpleNamespace(industry=code,with_boards=with_boards,board_scan_limit=80,output_json=True)
        payload=history.cmd_industry(args)
        return json.dumps(payload,ensure_ascii=False,allow_nan=False)

    @app.tool()
    @json_tool_output
    def get_boards_summary(mode: str = "sub", limit: int = 60, sort: str = "change_desc", retries: int = 0, format: str = "json", require_fresh: bool = False) -> str:
        """Same DangInvest industry/concept heatmap as a-share-data fetch_danginvest --summary.

        mode=major/sub/concept. Preserve tradeDate/snapshotTsMs/stale; stale is not today's data.
        """
        if format != "json":raise ValueError("This raw data facade supports format=json")
        if not 1<=limit<=1000 or retries not in (0,1,2):raise ValueError("invalid limit/retries")
        payload=danginvest.cmd_summary(mode,limit,sort,retries,True)
        if require_fresh and payload.get("meta",{}).get("stale") is not False:
            raise ValueError("fresh data unavailable: snapshot is stale or freshness unknown")
        return json.dumps(payload,ensure_ascii=False,allow_nan=False)

    @app.tool()
    @json_tool_output
    def get_board_members(group_key: str, mode: str = "sub", limit: int = 100, offset: int = 0, sort: str = "change_desc", retries: int = 0, format: str = "json", require_fresh: bool = False) -> str:
        """Same DangInvest board-detail query as the skill; use groupKey from summary."""
        if format != "json":raise ValueError("This raw data facade supports format=json")
        if not 1<=limit<=1000 or offset<0 or retries not in (0,1,2):raise ValueError("invalid limit/offset/retries")
        payload=danginvest.cmd_detail(mode,group_key,sort,limit,offset,retries,True)
        if require_fresh and payload.get("meta",{}).get("stale") is not False:
            raise ValueError("fresh data unavailable: snapshot is stale or freshness unknown")
        return json.dumps(payload,ensure_ascii=False,allow_nan=False)

    @app.tool()
    @json_tool_output
    def get_market_news(limit: int = 100, offset: int = 0, retries: int = 0, format: str = "json", require_fresh: bool = False) -> str:
        """Same DangInvest market-news JSON/freshness as a-share-data fetch_danginvest --news."""
        if format != "json":raise ValueError("This raw data facade supports format=json")
        if not 1<=limit<=1000 or offset<0 or retries not in (0,1,2):raise ValueError("invalid limit/offset/retries")
        payload=danginvest.cmd_news(limit,offset,retries,True)
        if require_fresh and payload.get("meta",{}).get("stale") is not False:
            raise ValueError("fresh data unavailable: snapshot is stale or freshness unknown")
        return json.dumps(payload,ensure_ascii=False,allow_nan=False)

    @app.tool()
    @json_tool_output
    def get_industry_data(action: str = "directory", taxonomy: str = "sw", level: int = 1, symbol: str = "", start: str = "", end: str = "", no_cache: bool = False) -> str:
        """Canonical skill industry directory/members/index history. Explicit SW/Eastmoney taxonomies; current constituents, not historical membership."""
        from src.skill_compat.fetch_industry import query_industry
        return json.dumps(query_industry(action,taxonomy,level,symbol,start,end,no_cache),ensure_ascii=False,allow_nan=False)
