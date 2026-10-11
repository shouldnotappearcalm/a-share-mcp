import json
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src.ashare_data_source import AshareDataSource
from src.formatting.markdown_formatter import format_table_output
from src.use_cases import market_data, realtime_market
from src import utils


def frame():
    return pd.DataFrame({"open": [10., 11.], "high": [12., 13.], "low": [9., 10.],
                         "close": [11., 12.], "volume": [100., 120.]},
                        index=pd.to_datetime(["2020-01-02", "2020-01-03"]))


def test_quote_compares_displayed_bar_to_previous_bar(monkeypatch):
    source = AshareDataSource()
    monkeypatch.setattr(source, "get_historical_k_data", lambda **kw: frame() if kw["frequency"]=="1d" else None)
    result = json.loads(realtime_market.fetch_realtime_quote(source,"600519",format="json"))
    assert result["data"][0]["昨收"] == 11
    assert result["data"][0]["涨跌幅(%)"] == 9.09


def test_kline_columns_preserve_sina_and_tencent_order(monkeypatch):
    for columns in (["open","high","low","close","volume"], ["open","close","high","low","volume"]):
        monkeypatch.setattr(market_data,"get_price",lambda *a,**kw: frame()[columns])
        result = json.loads(market_data.fetch_realtime_kline("600519",count=2,format="json"))
        assert result["data"][-1]["high"] == 13
        assert result["data"][-1]["close"] == 12
        technical = json.loads(market_data.fetch_technical_indicators("600519",count=2,indicators=["MA"],format="json"))
        assert technical["data"][-1]["high"] == 13
        assert technical["data"][-1]["close"] == 12


def test_json_is_strict_and_handles_native_dates():
    result = format_table_output(pd.DataFrame({"day":[date(2026,10,9)],"value":[float("nan")]}),format="json")
    parsed = json.loads(result,parse_constant=lambda value: pytest.fail("non-standard JSON: "+value))
    assert parsed["data"][0]["value"] is None
    assert parsed["data"][0]["day"].startswith("2026-10-09")


def test_baostock_login_messages_never_pollute_stdout(monkeypatch,capsys):
    def login():
        print("login success!")
        return SimpleNamespace(error_code="0",error_msg="")
    monkeypatch.setattr(utils.bs,"login",login)
    monkeypatch.setattr(utils.bs,"logout",lambda:print("logout success!"))
    with utils.baostock_login_context():
        print('{"data": []}')
    captured = capsys.readouterr()
    assert captured.out.strip() == '{"data": []}'
    assert "login success!" in captured.err


def test_count_and_date_range_are_not_ignored(monkeypatch):
    source = AshareDataSource()
    monkeypatch.setattr(source,"_get_price_sina",lambda *args: frame())
    result=source.get_historical_k_data("600519","2020-01-03","2020-01-03",frequency="1d",count=1)
    assert len(result)==1
    assert result.iloc[0]["close"]==12


def test_adjusted_history_is_not_silently_mixed():
    with pytest.raises(ValueError,match="raw prices"):
        AshareDataSource().get_historical_k_data("600519","","",adjust_flag="2")


def test_server_registers_without_external_ashare_directory():
    import mcp_server
    assert mcp_server.app is not None


def test_batch_timeout_returns_finished_results_without_waiting(monkeypatch):
    import time
    def quote(source,code,format):
        if code=="slow":time.sleep(.4)
        return json.dumps({"data":[{"代码":code,"最新价":1,"涨跌幅(%)":1}]})
    monkeypatch.setattr(realtime_market,"fetch_realtime_quote",quote)
    started=time.monotonic()
    result=json.loads(realtime_market.fetch_realtime_multi_quote(None,["fast","slow"],format="json",max_seconds=.05))
    assert time.monotonic()-started < .3
    assert result["meta"]["success"]==1
    assert result["meta"]["failed_codes"]==["slow"]


def test_market_tool_timeout_returns_json(monkeypatch):
    import time
    from src.services.tool_runner import json_tool_output
    monkeypatch.setenv("A_SHARE_TOOL_TIMEOUT","0.03")
    @json_tool_output
    def slow(format="json"):
        time.sleep(.2)
        return '{"data": [1]}'
    started=time.monotonic()
    result=json.loads(slow(format="json"))
    assert time.monotonic()-started<.15
    assert result["status"]=="error"
    assert "timeout" in result["error"]
