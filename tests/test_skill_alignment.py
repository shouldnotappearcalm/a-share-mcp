import hashlib
import json
from datetime import date, datetime
from pathlib import Path
import pandas as pd

from src.ashare_data_source import AshareDataSource
from src.skill_compat import history, danginvest
from src.tools.skill_data import register_skill_data_tools
from src.use_cases.realtime_market import _aggregate_intraday_data


class Registry:
    def __init__(self):self.tools={}
    def tool(self):
        def register(fn):self.tools[fn.__name__]=fn;return fn
        return register


def test_synchronized_module_hashes_are_verifiable():
    package=Path(__file__).parents[1]/"src/skill_compat"
    manifest=json.loads((package/"manifest.json").read_text())
    for name,entry in manifest["modules"].items():
        assert hashlib.sha256((package/(name+".py")).read_bytes()).hexdigest()==entry["generated_sha256"]


def test_stock_code_semantics_match_skill():
    for code,expected in [("000001","sz000001"),("600519","sh600519"),("sh000001","sh000001"),("sh.600519","sh600519"),("600519.XSHG","sh600519")]:
        assert AshareDataSource.normalize_code(code)==expected
        assert history.normalize_code(code)==expected


def test_forming_minute_bar_policy_matches_skill():
    frame=pd.DataFrame({"open":[10.,11.],"high":[11.,13.],"low":[9.,10.],"close":[10.5,12.],"volume":[100,200]},index=pd.to_datetime(["2026-06-17 10:30:00","2026-06-17 10:35:00"]))
    result=_aggregate_intraday_data(frame,date(2026,6,17),now=datetime(2026,6,17,10,37),freq_minutes=5)
    assert result["close"]==10.5
    assert result["volume"]==100


def test_board_tools_preserve_skill_native_payload_and_stale(monkeypatch):
    upstream={"tradeDate":"2026-09-30","snapshotTsMs":1,"stale":True,"data":{"items":[{"groupKey":"I:test","groupLabel":"test","changePct":1}],"count":1,"total":1}}
    monkeypatch.setattr(danginvest,"_get_json",lambda *a,**kw:upstream)
    registry=Registry();register_skill_data_tools(registry)
    result=json.loads(registry.tools["get_boards_summary"](mode="sub",limit=20,format="json"))
    assert result["data"]==upstream["data"]["items"]
    assert result["meta"]["stale"] is True
    assert result["meta"]["mode"]=="ths_industry"
    assert result["meta"]["tradeDate"]=="2026-09-30"


def test_financial_tools_use_same_period_filter_and_fields(monkeypatch):
    raw=pd.DataFrame([{"报告期":"2026-06-30","归母净利润":10,"数据源":"eastmoney"},{"报告期":"2025-12-31","归母净利润":9,"数据源":"eastmoney"}])
    monkeypatch.setattr(history,"_financials_from_akshare",lambda _:raw)
    registry=Registry();register_skill_data_tools(registry)
    result=json.loads(registry.tools["get_financials"](code="600519",start="2026-01-01",count=1,format="json"))
    assert len(result["data"])==1
    assert result["data"][0]["归母净利润"]==10
    assert result["data"][0]["数据源"]=="eastmoney"


def test_price_history_output_fields_match_skill(monkeypatch):
    raw=pd.DataFrame({"time":pd.to_datetime(["2026-10-08","2026-10-09"]),"open":[10,11],"high":[12,13],"low":[9,10],"close":[11,12],"volume":[100,120]})
    monkeypatch.setattr(history,"_fetch_kline_with_fallback",lambda *a,**kw:(raw,"腾讯"))
    registry=Registry();register_skill_data_tools(registry)
    result=json.loads(registry.tools["get_price_history"](code="000001",count=2,format="json"))
    assert list(result["data"][0])==["time","code","open","high","low","close","preclose","volume","pctChg"]
    assert result["data"][0]["code"]=="sz000001"
    assert result["meta"]["source"]=="腾讯"


def test_complete_copied_module_hashes_are_verifiable():
    package=Path(__file__).parents[1]/'src/skill_compat'
    manifest=json.loads((package/'manifest.json').read_text())
    for name,entry in manifest['copied_modules'].items():
        assert hashlib.sha256((package/(name+'.py')).read_bytes()).hexdigest()==entry['source_sha256']


def test_industry_facade_uses_canonical_raw_payload(monkeypatch):
    from src.skill_compat import fetch_industry
    raw={'status':'ok','meta':{'taxonomy':'sw','effective_at':None},'data':[{'额外字段':42}]}
    monkeypatch.setattr(fetch_industry,'query_industry',lambda *a:raw)
    registry=Registry();register_skill_data_tools(registry)
    assert json.loads(registry.tools['get_industry_data'](taxonomy='sw'))==raw


def test_default_json_format_is_preserved_on_errors():
    registry=Registry();register_skill_data_tools(registry)
    result=json.loads(registry.tools['get_price_history'](code='600519',frequency='invalid'))
    assert result['status']=='error' and result['data']==[]


def test_generated_history_keeps_conditional_runtime_imports(monkeypatch):
    raw=pd.DataFrame({'time':pd.to_datetime(['2026-10-09']),'open':[10.],'high':[12.],'low':[9.],'close':[11.],'volume':[100]})
    monkeypatch.setattr(history,'get_kline_tencent',lambda *a,**kw:raw)
    data,source=history._fetch_kline_with_fallback(None,'sh600519','1d',1,no_cache=True)
    assert source=='腾讯' and data.iloc[0]['close']==11
