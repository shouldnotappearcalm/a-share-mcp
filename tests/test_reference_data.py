import json
import pytest
from src.tools import reference_data as tools


class Registry:
    def __init__(self):
        self.tools = {}
    def tool(self):
        def register(fn):
            self.tools[fn.__name__] = fn
            return fn
        return register


@pytest.mark.parametrize("tool,query,kwargs", [
    ("get_stock_concepts", "query_stock_concepts", {"code": "601899"}),
    ("get_corporate_actions", "query_corporate_actions", {"kind": "unlock", "code": "688981"}),
    ("get_market_pool_details", "query_market_pool", {"kind": "broken", "date": "20261009"}),
    ("get_lhb_seats", "query_lhb_seats", {"code": "002317", "date": "20261009"}),
])
def test_facades_preserve_canonical_payload_and_forward_no_cache(monkeypatch, tool, query, kwargs):
    payload = {"status": "partial", "meta": {"coverage_limited": True, "trade_date": "2026-10-09"},
               "data": [{"raw": {"unknown_field": 42, "EXPLANATION": "原文", "missing": None}}]}
    seen = []
    def fetch(*args):
        seen.append(args)
        return payload
    monkeypatch.setattr(tools, query, fetch)
    registry = Registry(); tools.register_reference_data_tools(registry)
    output = json.loads(registry.tools[tool](**kwargs, no_cache=True))
    assert output == payload and seen[0][-1] is True
    output = registry.tools[tool](**kwargs, format="markdown")
    assert "support format=json" in output and len(seen) == 1


@pytest.mark.parametrize("tool,kwargs", [
    ("get_stock_concepts", {"code": "bad"}),
    ("get_corporate_actions", {"kind": "repurchase"}),
    ("get_market_pool_details", {"source": "ths", "market": "all"}),
    ("get_lhb_seats", {"code": "002317", "date": "20261301"}),
])
def test_invalid_inputs_return_json_without_network(tool, kwargs):
    registry = Registry(); tools.register_reference_data_tools(registry)
    output = json.loads(registry.tools[tool](**kwargs))
    assert output["status"] == "error" and output["data"] == []
