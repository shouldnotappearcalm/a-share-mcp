"""Raw company news/disclosure tools; no semantic classification."""
import json
from src.company_news import build_payload


def register_company_news_tools(app):
    @app.tool()
    def get_company_news(code: str, name: str = "", days: int = 7, limit: int = 100, scope: str = "all", no_cache: bool = False) -> str:
        """Return unclassified company news/disclosures with source/time/link diagnostics.

        code is a six-digit A-share code; scope is all/news/announcements.
        limit controls returned records, no_cache forces fresh source queries.
        """
        try:
            if not 1 <= days <= 366 or not 1 <= limit <= 200 or scope not in ("all","news","announcements"):
                raise ValueError("days=1..366, limit=1..200, scope=all/news/announcements")
            return json.dumps(build_payload(code,name,days,limit,scope=scope,no_cache=no_cache),ensure_ascii=False,allow_nan=False)
        except Exception as exc:
            return json.dumps({"status":"error","error":str(exc),"data":[]},ensure_ascii=False)
