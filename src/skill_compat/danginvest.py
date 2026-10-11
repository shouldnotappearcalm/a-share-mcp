# Generated from a-share-data; see manifest.json. No runtime sibling dependency.
import argparse
import json
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional
import requests
_REQUIRE_FRESH = False
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
BASE_URL = 'https://dang-invest.com/api/market'
NEWS_URL = f'{BASE_URL}/news'
BOARDS_SUMMARY_URL = f'{BASE_URL}/boards/summary'
BOARDS_DETAIL_URL = f'{BASE_URL}/boards/detail'
MODE_ALIASES = {'major': 'industry', 'industry': 'industry', '大类行业': 'industry', 'sub': 'ths_industry', 'ths_industry': 'ths_industry', '细分行业': 'ths_industry', 'concept': 'ths_concept', 'ths_concept': 'ths_concept', '概念': 'ths_concept'}
SORT_ALIASES = {'market_cap_desc': 'market_cap_desc', 'market_cap': 'market_cap_desc', '总市值': 'market_cap_desc', 'turnover_desc': 'turnover_desc', 'turnover': 'turnover_desc', '成交额': 'turnover_desc', '总成交额': 'turnover_desc', 'change_desc': 'change_desc', '涨幅': 'change_desc', '领涨': 'change_desc', 'change_asc': 'change_asc', '跌幅': 'change_asc', '领跌': 'change_asc'}
VALID_SORTS = frozenset(SORT_ALIASES.values())
DETAIL_INVALID_SORTS = frozenset({'changePct_desc', 'changePct_asc', 'change_pct_desc', 'change_pct_asc'})

def _build_session(max_retries: int) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(total=max_retries, connect=max_retries, read=max_retries, backoff_factor=0.4, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=('GET',), raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def _get_json(url: str, params: Optional[Dict[str, Any]]=None, retries: int=2, timeout: int=20) -> Dict[str, Any]:
    session = _build_session(0)
    last_error: Optional[Exception] = None
    attempts = retries + 1
    for attempt in range(attempts):
        try:
            resp = session.get(url, params=params or {}, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            if isinstance(payload, dict) and payload.get('error'):
                raise RuntimeError(str(payload.get('error')))
            if isinstance(payload, dict):
                payload['data_quality'] = 'stale' if payload.get('stale') else 'unknown' if 'stale' not in payload else 'provider_fresh'
                if _REQUIRE_FRESH and payload.get('stale') is not False:
                    raise ValueError('fresh data unavailable: upstream snapshot is stale or freshness unknown')
            return payload
        except ValueError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f'请求失败（已重试 {retries} 次）：{last_error}')

def normalize_mode(mode: str) -> str:
    key = str(mode or '').strip()
    resolved = MODE_ALIASES.get(key)
    if not resolved:
        allowed = ', '.join(sorted({k for k in MODE_ALIASES if k.isascii()}))
        raise ValueError(f'未知 mode={mode!r}，可用：{allowed}')
    return resolved

def normalize_sort(sort: str, for_detail: bool=False) -> str:
    key = str(sort or '').strip()
    if for_detail and key in DETAIL_INVALID_SORTS:
        raise ValueError(f'detail 不支持 sort={key!r}，请使用 change_desc（涨幅）或 change_asc（跌幅）')
    resolved = SORT_ALIASES.get(key, key)
    if resolved not in VALID_SORTS:
        allowed = ', '.join(sorted(VALID_SORTS))
        raise ValueError(f'未知 sort={sort!r}，可用：{allowed}')
    return resolved

def _now_meta() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def cmd_news(limit: int, offset: int, retries: int, output_json: bool) -> None:
    payload = _get_json(NEWS_URL, {'limit': limit, 'offset': offset}, retries=retries)
    items = payload.get('data') or []
    meta = {'url': NEWS_URL, 'limit': limit, 'offset': offset, 'count': payload.get('count'), 'has_more': payload.get('has_more'), 'update_time': _now_meta(), 'data_source': 'DangInvest'}
    from zoneinfo import ZoneInfo
    tz = ZoneInfo('Asia/Shanghai')
    dates = []
    for item in items:
        try:
            stamp = datetime.fromisoformat(str(item.get('published_at', '')).replace('Z', '+00:00'))
            dates.append(stamp.replace(tzinfo=tz) if stamp.tzinfo is None else stamp.astimezone(tz))
        except (ValueError, TypeError):
            pass
    latest = max(dates) if dates else None
    age = max(0, (datetime.now(tz) - latest).total_seconds()) if latest else None
    meta.update({'latest_published_at': latest.isoformat() if latest else None, 'age_seconds': round(age) if age is not None else None, 'stale': age > 86400 if age is not None else None, 'freshness_threshold_seconds': 86400})
    return {'meta': meta, 'data': items}

def cmd_summary(mode: str, limit: int, sort: str, retries: int, output_json: bool) -> None:
    api_mode = normalize_mode(mode)
    api_sort = normalize_sort(sort, for_detail=False)
    payload = _get_json(BOARDS_SUMMARY_URL, {'mode': api_mode, 'limit': limit, 'sort': api_sort}, retries=retries)
    data = payload.get('data') or {}
    items = data.get('items') or []
    meta = {'url': BOARDS_SUMMARY_URL, 'mode': api_mode, 'limit': limit, 'sort': api_sort, 'tradeDate': payload.get('tradeDate'), 'snapshotTsMs': payload.get('snapshotTsMs'), 'stale': payload.get('stale'), 'data_quality': payload.get('data_quality'), 'count': data.get('count'), 'total': data.get('total'), 'update_time': _now_meta(), 'data_source': 'DangInvest'}
    return {'meta': meta, 'data': items}

def cmd_detail(mode: str, group_key: str, sort: str, items_limit: int, items_offset: int, retries: int, output_json: bool) -> None:
    if not group_key:
        print('--group-key 不能为空')
        sys.exit(1)
    api_mode = normalize_mode(mode)
    api_sort = normalize_sort(sort, for_detail=True)
    payload = _get_json(BOARDS_DETAIL_URL, {'mode': api_mode, 'groupKey': group_key, 'sort': api_sort, 'items_limit': items_limit, 'items_offset': items_offset}, retries=retries)
    meta = {'url': BOARDS_DETAIL_URL, 'mode': api_mode, 'groupKey': group_key, 'sort': api_sort, 'items_limit': items_limit, 'items_offset': items_offset, 'tradeDate': payload.get('tradeDate'), 'snapshotTsMs': payload.get('snapshotTsMs'), 'stale': payload.get('stale'), 'data_quality': payload.get('data_quality'), 'update_time': _now_meta(), 'data_source': 'DangInvest'}
    data = payload.get('data') or {}
    return {'meta': meta, 'data': data}
