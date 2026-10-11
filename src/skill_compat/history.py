# Generated from a-share-data; see manifest.json. No runtime sibling dependency.
import argparse
import json
import re
import sys
import threading
import queue
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
try:
    from .data_runtime import call_source, cached_fetch
except ImportError:
    from data_runtime import call_source, cached_fetch
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
SINA_KLINE_URL = 'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData'
TENCENT_DAY_URL = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
TENCENT_QUOTE_URL = 'https://qt.gtimg.cn/q={code}'
SINA_QUOTE_URL = 'https://hq.sinajs.cn/list={code}'
EM_QUOTE_URL = 'https://push2.eastmoney.com/api/qt/stock/get'
EM_COMPANY_URL = 'https://emweb.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax'
EM_BOARD_CONCEPT_URL = 'https://push2.eastmoney.com/api/qt/clist/get'
EM_BOARD_INDUSTRY_URL = 'https://push2.eastmoney.com/api/qt/clist/get'
SINA_STOCK_LIST_COUNT = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeStockCount'
SINA_STOCK_LIST_DATA = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData'
SINA_FREQ_MAP = {'1d': 240, '1w': 1200, '1M': 7200}
TENCENT_DAY_FREQ_MAP = {'1d': 'day', '1w': 'week', '1M': 'month'}
INDEX_CODE_MAP = {'hs300': '000300', 'sz50': '000016', 'zz500': '000905'}
_SESSIONS = threading.local()

def _build_session() -> requests.Session:
    if getattr(_SESSIONS, 'session', None) is not None:
        return _SESSIONS.session
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36', 'Referer': 'https://finance.eastmoney.com'})
    retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=('GET',), raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    _SESSIONS.session = s
    return s

def normalize_code(code: str) -> str:
    c = code.strip()
    if '.XSHG' in c:
        return 'sh' + c.replace('.XSHG', '')
    if '.XSHE' in c:
        return 'sz' + c.replace('.XSHE', '')
    if '.' in c:
        p = c.split('.')
        if len(p) == 2:
            left = p[0].lower()
            right = p[1]
            if left in ('sh', 'sz') and right.isdigit():
                return f'{left}{right}'
    c = c.lower()
    if c.startswith(('sh', 'sz')):
        return c
    if c.isdigit():
        if c.startswith('6'):
            return 'sh' + c
        return 'sz' + c
    return c

def _code_digits(code: str) -> str:
    n = normalize_code(code)
    return n[2:] if n.startswith(('sh', 'sz')) else n

def _to_secid(norm_code: str) -> str:
    digits = _code_digits(norm_code)
    if norm_code.startswith('sh'):
        return f'1.{digits}'
    return f'0.{digits}'

def _get_json(session: requests.Session, url: str, params=None, timeout: int=10):
    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _get_text(session: requests.Session, url: str, params=None, timeout: int=10) -> str:
    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.text

def get_kline_sina(session: requests.Session, norm_code: str, freq: str, count: int) -> pd.DataFrame:
    if freq not in SINA_FREQ_MAP:
        return pd.DataFrame()
    params = {'symbol': norm_code, 'scale': SINA_FREQ_MAP[freq], 'ma': 5, 'datalen': count}
    try:
        data = _get_json(session, SINA_KLINE_URL, params=params, timeout=10)
        if not data or isinstance(data, dict):
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if 'day' in df.columns:
            df = df.rename(columns={'day': 'time'})
        df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
        df = df.dropna(subset=['time'])
        return df
    except Exception:
        return pd.DataFrame()

def get_kline_tencent(session: requests.Session, norm_code: str, freq: str, count: int, adjust: str='raw') -> pd.DataFrame:
    if freq not in TENCENT_DAY_FREQ_MAP:
        return pd.DataFrame()
    unit = TENCENT_DAY_FREQ_MAP[freq]
    if adjust not in ('raw', 'qfq', 'hfq'):
        raise ValueError('adjust=raw/qfq/hfq')
    endpoint = TENCENT_DAY_URL if adjust != 'raw' else 'https://web.ifzq.gtimg.cn/appstock/app/kline/kline'
    basis = '' if adjust == 'raw' else adjust
    url = f'{endpoint}?param={norm_code},{unit},,,{count},{basis}'
    try:
        payload = _get_json(session, url, timeout=10)
        if 'data' not in payload or norm_code not in payload['data']:
            return pd.DataFrame()
        block = payload['data'][norm_code]
        key = unit if adjust == 'raw' else adjust + unit
        arr = block.get(key) or []
        if not arr:
            return pd.DataFrame()
        df = pd.DataFrame([row[:6] for row in arr], columns=['time', 'open', 'close', 'high', 'low', 'volume'])
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
        df = df.dropna(subset=['time'])
        return df
    except Exception:
        return pd.DataFrame()

def _estimate_fetch_count(start: str, end: str, freq: str, default_count: int) -> int:
    """根据日期范围估算需要拉取的K线条数（含余量）。

    新浪/腾讯接口返回的是"从今天往前推 N 条"的数据，
    因此要覆盖到 start，需要拉取 今天→start 之间的交易日数量。
    """
    if not start:
        return default_count
    try:
        s = pd.to_datetime(start)
        now = pd.Timestamp.now()
        days_from_now = (now - s).days + 1
        if freq == '1d':
            return max(default_count, int(days_from_now * 0.72) + 30)
        if freq == '1w':
            return max(default_count, days_from_now // 7 + 10)
        if freq == '1M':
            return max(default_count, days_from_now // 30 + 5)
    except Exception:
        pass
    return default_count

def get_kline_xueqiu(session: requests.Session, norm_code: str, freq: str, count: int) -> pd.DataFrame:
    """雪球K线兜底（可能受反爬策略影响）。"""
    period_map = {'1d': 'day', '1w': 'week', '1M': 'month'}
    if freq not in period_map:
        return pd.DataFrame()
    digits = _code_digits(norm_code)
    if len(digits) != 6:
        return pd.DataFrame()
    symbol = ('SH' if norm_code.startswith('sh') else 'SZ') + digits
    url = 'https://stock.xueqiu.com/v5/stock/chart/kline.json'
    params = {'symbol': symbol, 'begin': int(pd.Timestamp.now().timestamp() * 1000), 'period': period_map[freq], 'type': 'before', 'count': max(60, int(count)), 'indicator': 'kline'}
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': f'https://xueqiu.com/S/{symbol}', 'Accept': 'application/json, text/plain, */*'}
    try:
        r = session.get(url, params=params, headers=headers, timeout=10)
        if r.status_code != 200:
            return pd.DataFrame()
        j = r.json()
        data = (j or {}).get('data', {})
        items = data.get('item') or []
        cols = data.get('column') or []
        if not items or not cols:
            return pd.DataFrame()
        if not all((k in cols for k in ['timestamp', 'open', 'high', 'low', 'close', 'volume'])):
            return pd.DataFrame()
        df = pd.DataFrame(items, columns=cols)
        out = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
        out = out.rename(columns={'timestamp': 'time'})
        out['time'] = pd.to_datetime(out['time'], unit='ms', errors='coerce')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            out[col] = pd.to_numeric(out[col], errors='coerce')
        out = out.dropna(subset=['time'])
        return out
    except Exception:
        return pd.DataFrame()

def get_kline_eastmoney(norm_code: str, freq: str, start: str=None, end: str=None, adjust: str='raw') -> pd.DataFrame:
    """东方财富日/周/月K线（akshare）兜底。"""
    try:
        import akshare as ak
    except Exception:
        return pd.DataFrame()
    period_map = {'1d': 'daily', '1w': 'weekly', '1M': 'monthly'}
    if freq not in period_map:
        return pd.DataFrame()
    symbol = _code_digits(norm_code)
    if not symbol or len(symbol) != 6:
        return pd.DataFrame()

    def _fmt(d):
        if not d:
            return ''
        try:
            return pd.to_datetime(d).strftime('%Y%m%d')
        except Exception:
            return ''
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period=period_map[freq], start_date=_fmt(start), end_date=_fmt(end), adjust='' if adjust == 'raw' else adjust)
        if df is None or df.empty:
            return pd.DataFrame()
        col_map = {'日期': 'time', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'}
        if not set(col_map.keys()).issubset(set(df.columns)):
            return pd.DataFrame()
        out = df.rename(columns=col_map)[list(col_map.values())].copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            out[col] = pd.to_numeric(out[col], errors='coerce')
        out['time'] = pd.to_datetime(out['time'], errors='coerce')
        out = out.dropna(subset=['time'])
        return out
    except Exception:
        return pd.DataFrame()

def _fetch_kline_uncached(session: requests.Session, norm: str, freq: str, fetch_count: int, start: str=None, end: str=None, retry: int=2, adjust: str='raw'):
    """Only fail over to providers supporting the requested adjustment contract."""
    if adjust not in ('raw', 'qfq', 'hfq'):
        raise ValueError('adjust=raw/qfq/hfq')
    callbacks = [('腾讯', lambda: get_kline_tencent(_build_session(), norm, freq, fetch_count, adjust))]
    if adjust == 'raw':
        callbacks.append(('新浪', lambda: get_kline_sina(_build_session(), norm, freq, fetch_count)))
    callbacks.append(('东财', lambda: get_kline_eastmoney(norm, freq, start=start, end=end, adjust=adjust)))
    errors = {}
    for source, callback in callbacks:
        try:
            df = call_source(callback, 8)
            if df is None or df.empty:
                raise ValueError('empty response')
            required = ['time', 'open', 'high', 'low', 'close']
            if not all((column in df for column in required)) or df[required].isna().any().any():
                raise ValueError('missing OHLC/time')
            if ((df['high'] < df[['open', 'close']].max(axis=1)) | (df['low'] > df[['open', 'close']].min(axis=1))).any():
                raise ValueError('invalid OHLC ordering')
            df.attrs.update(adjust_requested=adjust, adjust_actual=adjust, source_errors=errors, timezone='Asia/Shanghai')
            return (df, source)
        except Exception as exc:
            errors[source] = f'{type(exc).__name__}: {str(exc)[:200]}'
    df = pd.DataFrame()
    df.attrs['source_errors'] = errors
    return (df, None)

def _fetch_kline_with_fallback(session: requests.Session, norm: str, freq: str, fetch_count: int, start: str=None, end: str=None, retry: int=2, adjust: str='raw', no_cache: bool=False):

    def load(_):
        frame, source = _fetch_kline_uncached(session, norm, freq, fetch_count, start, end, retry, adjust)
        return {'data': json.loads(frame.to_json(orient='records', date_format='iso', force_ascii=False)), 'source': source, 'attributes': frame.attrs}
    value, cache = cached_fetch(f'history-v2:{norm}:{freq}:{fetch_count}:{start}:{end}:{adjust}', load, 300, no_cache, cacheable=lambda value: bool(value['data']))
    frame = pd.DataFrame(value['data'])
    if not frame.empty:
        frame['time'] = pd.to_datetime(frame['time'])
    frame.attrs.update(value['attributes'], cache=cache)
    return (frame, value['source'])

def _format_kline_output(df: pd.DataFrame, norm: str, count: int, start: str=None, end: str=None) -> pd.DataFrame:
    df = df.sort_values('time').copy()
    df['preclose'] = df['close'].shift(1)
    df['pctChg'] = (df['close'] / df['preclose'] - 1) * 100
    start_dt = pd.to_datetime(start) if start else None
    end_dt = pd.to_datetime(end) if end else None
    if start_dt is not None:
        df = df[df['time'] >= start_dt]
    if end_dt is not None:
        df = df[df['time'] <= end_dt]
    df = df.sort_values('time').tail(count)
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    out['code'] = norm
    out['time'] = out['time'].dt.strftime('%Y-%m-%d')
    cols = ['time', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'pctChg']
    return out[cols]

def _industry_from_eastmoney(session: requests.Session, norm: str):
    params = {'secid': _to_secid(norm), 'fields': 'f58,f84,f85,f127,f128,f116'}
    try:
        payload = _get_json(session, EM_QUOTE_URL, params=params, timeout=10)
        data = payload.get('data') or {}
        if not data:
            return None
        return {'code': norm, 'name': data.get('f58'), 'industry': data.get('f127'), 'industry_code': data.get('f128'), 'market_value': data.get('f116'), 'source': '东方财富'}
    except Exception:
        return None

def _get_boards_from_akshare(norm: str) -> list:
    """通过 akshare 获取个股所属板块"""
    digits = _code_digits(norm)
    boards = []
    try:
        import akshare as ak
        info = ak.stock_individual_info_em(symbol=digits)
        if info is not None and (not info.empty):
            industry_row = info[info['item'] == '行业']
            if not industry_row.empty:
                industry = industry_row['value'].values[0]
                boards.append({'group_key': 'industry', 'group_label': industry})
    except Exception:
        pass
    return boards

def _scan_boards_for_code(session: requests.Session, norm: str, max_boards: int=80):
    """扫描板块归属（东方财富接口）- 已弃用，改用 akshare"""
    return _get_boards_from_akshare(norm)

def cmd_industry(args):
    session = _build_session()
    norm = normalize_code(args.industry)
    industry_info = None
    try:
        r = session.get(EM_COMPANY_URL, params={'code': norm}, timeout=10)
        payload = r.json()
        if payload.get('jbzl'):
            jbzl = payload['jbzl']
            industry_info = {'code': norm, 'name': jbzl.get('agjc'), 'industry': jbzl.get('sshy'), 'industry_zjh': jbzl.get('sszjhhy'), 'source': '东方财富'}
    except Exception:
        pass
    if not industry_info:
        industry_info = _industry_from_eastmoney(session, norm)
    boards = _scan_boards_for_code(session, norm, max_boards=args.board_scan_limit) if args.with_boards else []
    output = {'code': norm, 'industry_info': industry_info, 'boards': boards, 'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    return output
FINANCIAL_FIELDS = {'REPORT_DATE': '报告期', 'NOTICE_DATE': '公告日期', 'EPSJB': '基本每股收益', 'BPS': '每股净资产', 'MGJYXJJE': '每股经营现金流', 'TOTALOPERATEREVE': '营业总收入', 'PARENTNETPROFIT': '归母净利润', 'KCFJCXSYJLR': '扣非净利润', 'TOTALOPERATEREVETZ': '营收同比增长率(%)', 'PARENTNETPROFITTZ': '归母净利润同比增长率(%)', 'KCFJCXSYJLRTZ': '扣非净利润同比增长率(%)', 'ROEJQ': '加权净资产收益率(%)', 'XSJLL': '销售净利率(%)', 'XSMLL': '销售毛利率(%)', 'ZZCJLL': '总资产净利率(%)', 'LD': '流动比率', 'SD': '速动比率', 'ZCFZL': '资产负债率(%)', 'QYCS': '权益乘数', 'TOAZZL': '总资产周转率', 'CHZZL': '存货周转率', 'YSZKZZL': '应收账款周转率', 'JYXJLYYSR': '经营现金净流量与营业收入比率(%)', 'XJLLB': '现金流量比率'}

def _financials_from_akshare(norm: str) -> pd.DataFrame:
    """Current Eastmoney indicators, then Sina abstract (transposed by period)."""
    import akshare as ak
    digits = _code_digits(norm)
    errors = []
    try:
        suffix = 'SH' if norm.startswith('sh') else 'BJ' if norm.startswith('bj') else 'SZ'
        df = ak.stock_financial_analysis_indicator_em(symbol=f'{digits}.{suffix}', indicator='按报告期')
        if df is not None and (not df.empty):
            df = df[[c for c in FINANCIAL_FIELDS if c in df.columns]].rename(columns=FINANCIAL_FIELDS)
            df['数据源'] = 'eastmoney'
            return df
    except Exception as exc:
        errors.append(f'eastmoney: {type(exc).__name__}: {str(exc)[:200]}')
    try:
        df = ak.stock_financial_abstract(symbol=digits)
        if df is not None and (not df.empty):
            periods = [c for c in df if re.fullmatch('\\d{8}', str(c))]
            data = df.drop_duplicates('指标').set_index('指标')[periods].T
            data.index.name = '报告期'
            data = data.reset_index()
            data['报告期'] = pd.to_datetime(data['报告期'], format='%Y%m%d').astype(str)
            data['数据源'] = 'sina'
            return data
    except Exception as exc:
        errors.append(f'sina: {type(exc).__name__}: {str(exc)[:200]}')
    df = pd.DataFrame()
    df.attrs['errors'] = errors
    return df

def _financial_single_from_akshare(norm: str, func_name: str) -> pd.DataFrame:
    df = _financials_from_akshare(norm)
    if df.empty:
        return df
    patterns = {'profit': '利润|净利率|毛利率|收益率|每股收益', 'growth': '同比|增长', 'balance': '负债|流动比率|速动比率|权益乘数|每股净资产', 'cashflow': '现金|现金流', 'dupont': '净资产收益率|销售净利率|总资产周转率|权益乘数'}
    fields = [c for c in df if c in ('报告期', '公告日期', '数据源') or re.search(patterns[func_name], c)]
    return df[fields]

def _filter_financial_periods(df, args):
    if df.empty:
        return df
    as_of = getattr(args, 'as_of', None)
    if as_of:
        if '公告日期' not in df:
            raise ValueError('as_of unavailable: source has no announcement dates')
        announced = pd.to_datetime(df['公告日期'], errors='coerce')
        df = df[announced.notna() & (announced <= pd.Timestamp(as_of))]
    periods = pd.to_datetime(df['报告期'], errors='coerce')
    mask = periods.notna()
    if args.start:
        mask &= periods >= pd.Timestamp(args.start)
    if args.end:
        mask &= periods <= pd.Timestamp(args.end)
    if args.year:
        mask &= periods.dt.year == int(args.year)
    if args.quarter:
        mask &= periods.dt.quarter == args.quarter
    return df[mask].sort_values('报告期', ascending=False).head(args.count)
