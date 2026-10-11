"""
Realtime quote use cases for live stock data.
Provides real-time quotes using minute K-line aggregation.
"""
from datetime import datetime, date, time
from typing import List, Optional
from datetime import timedelta, time as time_type
import json
import queue
import threading
import time as clock

import pandas as pd

from src.ashare_data_source import AshareDataSource
from src.formatting.markdown_formatter import format_table_output
from src.services.validation import validate_output_format


def _get_market_status() -> str:
    now = datetime.now()
    current_time = now.time()
    if now.weekday() >= 5:
        return 'closed'
    morning_start = time_type(9, 30)
    morning_end = time_type(11, 30)
    afternoon_start = time_type(13, 0)
    afternoon_end = time_type(15, 0)
    if morning_start <= current_time <= morning_end:
        return 'trading'
    elif afternoon_start <= current_time <= afternoon_end:
        return 'trading'
    elif time_type(9, 0) <= current_time < morning_start:
        return 'pre_market'
    elif current_time > afternoon_end:
        return 'post_market'
    else:
        return 'closed'

def _is_trading_session(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    current_time = now.time()
    morning_start = time_type(9, 30)
    morning_end = time_type(11, 30)
    afternoon_start = time_type(13, 0)
    afternoon_end = time_type(15, 0)
    return (morning_start <= current_time <= morning_end) or (afternoon_start <= current_time <= afternoon_end)

def _is_forming_minute_bar(bar_ts: pd.Timestamp, freq_minutes: int, now: datetime) -> bool:
    if pd.Timestamp(bar_ts).date() != now.date():
        return False
    if not _is_trading_session(now):
        return False
    return now < (pd.Timestamp(bar_ts).to_pydatetime() + timedelta(minutes=freq_minutes))

def _drop_forming_minute_bars(df: pd.DataFrame, freq_minutes: int, now: Optional[datetime] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", None))
    now = now or datetime.now()
    trimmed = df.sort_index().copy()
    if _is_forming_minute_bar(trimmed.index[-1], freq_minutes, now):
        trimmed = trimmed.iloc[:-1]
    return trimmed

def _aggregate_intraday_data(
    df_min: pd.DataFrame,
    today: date,
    now: Optional[datetime] = None,
    freq_minutes: int = 5,
) -> dict:
    df_today = df_min[df_min.index.date == today].sort_index().copy()
    df_today = _drop_forming_minute_bars(df_today, freq_minutes=freq_minutes, now=now)
    if df_today.empty:
        return None
    return {
        'open': df_today.iloc[0]['open'],
        'high': df_today['high'].max(),
        'low': df_today['low'].min(),
        'close': df_today.iloc[-1]['close'],
        'volume': df_today['volume'].sum(),
        'bar_time': df_today.index[-1],
    }


def fetch_realtime_quote(
    ashare_data_source: AshareDataSource,
    code: str,
    format: str = 'markdown',
) -> str:
    """
    Fetch realtime quote snapshot for a stock.
    
    Core fix: Use minute K-line aggregation for today's data instead of daily data.
    
    Steps:
    1. Get yesterday's close from daily K-line (iloc[-2])
    2. Get 5-minute K-line tail(48), filter today's data, aggregate OHLCV
    3. Calculate change percentage: (latest - prev_close) / prev_close
    4. Fallback to 15-minute K-line if 5m fails
    5. Mark as "最近" if today has no data
    6. Add market status indicator
    
    Args:
        ashare_data_source: AshareDataSource instance
        code: Stock code
        format: Output format: 'markdown' | 'json' | 'csv'
    
    Returns:
        Realtime quote data with correct date and change calculation
    """
    validate_output_format(format)
    
    normalized_code = ashare_data_source.normalize_code(code)
    today = date.today()
    now = datetime.now()
    
    try:
        # Step 1: Get daily K-line for yesterday's close
        df_day = ashare_data_source.get_historical_k_data(
            code=normalized_code,
            start_date='',
            end_date='',
            frequency='1d',
        )
        
        if df_day is None or df_day.empty or len(df_day) < 2:
            return f"未找到股票 {code} 的日线数据"
        
        # Yesterday's close (iloc[-2] because iloc[-1] might be today if market is open)
        # Actually, we need to check: if the last row is today, use iloc[-2] as yesterday
        # If the last row is not today, then iloc[-1] is the most recent trading day
        last_date = df_day.index[-1].date()
        
        if last_date == today:
            # Market is open or after hours today
            prev_close = df_day.iloc[-2]['close']
            prev_date = df_day.index[-2].date()
        else:
            # Market is closed, last row is most recent trading day
            prev_close = df_day.iloc[-1]['close']
            prev_date = last_date
        
        # Step 2: Get minute K-line for today's data
        today_data = None
        minute_freq = '5m'
        
        # Try 5m first only on weekdays.
        try:
            if today.weekday() >= 5:
                raise ValueError('weekend')
            df_min = ashare_data_source.get_historical_k_data(
                code=normalized_code,
                start_date='',
                end_date='',
                frequency='5m',
            )
            if df_min is not None and not df_min.empty:
                today_data = _aggregate_intraday_data(df_min, today, now=now, freq_minutes=5)
        except Exception:
            pass
        
        # Fallback to 15m if 5m fails
        if today_data is None and today.weekday() < 5:
            minute_freq = '15m'
            try:
                df_min = ashare_data_source.get_historical_k_data(
                    code=normalized_code,
                    start_date='',
                    end_date='',
                    frequency='15m',
                )
                if df_min is not None and not df_min.empty:
                    today_data = _aggregate_intraday_data(df_min, today, now=now, freq_minutes=15)
            except Exception:
                pass
        
        # Determine display values
        if today_data:
            # Today has data
            latest_price = today_data['close']
            today_open = today_data['open']
            today_high = today_data['high']
            today_low = today_data['low']
            today_volume = today_data['volume']
            display_date = today.strftime('%Y-%m-%d')
            date_label = '今日'
        else:
            # No data today, use most recent daily data
            latest_row = df_day.iloc[-1]
            prev_close = df_day.iloc[-2]['close']
            latest_price = latest_row['close']
            today_open = latest_row['open']
            today_high = latest_row['high']
            today_low = latest_row['low']
            today_volume = latest_row['volume']
            display_date = last_date.strftime('%Y-%m-%d')
            date_label = '最近'
        
        # Step 3: Calculate change
        change = latest_price - prev_close
        change_pct = (change / prev_close * 100) if prev_close > 0 else 0
        
        # Step 4: Determine market status
        market_status = _get_market_status()
        status_map = {
            'trading': '交易中',
            'closed': '休市',
            'pre_market': '盘前',
            'post_market': '盘后',
        }
        
        # Build result
        result = {
            '代码': code,
            '日期': display_date,
            '最新价': round(latest_price, 2),
            '今开': round(today_open, 2),
            '最高': round(today_high, 2),
            '最低': round(today_low, 2),
            '昨收': round(prev_close, 2),
            '涨跌额': round(change, 2),
            '涨跌幅(%)': round(change_pct, 2),
            '成交量': int(today_volume),
            '市场状态': status_map.get(market_status, market_status),
            '数据源': '新浪/腾讯',
            '更新时间': now.strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # Add date label if not today
        if today_data is None:
            result['备注'] = f'非交易日或休市，显示最近交易日({date_label})数据'
        
        df_result = pd.DataFrame([result])
        
        meta = {
            'code': code,
            'normalized_code': normalized_code,
            'data_source': 'Sina/Tencent (AshareDataSource)',
            'update_time': now.strftime('%Y-%m-%d %H:%M:%S'),
            'minute_freq': minute_freq if today_data else 'N/A',
        }
        
        return format_table_output(df_result, format=format, max_rows=1, meta=meta)
        
    except Exception as e:
        return f"获取实时行情失败: {str(e)}"


def fetch_intraday_minute_kline(
    ashare_data_source: AshareDataSource,
    code: str,
    frequency: str = '5m',
    format: str = 'markdown',
) -> str:
    """
    Fetch intraday minute K-line data for a stock (today only).
    
    Args:
        ashare_data_source: AshareDataSource instance
        code: Stock code
        frequency: Minute frequency: '1m', '5m', '15m', '30m', '60m'
        format: Output format: 'markdown' | 'json' | 'csv'
    
    Returns:
        Today's minute K-line data with Chinese column names
    """
    validate_output_format(format)
    
    valid_freqs = ['1m', '5m', '15m', '30m', '60m']
    if frequency not in valid_freqs:
        raise ValueError(f"Invalid frequency '{frequency}'. Valid: {valid_freqs}")
    
    normalized_code = ashare_data_source.normalize_code(code)
    today = date.today()
    
    try:
        df = ashare_data_source.get_historical_k_data(
            code=normalized_code,
            start_date='',
            end_date='',
            frequency=frequency,
        )
        
        if df is None or df.empty:
            return f"未找到股票 {code} 的分钟K线数据"
        
        # Filter today's data only
        df_today = df[df.index.date == today].copy()
        
        if df_today.empty:
            return format_table_output(pd.DataFrame(), format=format, max_rows=0,
                                       meta={"status":"empty","code":code,"date":str(today),"message":"No intraday bars for requested date"})
        
        # Reset index and rename columns to Chinese
        df_result = df_today.reset_index()
        df_result = df_result.rename(columns={df_result.columns[0]: '时间', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
        df_result = df_result[['时间', '开盘', '最高', '最低', '收盘', '成交量']]
        
        # Round numeric columns
        df_result['开盘'] = df_result['开盘'].round(2)
        df_result['最高'] = df_result['最高'].round(2)
        df_result['最低'] = df_result['最低'].round(2)
        df_result['收盘'] = df_result['收盘'].round(2)
        
        meta = {
            'code': code,
            'normalized_code': normalized_code,
            'frequency': frequency,
            'date': today.strftime('%Y-%m-%d'),
            'data_source': 'Sina/Tencent (AshareDataSource)',
        }
        
        return format_table_output(df_result, format=format, max_rows=len(df_result), meta=meta)
        
    except Exception as e:
        return f"获取分钟K线失败: {str(e)}"


def fetch_realtime_multi_quote(ashare_data_source: AshareDataSource, codes: List[str], format: str = "markdown", max_seconds: float = 30) -> str:
    """Fetch up to 10 quotes concurrently within a 30-second batch deadline."""
    validate_output_format(format)
    if not codes or len(codes)>10:
        raise ValueError("Provide 1..10 stock codes")
    completed=queue.Queue()
    def fetch(index,code):
        try:
            payload=json.loads(fetch_realtime_quote(ashare_data_source,code,format="json"))
            row=payload["data"][0]
            row=dict(row,名称=row.get("名称","N/A"),状态="成功")
        except Exception as exc:
            row={"代码":code,"最新价":"N/A","涨跌幅(%)":None,"状态":f"失败: {exc}"}
        completed.put((index,row))
    started=clock.monotonic();deadline=started+min(30,max(0.01,max_seconds))
    for index,code in enumerate(codes):threading.Thread(target=fetch,args=(index,code),daemon=True).start()
    pending=set(range(len(codes)));results=[]
    while pending:
        try:index,row=completed.get(timeout=max(0,deadline-clock.monotonic()))
        except queue.Empty:break
        pending.remove(index);results.append(row)
    results.extend({"代码":codes[index],"最新价":"N/A","涨跌幅(%)":None,"状态":"超时"} for index in sorted(pending))
    results.sort(key=lambda row:row.get("涨跌幅(%)") if isinstance(row.get("涨跌幅(%)"),(int,float)) else float("-inf"),reverse=True)
    failed=[row["代码"] for row in results if row.get("状态")!="成功"]
    meta={"requested":len(codes),"success":len(codes)-len(failed),"failed_codes":failed,
          "elapsed_sec":round(clock.monotonic()-started,3),"data_source":"Sina/Tencent"}
    return format_table_output(pd.DataFrame(results),format=format,max_rows=len(results),meta=meta)
