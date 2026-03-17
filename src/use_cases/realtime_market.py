"""Realtime market use cases for live stock data."""
import sys
import os
from typing import Optional, List

# Add Ashare library path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'Ashare'))

import pandas as pd
from Ashare import get_price
from MyTT import (
    MA, EMA, MACD, KDJ, RSI, WR, BOLL, BIAS, CCI, ATR, 
    DMI, TAQ, TRIX, VR, EMV, DPO, BRAR, DMA, MTM, ROC, BBI
)

from src.formatting.markdown_formatter import format_table_output
from src.services.validation import validate_output_format


# Frequency mapping: user-friendly to Ashare format
FREQUENCY_MAP = {
    '1m': '1m',    # 1分钟
    '5m': '5m',    # 5分钟
    '15m': '15m',  # 15分钟
    '30m': '30m',  # 30分钟
    '60m': '60m',  # 60分钟
    '1d': '1d',    # 日线
    '1w': '1w',    # 周线
    '1M': '1M',    # 月线
}


def normalize_code(code: str) -> str:
    """
    Normalize stock code to Ashare format.
    Supports formats: sh600000, 600000, sh.600000, sz000001, 000001.XSHE, 600519.XSHG
    """
    code = code.strip()
    
    # Baostock format: sh.600000 -> sh600000 (handle this first before sh prefix check)
    if '.' in code:
        parts = code.split('.')
        if len(parts) == 2:
            prefix, suffix = parts[0].lower(), parts[1].upper()
            # Baostock format: sh.600000 or sz.000001
            if prefix in ('sh', 'sz') and suffix.isdigit():
                return f"{prefix}{suffix}"
            # JQ format: 600519.XSHG or 000001.XSHE
            if suffix == 'XSHG':
                return f"sh{prefix}"
            if suffix == 'XSHE':
                return f"sz{prefix}"
    
    # Already in correct format (sh/sz prefix with number)
    if code.startswith('sh') or code.startswith('sz'):
        return code
    
    # Pure number: determine exchange by code rules
    if code.isdigit():
        # 上海: 6开头
        if code.startswith('6'):
            return f"sh{code}"
        # 深圳: 0开头(主板), 3开头(创业板), 2开头(中小板)
        elif code.startswith(('0', '2', '3')):
            return f"sz{code}"
    
    return code


def fetch_realtime_kline(
    code: str,
    frequency: str = '1d',
    count: int = 60,
    format: str = 'markdown',
) -> str:
    """
    Fetch realtime K-line data for a stock.
    
    Args:
        code: Stock code (supports multiple formats: sh600000, 600000, sh.600000, 600519.XSHG)
        frequency: K-line frequency. Options: '1m', '5m', '15m', '30m', '60m', '1d', '1w', '1M'
        count: Number of K-line bars to fetch (default 60)
        format: Output format: 'markdown' | 'json' | 'csv'
    """
    validate_output_format(format)
    
    if frequency not in FREQUENCY_MAP:
        raise ValueError(f"Invalid frequency '{frequency}'. Valid options: {list(FREQUENCY_MAP.keys())}")
    
    normalized_code = normalize_code(code)
    ashare_freq = FREQUENCY_MAP[frequency]
    
    try:
        df = get_price(normalized_code, frequency=ashare_freq, count=count)
        
        if df is None or df.empty:
            return f"未找到股票 {code} 的数据"
        
        # Reset index for display
        df = df.reset_index()
        df.columns = ['time', 'open', 'close', 'high', 'low', 'volume']
        
        # Format numbers
        df['open'] = df['open'].round(2)
        df['close'] = df['close'].round(2)
        df['high'] = df['high'].round(2)
        df['low'] = df['low'].round(2)
        
        meta = {
            'code': code,
            'normalized_code': normalized_code,
            'frequency': frequency,
            'count': count,
            'data_source': 'Ashare (腾讯/新浪)',
        }
        
        return format_table_output(df, format=format, max_rows=count, meta=meta)
        
    except Exception as e:
        return f"获取实时K线数据失败: {str(e)}"


def fetch_technical_indicators(
    code: str,
    frequency: str = '1d',
    count: int = 120,
    indicators: Optional[List[str]] = None,
    format: str = 'markdown',
) -> str:
    """
    Calculate technical indicators for a stock.
    
    Args:
        code: Stock code
        frequency: K-line frequency. Options: '1m', '5m', '15m', '30m', '60m', '1d', '1w', '1M'
        count: Number of K-line bars to fetch (default 120 for accurate indicator calculation)
        indicators: List of indicators to calculate. 
                    Options: 'MA', 'EMA', 'MACD', 'KDJ', 'RSI', 'WR', 'BOLL', 'BIAS', 'CCI', 'ATR', 'DMI', 'TAQ'
                    Default: ['MA', 'MACD', 'KDJ', 'BOLL', 'RSI']
        format: Output format: 'markdown' | 'json' | 'csv'
    """
    validate_output_format(format)
    
    if frequency not in FREQUENCY_MAP:
        raise ValueError(f"Invalid frequency '{frequency}'. Valid options: {list(FREQUENCY_MAP.keys())}")
    
    if indicators is None or len(indicators) == 0:
        indicators = ['MA', 'MACD', 'KDJ', 'BOLL', 'RSI']
    
    normalized_code = normalize_code(code)
    ashare_freq = FREQUENCY_MAP[frequency]
    
    try:
        df = get_price(normalized_code, frequency=ashare_freq, count=count)
        
        if df is None or df.empty:
            return f"未找到股票 {code} 的数据"
        
        # Extract OHLCV data
        CLOSE = df['close'].values
        OPEN = df['open'].values
        HIGH = df['high'].values
        LOW = df['low'].values
        VOL = df['volume'].values if 'volume' in df.columns else None
        
        # Reset index and prepare result dataframe
        result_df = df.reset_index()
        result_df.columns = ['time', 'open', 'close', 'high', 'low', 'volume']
        
        # Calculate requested indicators
        calculated = []
        
        for ind in indicators:
            ind = ind.upper()
            
            if ind == 'MA':
                result_df['MA5'] = MA(CLOSE, 5)
                result_df['MA10'] = MA(CLOSE, 10)
                result_df['MA20'] = MA(CLOSE, 20)
                calculated.append('MA(5,10,20)')
                
            elif ind == 'EMA':
                result_df['EMA12'] = EMA(CLOSE, 12)
                result_df['EMA26'] = EMA(CLOSE, 26)
                calculated.append('EMA(12,26)')
                
            elif ind == 'MACD':
                DIF, DEA, MACD_val = MACD(CLOSE)
                result_df['MACD_DIF'] = DIF
                result_df['MACD_DEA'] = DEA
                result_df['MACD'] = MACD_val
                calculated.append('MACD(12,26,9)')
                
            elif ind == 'KDJ':
                K, D, J = KDJ(CLOSE, HIGH, LOW)
                result_df['KDJ_K'] = K
                result_df['KDJ_D'] = D
                result_df['KDJ_J'] = J
                calculated.append('KDJ(9,3,3)')
                
            elif ind == 'RSI':
                result_df['RSI'] = RSI(CLOSE)
                calculated.append('RSI(24)')
                
            elif ind == 'WR':
                WR1, WR2 = WR(CLOSE, HIGH, LOW)
                result_df['WR10'] = WR1
                result_df['WR6'] = WR2
                calculated.append('WR(10,6)')
                
            elif ind == 'BOLL':
                UP, MID, LOW_BOLL = BOLL(CLOSE)
                result_df['BOLL_UP'] = UP
                result_df['BOLL_MID'] = MID
                result_df['BOLL_LOW'] = LOW_BOLL
                calculated.append('BOLL(20,2)')
                
            elif ind == 'BIAS':
                BIAS1, BIAS2, BIAS3 = BIAS(CLOSE)
                result_df['BIAS6'] = BIAS1
                result_df['BIAS12'] = BIAS2
                result_df['BIAS24'] = BIAS3
                calculated.append('BIAS(6,12,24)')
                
            elif ind == 'CCI':
                result_df['CCI'] = CCI(CLOSE, HIGH, LOW)
                calculated.append('CCI(14)')
                
            elif ind == 'ATR':
                result_df['ATR'] = ATR(CLOSE, HIGH, LOW)
                calculated.append('ATR(20)')
                
            elif ind == 'DMI':
                PDI, MDI, ADX, ADXR = DMI(CLOSE, HIGH, LOW)
                result_df['DMI_PDI'] = PDI
                result_df['DMI_MDI'] = MDI
                result_df['DMI_ADX'] = ADX
                result_df['DMI_ADXR'] = ADXR
                calculated.append('DMI(14,6)')
                
            elif ind == 'TAQ':
                UP, MID, DOWN = TAQ(HIGH, LOW, 20)
                result_df['TAQ_UP'] = UP
                result_df['TAQ_MID'] = MID
                result_df['TAQ_DOWN'] = DOWN
                calculated.append('TAQ(20)')
        
        # Round numeric columns
        for col in result_df.columns:
            if result_df[col].dtype in ['float64', 'float32']:
                result_df[col] = result_df[col].round(2)
        
        meta = {
            'code': code,
            'normalized_code': normalized_code,
            'frequency': frequency,
            'indicators': ', '.join(calculated),
            'data_source': 'Ashare + MyTT',
        }
        
        return format_table_output(result_df, format=format, max_rows=min(count, 50), meta=meta)
        
    except Exception as e:
        return f"计算技术指标失败: {str(e)}"


def fetch_realtime_quote(
    code: str,
    format: str = 'markdown',
) -> str:
    """
    Fetch realtime quote snapshot for a stock.
    Returns the latest K-line data with key metrics.
    
    Args:
        code: Stock code
        format: Output format: 'markdown' | 'json' | 'csv'
    """
    validate_output_format(format)
    
    normalized_code = normalize_code(code)
    
    try:
        # Get latest daily data
        df_day = get_price(normalized_code, frequency='1d', count=5)
        
        if df_day is None or df_day.empty:
            return f"未找到股票 {code} 的数据"
        
        # Get latest minute data (if market is open)
        try:
            df_min = get_price(normalized_code, frequency='1m', count=5)
            has_intraday = df_min is not None and not df_min.empty
        except:
            has_intraday = False
        
        # Latest daily data
        latest = df_day.iloc[-1]
        prev = df_day.iloc[-2] if len(df_day) > 1 else None
        
        # Calculate change
        change = latest['close'] - prev['close'] if prev is not None else 0
        change_pct = (change / prev['close'] * 100) if prev is not None and prev['close'] > 0 else 0
        
        result = {
            '代码': code,
            '日期': str(latest.name)[:10] if hasattr(latest, 'name') else '',
            '开盘价': round(latest['open'], 2),
            '最高价': round(latest['high'], 2),
            '最低价': round(latest['low'], 2),
            '收盘价': round(latest['close'], 2),
            '成交量': int(latest['volume']),
            '涨跌额': round(change, 2),
            '涨跌幅(%)': round(change_pct, 2),
        }
        
        if has_intraday:
            latest_min = df_min.iloc[-1]
            result['最新价(分钟)'] = round(latest_min['close'], 2)
        
        df_result = pd.DataFrame([result])
        
        meta = {
            'code': code,
            'normalized_code': normalized_code,
            'data_source': 'Ashare (腾讯/新浪)',
        }
        
        return format_table_output(df_result, format=format, max_rows=1, meta=meta)
        
    except Exception as e:
        return f"获取实时行情失败: {str(e)}"
