"""
Realtime market tools for the MCP server.
Provides live stock data using Ashare library (Tencent/Sina data sources).
"""
import logging
from typing import List, Optional

from mcp.server.fastmcp import FastMCP
from src.use_cases.realtime_market import (
    fetch_realtime_kline,
    fetch_technical_indicators,
    fetch_realtime_quote,
)

logger = logging.getLogger(__name__)


def register_realtime_market_tools(app: FastMCP):
    """
    Register realtime market data tools with the MCP app.
    
    These tools use Ashare library which fetches data from Tencent/Sina APIs.
    No login required, supports intraday data.
    
    Args:
        app: The FastMCP app instance
    """

    @app.tool()
    def get_realtime_kline(
        code: str,
        frequency: str = "1d",
        count: int = 60,
        format: str = "markdown",
    ) -> str:
        """
        Fetches realtime K-line (OHLCV) data for a Chinese A-share stock.
        
        This tool uses Ashare library which fetches live data from Tencent/Sina APIs.
        No login required, supports intraday data during trading hours.

        Args:
            code: The stock code. Supports multiple formats:
                  - 'sh600000' or 'sz000001' (Tongdaxin format)
                  - '600000' or '000001' (pure number, auto-detect exchange)
                  - 'sh.600000' or 'sz.000001' (Baostock format)
                  - '600519.XSHG' or '000001.XSHE' (JoinQuant format)
            frequency: K-line frequency. Valid options:
                       '1m': 1 minute
                       '5m': 5 minutes
                       '15m': 15 minutes
                       '30m': 30 minutes
                       '60m': 60 minutes
                       '1d': daily (default)
                       '1w': weekly
                       '1M': monthly
            count: Number of K-line bars to fetch. Defaults to 60.
            format: Output format: 'markdown' | 'json' | 'csv'. Defaults to 'markdown'.

        Returns:
            K-line data table with columns: time, open, close, high, low, volume
        """
        logger.info(
            f"Tool 'get_realtime_kline' called for {code} (freq={frequency}, count={count})"
        )
        return fetch_realtime_kline(
            code=code,
            frequency=frequency,
            count=count,
            format=format,
        )

    @app.tool()
    def get_technical_indicators(
        code: str,
        frequency: str = "1d",
        count: int = 120,
        indicators: Optional[List[str]] = None,
        format: str = "markdown",
    ) -> str:
        """
        Calculates technical indicators for a Chinese A-share stock.
        
        This tool fetches K-line data using Ashare and calculates indicators using MyTT library.
        Combines realtime data with technical analysis.

        Args:
            code: The stock code. Supports multiple formats (same as get_realtime_kline).
            frequency: K-line frequency. Valid options:
                       '1m', '5m', '15m', '30m', '60m', '1d', '1w', '1M'.
                       Defaults to '1d'.
            count: Number of K-line bars to fetch. Defaults to 120 for accurate indicator calculation.
            indicators: List of indicators to calculate. Valid options:
                        - 'MA': Moving Average (5, 10, 20日均线)
                        - 'EMA': Exponential Moving Average (12, 26日)
                        - 'MACD': Moving Average Convergence Divergence
                        - 'KDJ': Stochastic Oscillator
                        - 'RSI': Relative Strength Index
                        - 'WR': Williams %R
                        - 'BOLL': Bollinger Bands
                        - 'BIAS': Bias Ratio
                        - 'CCI': Commodity Channel Index
                        - 'ATR': Average True Range
                        - 'DMI': Directional Movement Index
                        - 'TAQ': Donchian Channel (唐安奇通道)
                        Defaults to ['MA', 'MACD', 'KDJ', 'BOLL', 'RSI'] if not specified.
            format: Output format: 'markdown' | 'json' | 'csv'. Defaults to 'markdown'.

        Returns:
            K-line data with calculated technical indicators.
        """
        logger.info(
            f"Tool 'get_technical_indicators' called for {code} (freq={frequency}, indicators={indicators})"
        )
        return fetch_technical_indicators(
            code=code,
            frequency=frequency,
            count=count,
            indicators=indicators,
            format=format,
        )

    @app.tool()
    def get_realtime_quote(
        code: str,
        format: str = "markdown",
    ) -> str:
        """
        Fetches realtime quote snapshot for a Chinese A-share stock.
        
        Returns the latest trading data including price, volume, and daily change.
        Quick overview of current stock status.

        Args:
            code: The stock code. Supports multiple formats (same as get_realtime_kline).
            format: Output format: 'markdown' | 'json' | 'csv'. Defaults to 'markdown'.

        Returns:
            Latest quote data with: 日期, 开盘价, 最高价, 最低价, 收盘价, 成交量, 涨跌额, 涨跌幅
        """
        logger.info(f"Tool 'get_realtime_quote' called for {code}")
        return fetch_realtime_quote(
            code=code,
            format=format,
        )
