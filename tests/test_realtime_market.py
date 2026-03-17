"""
Tests for realtime market tools.
"""
import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.use_cases.realtime_market import (
    normalize_code,
    fetch_realtime_kline,
    fetch_technical_indicators,
    fetch_realtime_quote,
    FREQUENCY_MAP,
)


class TestNormalizeCode:
    """Test stock code normalization."""

    def test_sh_prefix_code(self):
        """Test code with sh prefix."""
        assert normalize_code('sh600519') == 'sh600519'
        assert normalize_code('sh000001') == 'sh000001'

    def test_sz_prefix_code(self):
        """Test code with sz prefix."""
        assert normalize_code('sz000001') == 'sz000001'
        assert normalize_code('sz399006') == 'sz399006'

    def test_baostock_format(self):
        """Test Baostock format (sh.600000)."""
        assert normalize_code('sh.600000') == 'sh600000'
        assert normalize_code('sz.000001') == 'sz000001'

    def test_joinquant_format(self):
        """Test JoinQuant format (600519.XSHG)."""
        assert normalize_code('600519.XSHG') == 'sh600519'
        assert normalize_code('000001.XSHE') == 'sz000001'

    def test_pure_number_shanghai(self):
        """Test pure number for Shanghai stocks (6xx)."""
        assert normalize_code('600519') == 'sh600519'
        assert normalize_code('601318') == 'sh601318'

    def test_pure_number_shenzhen(self):
        """Test pure number for Shenzhen stocks (0xx, 2xx, 3xx)."""
        assert normalize_code('000001') == 'sz000001'
        assert normalize_code('002415') == 'sz002415'
        assert normalize_code('300750') == 'sz300750'


class TestFetchRealtimeKline:
    """Test realtime K-line fetching."""

    def test_daily_kline(self):
        """Test fetching daily K-line data."""
        result = fetch_realtime_kline('sh600519', frequency='1d', count=5)
        
        assert result is not None
        assert 'sh600519' in result
        assert '1d' in result
        assert 'open' in result.lower() or '开盘' in result

    def test_minute_kline(self):
        """Test fetching minute K-line data."""
        result = fetch_realtime_kline('sh600519', frequency='60m', count=5)
        
        assert result is not None
        assert 'sh600519' in result
        assert '60m' in result

    def test_invalid_frequency(self):
        """Test with invalid frequency."""
        with pytest.raises(ValueError) as excinfo:
            fetch_realtime_kline('sh600519', frequency='invalid', count=5)
        assert 'Invalid frequency' in str(excinfo.value)

    def test_different_code_formats(self):
        """Test with different code formats."""
        # JoinQuant format
        result1 = fetch_realtime_kline('600519.XSHG', frequency='1d', count=3)
        assert result1 is not None
        
        # Pure number
        result2 = fetch_realtime_kline('600519', frequency='1d', count=3)
        assert result2 is not None
        
        # Baostock format
        result3 = fetch_realtime_kline('sh.600519', frequency='1d', count=3)
        assert result3 is not None

    def test_json_format(self):
        """Test JSON output format."""
        result = fetch_realtime_kline('sh600519', frequency='1d', count=3, format='json')
        
        assert result is not None
        # JSON should contain these keywords
        assert 'open' in result.lower() or 'close' in result.lower()


class TestFetchTechnicalIndicators:
    """Test technical indicators calculation."""

    def test_default_indicators(self):
        """Test with default indicators (MA, MACD, KDJ, BOLL, RSI)."""
        result = fetch_technical_indicators('sh600519', frequency='1d', count=60)
        
        assert result is not None
        # Check for indicator columns
        assert 'MA' in result or 'MA5' in result
        assert 'MACD' in result

    def test_specific_indicators(self):
        """Test with specific indicators."""
        result = fetch_technical_indicators(
            'sh600519', 
            frequency='1d', 
            count=60,
            indicators=['MA', 'KDJ', 'BOLL']
        )
        
        assert result is not None
        assert 'MA' in result
        assert 'KDJ' in result
        assert 'BOLL' in result

    def test_single_indicator(self):
        """Test with single indicator."""
        result = fetch_technical_indicators(
            'sh600519',
            frequency='1d',
            count=60,
            indicators=['RSI']
        )
        
        assert result is not None
        assert 'RSI' in result

    def test_invalid_indicator(self):
        """Test with invalid indicator (should be ignored gracefully)."""
        # Invalid indicators should be ignored, not cause errors
        result = fetch_technical_indicators(
            'sh600519',
            frequency='1d',
            count=30,
            indicators=['INVALID_INDICATOR']
        )
        # Should return data without crashing
        assert result is not None


class TestFetchRealtimeQuote:
    """Test realtime quote fetching."""

    def test_quote_basic(self):
        """Test basic quote fetching."""
        result = fetch_realtime_quote('sh600519')
        
        assert result is not None
        assert 'sh600519' in result
        # Check for key fields
        assert '收盘价' in result or 'close' in result.lower()

    def test_quote_with_different_codes(self):
        """Test quote with different code formats."""
        result = fetch_realtime_quote('000001')
        assert result is not None
        assert 'sz000001' in result  # Should be normalized to sz000001

    def test_quote_json_format(self):
        """Test quote with JSON format."""
        result = fetch_realtime_quote('sh600519', format='json')
        assert result is not None


class TestFrequencyMap:
    """Test frequency mapping."""

    def test_all_frequencies_exist(self):
        """Test that all expected frequencies are defined."""
        expected = ['1m', '5m', '15m', '30m', '60m', '1d', '1w', '1M']
        for freq in expected:
            assert freq in FREQUENCY_MAP

    def test_frequency_values(self):
        """Test frequency mapping values."""
        assert FREQUENCY_MAP['1d'] == '1d'
        assert FREQUENCY_MAP['60m'] == '60m'
        assert FREQUENCY_MAP['1m'] == '1m'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
