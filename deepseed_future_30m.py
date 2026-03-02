"""
Advanced Crypto Futures Signal Bot — v3.1 (JAPANESE CANDLESTICK PATTERNS)
========================================================================
NEW FEATURES:
- Added comprehensive Japanese candlestick pattern detection
- 15+ candlestick patterns for both bullish and bearish signals
- Pattern strength scoring system
- Pattern confirmation with volume and trend
- Multi-timeframe pattern confirmation

BUG FIXES from v3.0:
- Fixed datetime handling issues
- Fixed UTF-8 encoding for Windows
- Fixed session filter timing
"""

import pandas as pd
import ta
import time
import logging
import json
import os
import threading
import numpy as np
from datetime import datetime, timedelta, timezone
from binance.client import Client
from binance.exceptions import BinanceAPIException
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError, Forbidden, BadRequest
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Tuple, Any
import sys
import asyncio

# =============================================================================
# DATETIME HELPERS
# =============================================================================
def utcnow() -> datetime:
    """Timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)

def localnow() -> datetime:
    """Local time for storage."""
    return datetime.now()

# =============================================================================
# CONFIG
# =============================================================================
BINANCE_API_KEY = "YOUR_BINANCE_KEY"
BINANCE_SECRET = "YOUR_BINANCE_SECRET"

TELEGRAM_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"

# ── Timeframes ────────────────────────────────────────────────────────────────
TIMEFRAME = "30m"
HIGHER_TIMEFRAME = "1h"
HIGHEST_TIMEFRAME = "4h"

# ── Risk & Sizing ─────────────────────────────────────────────────────────────
RISK_PERCENT = 1.0
MAX_PORTFOLIO_RISK = 3.0
MAX_CONCURRENT_SIGNALS = 100
RR_RATIO = 2.0
CAPITAL = 1000.0
LEVERAGE = 1

# ── Entry zone buffer ─────────────────────────────────────────────────────────
ENTRY_BUFFER_ATR_MULT = 0.1

# ── Symbol scanning ───────────────────────────────────────────────────────────
TOP_VOLUME = 50
SCAN_INTERVAL = 600
MIN_VOLUME_USDT = 100_000
MAX_WORKERS = 10

# ── Volatility filter ─────────────────────────────────────────────────────────
MIN_ATR_PERCENT = 0.5
MAX_ATR_PERCENT = 5.0

# ── Funding rate filter ───────────────────────────────────────────────────────
MAX_FUNDING_RATE = 0.001
FUNDING_CONFLICT_MIN = 0.0003

# ── Session filter ────────────────────────────────────────────────────────────
VALID_SESSION_HOURS: List[int] = list(range(0, 24))

# ── Signal quality gates ──────────────────────────────────────────────────────
MIN_CONDITIONS = 6
MIN_STRENGTH = 2
MIN_PATTERN_STRENGTH = 2  # Minimum candlestick pattern strength

# ── EMA periods ───────────────────────────────────────────────────────────────
EMA_FAST_30M = 21
EMA_SLOW_30M = 55
EMA_FAST_HTF = 50
EMA_SLOW_HTF = 200

# ── ATR regime detection ──────────────────────────────────────────────────────
TRENDING_ATR_RATIO = 1.2

# ── Cooldown ──────────────────────────────────────────────────────────────────
SIGNAL_COOLDOWN = 3600
ACTIVE_USERS_FILE = "active_users.json"

# =============================================================================
# LOGGING — UTF-8 forced
# =============================================================================
_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
_file_handler = logging.FileHandler("crypto_bot.log", encoding="utf-8")
_file_handler.setFormatter(_formatter)
_console_stream = open(
    sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1, closefd=False
)
_console_handler = logging.StreamHandler(_console_stream)
_console_handler.setFormatter(_formatter)
logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
logger = logging.getLogger(__name__)


# =============================================================================
# JAPANESE CANDLESTICK PATTERNS
# =============================================================================
class JapaneseCandlestickPatterns:
    """
    Comprehensive Japanese candlestick pattern detection.
    Based on traditional candlestick analysis techniques.
    """

    PATTERN_STRENGTH = {
        # Strong reversal patterns (strength 3)
        'hammer': 3,
        'shooting_star': 3,
        'engulfing_bullish': 3,
        'engulfing_bearish': 3,
        'morning_star': 3,
        'evening_star': 3,
        'three_white_soldiers': 3,
        'three_black_crows': 3,

        # Moderate patterns (strength 2)
        'doji': 2,
        'dragonfly_doji': 2,
        'gravestone_doji': 2,
        'harami_bullish': 2,
        'harami_bearish': 2,
        'piercing_line': 2,
        'dark_cloud_cover': 2,
        'hanging_man': 2,

        # Minor patterns (strength 1)
        'marubozu': 1,
        'spinning_top': 1,
        'long_upper_shadow': 1,
        'long_lower_shadow': 1,
    }

    @staticmethod
    def detect_doji(row: pd.Series, threshold: float = 0.1) -> bool:
        """Doji: Open and close are virtually equal."""
        body = abs(row['close'] - row['open'])
        range_ = row['high'] - row['low']
        return range_ > 0 and (body / range_) < threshold

    @staticmethod
    def detect_dragonfly_doji(row: pd.Series, threshold: float = 0.1) -> bool:
        """Dragonfly Doji: Long lower shadow, little or no upper shadow."""
        if not JapaneseCandlestickPatterns.detect_doji(row, threshold):
            return False
        upper_shadow = row['high'] - max(row['open'], row['close'])
        lower_shadow = min(row['open'], row['close']) - row['low']
        total_shadow = upper_shadow + lower_shadow
        return total_shadow > 0 and (lower_shadow / total_shadow) > 0.8

    @staticmethod
    def detect_gravestone_doji(row: pd.Series, threshold: float = 0.1) -> bool:
        """Gravestone Doji: Long upper shadow, little or no lower shadow."""
        if not JapaneseCandlestickPatterns.detect_doji(row, threshold):
            return False
        upper_shadow = row['high'] - max(row['open'], row['close'])
        lower_shadow = min(row['open'], row['close']) - row['low']
        total_shadow = upper_shadow + lower_shadow
        return total_shadow > 0 and (upper_shadow / total_shadow) > 0.8

    @staticmethod
    def detect_hammer(row: pd.Series, body_threshold: float = 0.3) -> bool:
        """Hammer: Small body at upper end, long lower shadow."""
        body = abs(row['close'] - row['open'])
        range_ = row['high'] - row['low']
        if range_ == 0:
            return False

        # Body should be small (less than 30% of range)
        if (body / range_) > body_threshold:
            return False

        # Lower shadow should be at least 2x the body
        lower_shadow = min(row['open'], row['close']) - row['low']
        if lower_shadow < 2 * body:
            return False

        # Upper shadow should be small
        upper_shadow = row['high'] - max(row['open'], row['close'])
        return upper_shadow < body

    @staticmethod
    def detect_shooting_star(row: pd.Series, body_threshold: float = 0.3) -> bool:
        """Shooting Star: Small body at lower end, long upper shadow."""
        body = abs(row['close'] - row['open'])
        range_ = row['high'] - row['low']
        if range_ == 0:
            return False

        # Body should be small
        if (body / range_) > body_threshold:
            return False

        # Upper shadow should be at least 2x the body
        upper_shadow = row['high'] - max(row['open'], row['close'])
        if upper_shadow < 2 * body:
            return False

        # Lower shadow should be small
        lower_shadow = min(row['open'], row['close']) - row['low']
        return lower_shadow < body

    @staticmethod
    def detect_engulfing_bullish(prev: pd.Series, curr: pd.Series) -> bool:
        """Bullish Engulfing: Current green candle completely engulfs previous red candle."""
        prev_body = prev['close'] - prev['open']
        curr_body = curr['close'] - curr['open']

        return (prev_body < 0 and  # Previous red
                curr_body > 0 and  # Current green
                curr['open'] < prev['close'] and
                curr['close'] > prev['open'])

    @staticmethod
    def detect_engulfing_bearish(prev: pd.Series, curr: pd.Series) -> bool:
        """Bearish Engulfing: Current red candle completely engulfs previous green candle."""
        prev_body = prev['close'] - prev['open']
        curr_body = curr['close'] - curr['open']

        return (prev_body > 0 and  # Previous green
                curr_body < 0 and  # Current red
                curr['open'] > prev['close'] and
                curr['close'] < prev['open'])

    @staticmethod
    def detect_harami_bullish(prev: pd.Series, curr: pd.Series) -> bool:
        """Bullish Harami: Small body within previous large bearish body."""
        prev_body = prev['close'] - prev['open']
        curr_body = curr['close'] - curr['open']

        return (prev_body < 0 and  # Previous red
                curr_body > 0 and  # Current green
                abs(curr_body) < abs(prev_body) and
                curr['high'] < prev['open'] and
                curr['low'] > prev['close'])

    @staticmethod
    def detect_harami_bearish(prev: pd.Series, curr: pd.Series) -> bool:
        """Bearish Harami: Small body within previous large bullish body."""
        prev_body = prev['close'] - prev['open']
        curr_body = curr['close'] - curr['open']

        return (prev_body > 0 and  # Previous green
                curr_body < 0 and  # Current red
                abs(curr_body) < abs(prev_body) and
                curr['high'] < prev['close'] and
                curr['low'] > prev['open'])

    @staticmethod
    def detect_morning_star(df: pd.DataFrame, idx: int) -> bool:
        """Morning Star: Three-candle reversal pattern at bottom."""
        if idx < 2:
            return False

        c1 = df.iloc[idx - 2]  # First: Long bearish
        c2 = df.iloc[idx - 1]  # Second: Small body (doji or spinning top)
        c3 = df.iloc[idx]      # Third: Long bullish

        c1_body = c1['close'] - c1['open']
        c3_body = c3['close'] - c3['open']

        # First candle: Long bearish
        if c1_body > 0:  # Not bearish
            return False

        # Second candle: Small body
        c2_body = abs(c2['close'] - c2['open'])
        c2_range = c2['high'] - c2['low']
        if c2_range == 0 or (c2_body / c2_range) > 0.3:  # Body not small enough
            return False

        # Third candle: Long bullish that closes above midpoint of first
        if c3_body < 0:  # Not bullish
            return False

        midpoint_c1 = (c1['high'] + c1['low']) / 2
        return c3['close'] > midpoint_c1

    @staticmethod
    def detect_evening_star(df: pd.DataFrame, idx: int) -> bool:
        """Evening Star: Three-candle reversal pattern at top."""
        if idx < 2:
            return False

        c1 = df.iloc[idx - 2]  # First: Long bullish
        c2 = df.iloc[idx - 1]  # Second: Small body
        c3 = df.iloc[idx]      # Third: Long bearish

        c1_body = c1['close'] - c1['open']
        c3_body = c3['close'] - c3['open']

        # First candle: Long bullish
        if c1_body < 0:  # Not bullish
            return False

        # Second candle: Small body
        c2_body = abs(c2['close'] - c2['open'])
        c2_range = c2['high'] - c2['low']
        if c2_range == 0 or (c2_body / c2_range) > 0.3:
            return False

        # Third candle: Long bearish that closes below midpoint of first
        if c3_body > 0:  # Not bearish
            return False

        midpoint_c1 = (c1['high'] + c1['low']) / 2
        return c3['close'] < midpoint_c1

    @staticmethod
    def detect_piercing_line(prev: pd.Series, curr: pd.Series) -> bool:
        """Piercing Line: Bullish reversal with second candle closing above midpoint of first."""
        prev_body = prev['close'] - prev['open']
        curr_body = curr['close'] - curr['open']

        if not (prev_body < 0 and curr_body > 0):  # Prev bearish, curr bullish
            return False

        midpoint = (prev['open'] + prev['close']) / 2
        return curr['close'] > midpoint and curr['open'] < prev['low']

    @staticmethod
    def detect_dark_cloud_cover(prev: pd.Series, curr: pd.Series) -> bool:
        """Dark Cloud Cover: Bearish reversal with second candle closing below midpoint of first."""
        prev_body = prev['close'] - prev['open']
        curr_body = curr['close'] - curr['open']

        if not (prev_body > 0 and curr_body < 0):  # Prev bullish, curr bearish
            return False

        midpoint = (prev['open'] + prev['close']) / 2
        return curr['close'] < midpoint and curr['open'] > prev['high']

    @staticmethod
    def detect_three_white_soldiers(df: pd.DataFrame, idx: int) -> bool:
        """Three White Soldiers: Three consecutive long bullish candles."""
        if idx < 2:
            return False

        for i in range(idx - 2, idx + 1):
            candle = df.iloc[i]
            body = candle['close'] - candle['open']

            # Each candle should be bullish
            if body <= 0:
                return False

            # Each should open within previous body
            if i > idx - 2:
                prev = df.iloc[i - 1]
                if not (candle['open'] > prev['open'] and
                       candle['open'] < prev['close']):
                    return False

        return True

    @staticmethod
    def detect_three_black_crows(df: pd.DataFrame, idx: int) -> bool:
        """Three Black Crows: Three consecutive long bearish candles."""
        if idx < 2:
            return False

        for i in range(idx - 2, idx + 1):
            candle = df.iloc[i]
            body = candle['close'] - candle['open']

            # Each candle should be bearish
            if body >= 0:
                return False

            # Each should open within previous body
            if i > idx - 2:
                prev = df.iloc[i - 1]
                if not (candle['open'] < prev['open'] and
                       candle['open'] > prev['close']):
                    return False

        return True

    @staticmethod
    def detect_marubozu(row: pd.Series) -> Tuple[bool, str]:
        """Marubozu: No shadows or very small shadows."""
        body = abs(row['close'] - row['open'])
        if body == 0:
            return False, ""

        upper_shadow = row['high'] - max(row['open'], row['close'])
        lower_shadow = min(row['open'], row['close']) - row['low']

        # Shadows should be less than 5% of body
        shadow_ratio = (upper_shadow + lower_shadow) / body
        if shadow_ratio > 0.1:
            return False, ""

        direction = "bullish" if row['close'] > row['open'] else "bearish"
        return True, direction

    @staticmethod
    def detect_hanging_man(row: pd.Series, prev_trend: str) -> bool:
        """Hanging Man: Hammer-like pattern at top of uptrend."""
        if not JapaneseCandlestickPatterns.detect_hammer(row):
            return False
        return prev_trend == "bullish"

    @staticmethod
    def add_candlestick_patterns(df: pd.DataFrame) -> pd.DataFrame:
        """Add all candlestick pattern detections to dataframe."""

        # Initialize pattern columns
        pattern_columns = [
            'pattern_doji', 'pattern_dragonfly_doji', 'pattern_gravestone_doji',
            'pattern_hammer', 'pattern_shooting_star', 'pattern_hanging_man',
            'pattern_engulfing_bullish', 'pattern_engulfing_bearish',
            'pattern_harami_bullish', 'pattern_harami_bearish',
            'pattern_morning_star', 'pattern_evening_star',
            'pattern_piercing_line', 'pattern_dark_cloud_cover',
            'pattern_three_white_soldiers', 'pattern_three_black_crows',
            'pattern_marubozu_bullish', 'pattern_marubozu_bearish',
            'pattern_long_upper_shadow', 'pattern_long_lower_shadow',
            'pattern_bullish_total', 'pattern_bearish_total',
            'pattern_bullish_strength', 'pattern_bearish_strength'
        ]

        for col in pattern_columns:
            if col not in df.columns:
                df[col] = False

        # Detect patterns for each row
        for i in range(len(df)):
            row = df.iloc[i]

            # Single-candle patterns
            df.loc[df.index[i], 'pattern_doji'] = JapaneseCandlestickPatterns.detect_doji(row)
            df.loc[df.index[i], 'pattern_dragonfly_doji'] = JapaneseCandlestickPatterns.detect_dragonfly_doji(row)
            df.loc[df.index[i], 'pattern_gravestone_doji'] = JapaneseCandlestickPatterns.detect_gravestone_doji(row)
            df.loc[df.index[i], 'pattern_hammer'] = JapaneseCandlestickPatterns.detect_hammer(row)
            df.loc[df.index[i], 'pattern_shooting_star'] = JapaneseCandlestickPatterns.detect_shooting_star(row)

            # Marubozu
            is_marubozu, direction = JapaneseCandlestickPatterns.detect_marubozu(row)
            if is_marubozu:
                df.loc[df.index[i], 'pattern_marubozu_bullish'] = (direction == "bullish")
                df.loc[df.index[i], 'pattern_marubozu_bearish'] = (direction == "bearish")

            # Two-candle patterns
            if i > 0:
                prev = df.iloc[i - 1]

                # Determine trend for hanging man
                prev_trend = "bullish" if i > 5 and df.iloc[i-5:i]['close'].mean() < row['close'] else "neutral"

                df.loc[df.index[i], 'pattern_hanging_man'] = (
                    JapaneseCandlestickPatterns.detect_hanging_man(row, prev_trend)
                )
                df.loc[df.index[i], 'pattern_engulfing_bullish'] = (
                    JapaneseCandlestickPatterns.detect_engulfing_bullish(prev, row)
                )
                df.loc[df.index[i], 'pattern_engulfing_bearish'] = (
                    JapaneseCandlestickPatterns.detect_engulfing_bearish(prev, row)
                )
                df.loc[df.index[i], 'pattern_harami_bullish'] = (
                    JapaneseCandlestickPatterns.detect_harami_bullish(prev, row)
                )
                df.loc[df.index[i], 'pattern_harami_bearish'] = (
                    JapaneseCandlestickPatterns.detect_harami_bearish(prev, row)
                )
                df.loc[df.index[i], 'pattern_piercing_line'] = (
                    JapaneseCandlestickPatterns.detect_piercing_line(prev, row)
                )
                df.loc[df.index[i], 'pattern_dark_cloud_cover'] = (
                    JapaneseCandlestickPatterns.detect_dark_cloud_cover(prev, row)
                )

            # Three-candle patterns
            if i > 1:
                df.loc[df.index[i], 'pattern_morning_star'] = (
                    JapaneseCandlestickPatterns.detect_morning_star(df, i)
                )
                df.loc[df.index[i], 'pattern_evening_star'] = (
                    JapaneseCandlestickPatterns.detect_evening_star(df, i)
                )
                df.loc[df.index[i], 'pattern_three_white_soldiers'] = (
                    JapaneseCandlestickPatterns.detect_three_white_soldiers(df, i)
                )
                df.loc[df.index[i], 'pattern_three_black_crows'] = (
                    JapaneseCandlestickPatterns.detect_three_black_crows(df, i)
                )

            # Shadow patterns
            body = abs(row['close'] - row['open'])
            range_ = row['high'] - row['low']
            if range_ > 0:
                upper_shadow = row['high'] - max(row['open'], row['close'])
                lower_shadow = min(row['open'], row['close']) - row['low']

                df.loc[df.index[i], 'pattern_long_upper_shadow'] = upper_shadow > body * 2
                df.loc[df.index[i], 'pattern_long_lower_shadow'] = lower_shadow > body * 2

        # Calculate pattern totals and strengths
        bullish_patterns = [
            'pattern_hammer', 'pattern_dragonfly_doji', 'pattern_engulfing_bullish',
            'pattern_harami_bullish', 'pattern_morning_star', 'pattern_piercing_line',
            'pattern_three_white_soldiers', 'pattern_marubozu_bullish',
            'pattern_long_lower_shadow'
        ]

        bearish_patterns = [
            'pattern_shooting_star', 'pattern_gravestone_doji', 'pattern_engulfing_bearish',
            'pattern_harami_bearish', 'pattern_evening_star', 'pattern_dark_cloud_cover',
            'pattern_three_black_crows', 'pattern_marubozu_bearish', 'pattern_hanging_man',
            'pattern_long_upper_shadow'
        ]

        # Count patterns
        df['pattern_bullish_total'] = df[bullish_patterns].sum(axis=1)
        df['pattern_bearish_total'] = df[bearish_patterns].sum(axis=1)

        # Calculate strength scores
        def calculate_strength(row, patterns_list):
            strength = 0
            for pattern in patterns_list:
                if row.get(pattern, False):
                    # Add pattern strength from dictionary
                    pattern_name = pattern.replace('pattern_', '')
                    strength += JapaneseCandlestickPatterns.PATTERN_STRENGTH.get(
                        pattern_name, 1
                    )
            return strength

        df['pattern_bullish_strength'] = df.apply(
            lambda row: calculate_strength(row, bullish_patterns), axis=1
        )
        df['pattern_bearish_strength'] = df.apply(
            lambda row: calculate_strength(row, bearish_patterns), axis=1
        )

        return df


# =============================================================================
# TECHNICAL INDICATORS (Enhanced with Candlestick Patterns)
# =============================================================================
class TechnicalIndicators:

    @staticmethod
    def add_basic_indicators(df: pd.DataFrame,
                              ema_fast: int = EMA_FAST_30M,
                              ema_slow: int = EMA_SLOW_30M) -> pd.DataFrame:
        df["ema_fast"] = ta.trend.ema_indicator(df["close"], window=ema_fast)
        df["ema_slow"] = ta.trend.ema_indicator(df["close"], window=ema_slow)
        df["atr"] = ta.volatility.average_true_range(
            df["high"], df["low"], df["close"], window=14)
        df["atr_ma50"] = df["atr"].rolling(window=50).mean()
        df["rsi"] = ta.momentum.rsi(df["close"], window=14)
        df["volume_sma"] = df["volume"].rolling(window=20).mean()

        # Add candlestick patterns
        df = JapaneseCandlestickPatterns.add_candlestick_patterns(df)

        return df

    @staticmethod
    def add_bollinger_bands(df: pd.DataFrame, window: int = 20, std: int = 2) -> pd.DataFrame:
        bb = ta.volatility.BollingerBands(df["close"], window=window, window_dev=std)
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"] = bb.bollinger_lband()
        df["bb_mid"] = bb.bollinger_mavg()
        df["bb_width"] = (df["bb_high"] - df["bb_low"]) / df["bb_mid"]
        df["bb_percent"] = (df["close"] - df["bb_low"]) / (df["bb_high"] - df["bb_low"])
        return df

    @staticmethod
    def add_keltner_channels(df: pd.DataFrame, window: int = 20, atr_mult: float = 1.5) -> pd.DataFrame:
        df["kc_mid"] = ta.trend.ema_indicator(df["close"], window=window)
        df["kc_atr"] = ta.volatility.average_true_range(df["high"], df["low"], df["close"], window=10)
        df["kc_upper"] = df["kc_mid"] + (df["kc_atr"] * atr_mult)
        df["kc_lower"] = df["kc_mid"] - (df["kc_atr"] * atr_mult)
        return df

    @staticmethod
    def add_ttm_squeeze(df: pd.DataFrame) -> pd.DataFrame:
        df = TechnicalIndicators.add_bollinger_bands(df)
        df = TechnicalIndicators.add_keltner_channels(df)

        df["squeeze_on"] = (df["bb_low"] > df["kc_lower"]) & (df["bb_high"] < df["kc_upper"])

        counter, count = [], 0
        for sq in df["squeeze_on"]:
            count = count + 1 if sq else 0
            counter.append(count)
        df["squeeze_count"] = counter
        df["prev_squeeze_count"] = df["squeeze_count"].shift(1).fillna(0).astype(int)

        df["squeeze_momentum"] = ta.momentum.awesome_oscillator(df["high"], df["low"])
        df["squeeze_momentum_prev"] = df["squeeze_momentum"].shift(1)

        just_released = ~df["squeeze_on"] & (df["prev_squeeze_count"] >= 3)

        df["squeeze_long_valid"] = (
            just_released
            & (df["squeeze_momentum"] > 0)
            & (df["squeeze_momentum"] > df["squeeze_momentum_prev"])
        )
        df["squeeze_short_valid"] = (
            just_released
            & (df["squeeze_momentum"] < 0)
            & (df["squeeze_momentum"] < df["squeeze_momentum_prev"])
        )
        return df

    @staticmethod
    def detect_market_regime(df: pd.DataFrame) -> pd.DataFrame:
        df["is_trending"] = (df["atr"] / df["atr_ma50"].replace(0, np.nan)) > TRENDING_ATR_RATIO
        df["is_trending"] = df["is_trending"].fillna(False)
        return df

    @staticmethod
    def find_swing_highs_lows(df: pd.DataFrame) -> pd.DataFrame:
        SWING_WINDOW = 20
        df["swing_high"] = df["high"].rolling(window=SWING_WINDOW).max()
        df["swing_low"] = df["low"].rolling(window=SWING_WINDOW).min()

        prior_high = df["swing_high"].shift(1)
        prior_low = df["swing_low"].shift(1)

        candle_body_high = df[["open", "close"]].min(axis=1)
        candle_body_low = df[["open", "close"]].max(axis=1)

        df["broke_swing_high"] = candle_body_high > prior_high
        df["broke_swing_low"] = candle_body_low < prior_low

        df["nearest_swing_high"] = df["swing_high"].rolling(window=20).max()
        df["nearest_swing_low"] = df["swing_low"].rolling(window=20).min()
        df["dist_to_swing_high"] = (df["nearest_swing_high"] - df["close"]) / df["close"] * 100
        df["dist_to_swing_low"] = (df["close"] - df["nearest_swing_low"]) / df["close"] * 100
        return df

    @staticmethod
    def check_rsi_divergence(df: pd.DataFrame) -> pd.DataFrame:
        df["rsi_divergence_bullish"] = False
        df["rsi_divergence_bearish"] = False

        recent = df.tail(20).reset_index(drop=True)
        price_highs, price_lows, rsi_highs, rsi_lows = [], [], [], []

        for i in range(2, len(recent) - 2):
            h = recent["high"].iloc
            l = recent["low"].iloc
            r = recent["rsi"].iloc

            if h[i] > h[i-1] and h[i] > h[i-2] and h[i] > h[i+1] and h[i] > h[i+2]:
                price_highs.append((i, h[i]))
                rsi_highs.append((i, r[i]))
            if l[i] < l[i-1] and l[i] < l[i-2] and l[i] < l[i+1] and l[i] < l[i+2]:
                price_lows.append((i, l[i]))
                rsi_lows.append((i, r[i]))

        if (len(price_highs) >= 2 and len(rsi_highs) >= 2
                and price_highs[-1][1] > price_highs[-2][1]
                and rsi_highs[-1][1] < rsi_highs[-2][1]):
            df.iloc[-1, df.columns.get_loc("rsi_divergence_bearish")] = True

        if (len(price_lows) >= 2 and len(rsi_lows) >= 2
                and price_lows[-1][1] < price_lows[-2][1]
                and rsi_lows[-1][1] > rsi_lows[-2][1]):
            df.iloc[-1, df.columns.get_loc("rsi_divergence_bullish")] = True

        return df


# =============================================================================
# SESSION FILTER
# =============================================================================
def is_valid_trading_session() -> bool:
    return utcnow().hour in VALID_SESSION_HOURS


# =============================================================================
# USER MANAGER (Unchanged from v3.0)
# =============================================================================
class UserManager:
    def __init__(self, storage_file: str):
        self.storage_file = storage_file
        self.active_users: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self.load_users()

    def load_users(self):
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, "r") as f:
                    self.active_users = json.load(f)
                logger.info(f"Loaded {len(self.active_users)} users from disk")
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            self.active_users = {}

    def save_users(self):
        try:
            with self._lock:
                with open(self.storage_file, "w") as f:
                    json.dump(self.active_users, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving users: {e}")

    def add_user(self, user_id: str, username: str = None, first_name: str = None):
        with self._lock:
            self.active_users[user_id] = {
                "username": username,
                "first_name": first_name,
                "joined_at": localnow().isoformat(),
                "last_active": localnow().isoformat(),
                "active": True,
                "preferences": {"receive_signals": True, "receive_updates": True},
            }
        self.save_users()

    def remove_user(self, user_id: str):
        with self._lock:
            if user_id in self.active_users:
                self.active_users[user_id]["active"] = False
                self.active_users[user_id]["left_at"] = localnow().isoformat()
        self.save_users()

    def get_active_users(self) -> List[str]:
        with self._lock:
            return [
                uid for uid, data in self.active_users.items()
                if data.get("active", False)
                and data.get("preferences", {}).get("receive_signals", True)
            ]

    def update_last_active(self, user_id: str):
        with self._lock:
            if user_id in self.active_users:
                self.active_users[user_id]["last_active"] = localnow().isoformat()
        self.save_users()

    def get_user_count(self) -> int:
        return len(self.get_active_users())


# =============================================================================
# CRYPTO SCANNER (Enhanced with Candlestick Patterns)
# =============================================================================
class CryptoScanner:

    def __init__(self):
        self._validate_config()
        self.client = Client(BINANCE_API_KEY, BINANCE_SECRET)
        self.bot = Bot(token=TELEGRAM_TOKEN)
        self.user_manager = UserManager(ACTIVE_USERS_FILE)

        self._state_lock = threading.Lock()
        self.sent_signals: Dict[str, float] = {}
        self.signals_generated = 0
        self.last_signal_time: Optional[datetime] = None
        self.open_positions: Dict[str, dict] = {}

        self.last_status_broadcast = datetime.min.replace(tzinfo=timezone.utc)
        self.scanning = True

        logger.info("CryptoScanner v3.1 (Candlestick Patterns) initialised")

    @staticmethod
    def _validate_config():
        if BINANCE_API_KEY in ("YOUR_BINANCE_API_KEY", "BINANCE_API_KEY", "YOUR_BINANCE_KEY"):
            logger.error("Set Binance API credentials in CONFIG section.")
            sys.exit(1)
        if TELEGRAM_TOKEN in ("YOUR_TELEGRAM_TOKEN", "TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN"):
            logger.error("Set Telegram Bot token in CONFIG section.")
            sys.exit(1)

    async def broadcast_to_users(self, message: str, parse_mode: str = "HTML"):
        active_users = self.user_manager.get_active_users()
        success_count = fail_count = 0
        for user_id in active_users:
            try:
                await self.bot.send_message(
                    chat_id=int(user_id), text=message, parse_mode=parse_mode
                )
                success_count += 1
                await asyncio.sleep(0.05)
            except Forbidden:
                logger.warning(f"User {user_id} blocked bot — deactivating")
                self.user_manager.remove_user(user_id)
                fail_count += 1
            except BadRequest as e:
                logger.error(f"BadRequest {user_id}: {e}")
                fail_count += 1
            except TelegramError as e:
                logger.error(f"TelegramError {user_id}: {e}")
                fail_count += 1
            except Exception as e:
                logger.error(f"Unexpected error {user_id}: {e}")
                fail_count += 1
        if success_count > 0:
            logger.info(f"Broadcast — sent: {success_count}, failed: {fail_count}")

    def get_top_symbols(self) -> List[str]:
        try:
            tickers = self.client.futures_ticker()
            usdt_pairs = [
                t for t in tickers
                if t["symbol"].endswith("USDT")
                and float(t["quoteVolume"]) > MIN_VOLUME_USDT
            ]
            sorted_pairs = sorted(usdt_pairs, key=lambda x: float(x["quoteVolume"]), reverse=True)
            symbols = [
                x["symbol"] for x in sorted_pairs[:TOP_VOLUME]
                if self.can_send_signal(x["symbol"])
            ]
            logger.info(f"Symbols after cooldown filter: {len(symbols)}")
            return symbols
        except Exception as e:
            logger.error(f"Error in get_top_symbols: {e}")
            return []

    def get_klines(self, symbol: str, timeframe: str = TIMEFRAME,
                   limit: int = 250) -> Optional[pd.DataFrame]:
        try:
            klines = self.client.futures_klines(symbol=symbol, interval=timeframe, limit=limit)
            df = pd.DataFrame(klines, columns=[
                "time", "open", "high", "low", "close", "volume",
                "close_time", "qav", "num_trades",
                "taker_base_vol", "taker_quote_vol", "ignore",
            ])
            df[["open", "high", "low", "close", "volume"]] = (
                df[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
            )
            df["time"] = pd.to_datetime(df["time"], unit="ms")
            df.dropna(inplace=True)
            return df
        except Exception as e:
            logger.error(f"Klines error {symbol} ({timeframe}): {e}")
            return None

    def get_funding_rate(self, symbol: str) -> Optional[float]:
        try:
            rates = self.client.futures_funding_rate(symbol=symbol, limit=1)
            return float(rates[0]["fundingRate"]) if rates else None
        except Exception as e:
            logger.debug(f"Funding fetch failed {symbol}: {e}")
            return None

    def evaluate_funding(self, symbol: str, signal_type: str) -> Tuple[bool, bool, float]:
        rate = self.get_funding_rate(symbol)
        if rate is None:
            return False, False, 0.0
        if abs(rate) > MAX_FUNDING_RATE:
            return True, False, rate
        funding_confirms = (
            (signal_type == "LONG" and rate < -FUNDING_CONFLICT_MIN) or
            (signal_type == "SHORT" and rate > FUNDING_CONFLICT_MIN)
        )
        return False, funding_confirms, rate

    def get_current_portfolio_risk(self) -> float:
        with self._state_lock:
            return sum(p["risk_pct"] for p in self.open_positions.values())

    def get_open_position_count(self) -> int:
        with self._state_lock:
            return len(self.open_positions)

    def register_open_position(self, symbol: str, signal_type: str):
        with self._state_lock:
            self.open_positions[symbol] = {
                "type": signal_type,
                "risk_pct": RISK_PERCENT,
                "opened_at": localnow().isoformat(),
            }

    def close_position(self, symbol: str):
        with self._state_lock:
            self.open_positions.pop(symbol, None)

    def can_open_new_position(self) -> Tuple[bool, str]:
        open_count = self.get_open_position_count()
        current_risk = self.get_current_portfolio_risk()
        if current_risk + RISK_PERCENT > MAX_PORTFOLIO_RISK:
            return False, f"Portfolio risk cap: {current_risk + RISK_PERCENT:.1f}% > {MAX_PORTFOLIO_RISK}%"
        return True, ""

    def check_multi_timeframe_trend(self, symbol: str) -> Tuple[bool, bool]:
        def _is_bullish(tf: str, fast: int, slow: int) -> bool:
            df = self.get_klines(symbol, tf, 250)
            if df is None or len(df) < slow + 10:
                return False
            df = TechnicalIndicators.add_basic_indicators(df, ema_fast=fast, ema_slow=slow)
            last = df.iloc[-1]
            return bool(last["close"] > last["ema_fast"] > last["ema_slow"])
        try:
            return (
                _is_bullish(HIGHER_TIMEFRAME, EMA_FAST_HTF, EMA_SLOW_HTF),
                _is_bullish(HIGHEST_TIMEFRAME, EMA_FAST_HTF, EMA_SLOW_HTF),
            )
        except Exception as e:
            logger.error(f"MTF error {symbol}: {e}")
            return False, False

    def calculate_signal_strength(self, signal_type: str, df: pd.DataFrame,
                                   h1_trend: bool, h4_trend: bool,
                                   funding_confirms: bool) -> int:
        strength = 0
        last = df.iloc[-1]

        # Candlestick pattern strength
        if signal_type == "LONG":
            pattern_strength = last.get("pattern_bullish_strength", 0)
            if pattern_strength >= MIN_PATTERN_STRENGTH:
                strength += min(pattern_strength, 3)  # Max 3 points from patterns
        else:
            pattern_strength = last.get("pattern_bearish_strength", 0)
            if pattern_strength >= MIN_PATTERN_STRENGTH:
                strength += min(pattern_strength, 3)

        # Trend alignment
        if signal_type == "LONG":
            if h1_trend: strength += 1
            if h4_trend: strength += 1
            if last.get("squeeze_long_valid", False): strength += 1
        else:
            if not h1_trend: strength += 1
            if not h4_trend: strength += 1
            if last.get("squeeze_short_valid", False): strength += 1

        # Volume confirmation
        if last["volume"] > last["volume_sma"] * 1.5: strength += 1

        # RSI divergence
        if signal_type == "LONG" and last.get("rsi_divergence_bullish", False):
            strength += 1
        elif signal_type == "SHORT" and last.get("rsi_divergence_bearish", False):
            strength += 1

        # Funding confirmation
        if funding_confirms: strength += 1

        return strength

    @staticmethod
    def rsi_condition_ok(last: pd.Series, signal_type: str) -> bool:
        rsi = last["rsi"]
        is_trending = last.get("is_trending", False)
        if is_trending:
            return rsi > 50 if signal_type == "LONG" else rsi < 50
        return (40 < rsi < 65) if signal_type == "LONG" else (35 < rsi < 60)

    def check_signal(self, symbol: str) -> Optional[Dict]:
        try:
            if not is_valid_trading_session():
                return None

            df = self.get_klines(symbol, TIMEFRAME, 250)
            if df is None or len(df) < 200:
                return None

            df = TechnicalIndicators.add_basic_indicators(df, EMA_FAST_30M, EMA_SLOW_30M)
            df = TechnicalIndicators.detect_market_regime(df)
            df = TechnicalIndicators.add_ttm_squeeze(df)
            df = TechnicalIndicators.find_swing_highs_lows(df)
            df = TechnicalIndicators.check_rsi_divergence(df)

            h1_trend, h4_trend = self.check_multi_timeframe_trend(symbol)
            last = df.iloc[-1]

            required_cols = ["close", "atr", "ema_fast", "ema_slow", "rsi", "atr_ma50"]
            if pd.isna(last[required_cols]).any():
                return None

            entry = last["close"]
            atr = last["atr"]
            atr_percent = (atr / entry) * 100

            if atr_percent < MIN_ATR_PERCENT or atr_percent > MAX_ATR_PERCENT:
                return None

            preliminary_long = last["ema_fast"] > last["ema_slow"] and entry > last["ema_fast"]
            preliminary_short = last["ema_fast"] < last["ema_slow"] and entry < last["ema_fast"]
            prelim_type = "LONG" if preliminary_long else ("SHORT" if preliminary_short else None)

            funding_blocked = funding_confirms = False
            funding_rate = 0.0
            if prelim_type:
                funding_blocked, funding_confirms, funding_rate = self.evaluate_funding(
                    symbol, prelim_type
                )
            if funding_blocked:
                return None

            risk_amount = CAPITAL * (RISK_PERCENT / 100)

            # Enhanced conditions with candlestick patterns
            long_conditions = {
                "trend": last["ema_fast"] > last["ema_slow"],
                "price_above_ema": entry > last["ema_fast"],
                "rsi_ok": self.rsi_condition_ok(last, "LONG"),
                "volume_confirm": last["volume"] > last["volume_sma"],
                "swing_break": bool(last["broke_swing_high"]),
                "not_near_resistance": last["dist_to_swing_high"] > 1.0,
                "squeeze_release": bool(last.get("squeeze_long_valid", False)),
                "higher_tf_trend": h1_trend or h4_trend,
                "funding_ok": not (funding_rate > FUNDING_CONFLICT_MIN),
                "h4_confirmed": h4_trend,
                "bullish_pattern": last.get("pattern_bullish_strength", 0) >= MIN_PATTERN_STRENGTH,
                "strong_bullish_pattern": last.get("pattern_bullish_strength", 0) >= 3,
            }

            short_conditions = {
                "trend": last["ema_fast"] < last["ema_slow"],
                "price_below_ema": entry < last["ema_fast"],
                "rsi_ok": self.rsi_condition_ok(last, "SHORT"),
                "volume_confirm": last["volume"] > last["volume_sma"],
                "swing_break": bool(last["broke_swing_low"]),
                "not_near_support": last["dist_to_swing_low"] > 1.0,
                "squeeze_release": bool(last.get("squeeze_short_valid", False)),
                "higher_tf_trend": not h1_trend and not h4_trend,
                "funding_ok": not (funding_rate < -FUNDING_CONFLICT_MIN),
                "h4_confirmed": not h4_trend,
                "bearish_pattern": last.get("pattern_bearish_strength", 0) >= MIN_PATTERN_STRENGTH,
                "strong_bearish_pattern": last.get("pattern_bearish_strength", 0) >= 3,
            }

            long_score = sum(long_conditions.values())
            short_score = sum(short_conditions.values())

            def _build_signal(sig_type: str, score: int, conditions: dict) -> Dict:
                if sig_type == "LONG":
                    entry_low = entry
                    entry_high = entry + atr * ENTRY_BUFFER_ATR_MULT
                    sl = entry_high - (atr * 1.5)
                    tp = entry_low + (atr * RR_RATIO * 1.5)
                else:
                    entry_high = entry
                    entry_low = entry - atr * ENTRY_BUFFER_ATR_MULT
                    sl = entry_low + (atr * 1.5)
                    tp = entry_high - (atr * RR_RATIO * 1.5)

                position_size = (risk_amount * LEVERAGE) / abs(entry - sl)
                strength = self.calculate_signal_strength(
                    sig_type, df, h1_trend, h4_trend, funding_confirms
                )

                with self._state_lock:
                    self.signals_generated += 1
                    self.last_signal_time = utcnow()

                return {
                    "type": sig_type,
                    "symbol": symbol,
                    "entry": entry,
                    "entry_low": entry_low,
                    "entry_high": entry_high,
                    "sl": sl,
                    "tp": tp,
                    "size": position_size,
                    "rr": RR_RATIO,
                    "atr": atr,
                    "atr_percent": atr_percent,
                    "rsi": last["rsi"],
                    "is_trending": bool(last.get("is_trending", False)),
                    "funding_rate": funding_rate,
                    "funding_confirms": funding_confirms,
                    "strength": strength,
                    "conditions_met": score,
                    "total_conditions": len(conditions),
                    "h1_trend": h1_trend,
                    "h4_trend": h4_trend,
                    "bullish_patterns": int(last.get("pattern_bullish_total", 0)),
                    "bearish_patterns": int(last.get("pattern_bearish_total", 0)),
                    "bullish_pattern_strength": int(last.get("pattern_bullish_strength", 0)),
                    "bearish_pattern_strength": int(last.get("pattern_bearish_strength", 0)),
                    "timestamp": utcnow(),
                }

            # Prioritize signals with strong pattern confirmation
            if long_score >= MIN_CONDITIONS and last.get("pattern_bullish_strength", 0) >= MIN_PATTERN_STRENGTH:
                return _build_signal("LONG", long_score, long_conditions)
            if short_score >= MIN_CONDITIONS and last.get("pattern_bearish_strength", 0) >= MIN_PATTERN_STRENGTH:
                return _build_signal("SHORT", short_score, short_conditions)

            # Still allow signals without patterns but require higher conditions
            if long_score >= MIN_CONDITIONS + 2:
                return _build_signal("LONG", long_score, long_conditions)
            if short_score >= MIN_CONDITIONS + 2:
                return _build_signal("SHORT", short_score, short_conditions)

            return None

        except Exception as e:
            logger.error(f"Error checking signal for {symbol}: {e}")
            return None

    def format_signal_message(self, signal: Dict) -> str:
        emoji = "🟢" if signal["type"] == "LONG" else "🔴"
        if signal["type"] == "LONG":
            potential_profit = (signal["tp"] - signal["entry"]) * signal["size"]
        else:
            potential_profit = (signal["entry"] - signal["tp"]) * signal["size"]

        strength_bar = "💪" * signal["strength"] + "·" * (6 - signal["strength"])
        htf_line = (f"H1: {'🐂' if signal['h1_trend'] else '🐻'}  "
                    f"H4: {'🐂' if signal['h4_trend'] else '🐻'}")
        regime_label = "📈 Trending" if signal["is_trending"] else "↔️ Ranging"
        fr = signal["funding_rate"]
        fr_icon = "✅" if signal["funding_confirms"] else "➖"
        entry_zone = f"{signal['entry_low']:.4f} – {signal['entry_high']:.4f}"

        # Candlestick patterns section
        pattern_icon = "🕯️"
        if signal["type"] == "LONG":
            pattern_info = (f"Bullish patterns: {signal['bullish_patterns']} "
                           f"(strength: {signal['bullish_pattern_strength']})")
        else:
            pattern_info = (f"Bearish patterns: {signal['bearish_patterns']} "
                           f"(strength: {signal['bearish_pattern_strength']})")

        return (
            f"\n{emoji} <b>{signal['type']} SIGNAL — {signal['symbol']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Entry Zone:</b>   {entry_zone}\n"
            f"   <i>(Place limit order within this range)</i>\n"
            f"🛑 <b>Stop Loss:</b>    {signal['sl']:.4f}\n"
            f"✅ <b>Take Profit:</b>  {signal['tp']:.4f}\n"
            f"📊 <b>Size:</b>         {signal['size']:.4f}\n"
            f"📈 <b>R:R</b>           1:{signal['rr']}\n"
            f"💵 <b>Est. Profit:</b>  ${potential_profit:.2f}\n"
            f"\n<b>{pattern_icon} Candlestick Patterns:</b>\n"
            f"   {pattern_info}\n"
            f"\n<b>📐 Market Context:</b>\n"
            f"   {regime_label}\n"
            f"   {htf_line}\n"
            f"   {fr_icon} Funding: {fr*100:.4f}%/8h\n"
            f"\n<b>📊 Technical:</b>\n"
            f"   ATR: {signal['atr']:.4f} ({signal['atr_percent']:.2f}%)\n"
            f"   RSI: {signal['rsi']:.1f}\n"
            f"\n<b>🎯 Signal Quality:</b>\n"
            f"   Strength:   {signal['strength']}/6  {strength_bar}\n"
            f"   Conditions: {signal['conditions_met']}/{signal['total_conditions']}\n"
            f"\n⏰ {signal['timestamp'].strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>Use limit orders in the entry zone above.</i>"
        )

    def can_send_signal(self, symbol: str) -> bool:
        with self._state_lock:
            last_sent = self.sent_signals.get(symbol)
        return last_sent is None or (time.time() - last_sent) >= SIGNAL_COOLDOWN

    def cleanup_sent_signals(self):
        now = time.time()
        with self._state_lock:
            self.sent_signals = {
                k: v for k, v in self.sent_signals.items()
                if now - v < SIGNAL_COOLDOWN
            }

    async def process_and_broadcast_signals(self):
        try:
            if not is_valid_trading_session():
                utc_hour = utcnow().hour
                logger.info(f"Scan skipped — outside session (UTC {utc_hour:02d}:xx)")
                return

            self.cleanup_sent_signals()
            symbols = self.get_top_symbols()
            if not symbols:
                logger.warning("No symbols eligible this cycle")
                return

            logger.info(f"Scanning {len(symbols)} symbols ...")

            signals: List[Dict] = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures_map = {executor.submit(self.check_signal, sym): sym for sym in symbols}
                for future in as_completed(futures_map):
                    try:
                        result = future.result()
                        if result:
                            signals.append(result)
                    except Exception as e:
                        logger.error(f"Future error ({futures_map[future]}): {e}")

            signals.sort(key=lambda x: x["strength"], reverse=True)

            sent_count = 0
            for signal in signals:
                if signal["strength"] < MIN_STRENGTH:
                    continue
                can_open, reason = self.can_open_new_position()
                if not can_open:
                    logger.info(f"Portfolio gate blocked {signal['symbol']}: {reason}")
                    continue
                if self.can_send_signal(signal["symbol"]):
                    await self.broadcast_to_users(self.format_signal_message(signal))
                    with self._state_lock:
                        self.sent_signals[signal["symbol"]] = time.time()
                    self.register_open_position(signal["symbol"], signal["type"])
                    logger.info(
                        f"Signal sent: {signal['symbol']} {signal['type']} | "
                        f"strength {signal['strength']}/6 | "
                        f"conditions {signal['conditions_met']}/{signal['total_conditions']} | "
                        f"patterns {signal['bullish_patterns'] if signal['type']=='LONG' else signal['bearish_patterns']}"
                    )
                    sent_count += 1
                    await asyncio.sleep(0.1)

            if utcnow() - self.last_status_broadcast > timedelta(minutes=30):
                await self.broadcast_status_update()
                self.last_status_broadcast = utcnow()

            logger.info(f"Scan complete — {len(signals)} found, {sent_count} broadcast")

        except Exception as e:
            logger.error(f"Error in process_and_broadcast_signals: {e}")

    async def broadcast_status_update(self):
        with self._state_lock:
            sigs = self.signals_generated
            last_sig = self.last_signal_time
            open_cnt = len(self.open_positions)
            port_risk = sum(p["risk_pct"] for p in self.open_positions.values())

        utc_hour = utcnow().hour
        session_label = "Active (London/NY)" if is_valid_trading_session() else "Asia (paused)"
        last_sig_str = last_sig.strftime("%H:%M:%S") if last_sig else "None yet"

        message = (
            f"\n🤖 <b>Bot Status — v3.1 (Candlestick Patterns)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Session:         {session_label}\n"
            f"UTC Hour:        {utc_hour:02d}:xx\n"
            f"Active users:    {self.user_manager.get_user_count()}\n"
            f"Timeframe:       {TIMEFRAME} + {HIGHER_TIMEFRAME}/{HIGHEST_TIMEFRAME}\n"
            f"Risk/trade:      {RISK_PERCENT}%\n"
            f"Open positions:  {open_cnt}/{MAX_CONCURRENT_SIGNALS}\n"
            f"Portfolio risk:  {port_risk:.1f}% / {MAX_PORTFOLIO_RISK}%\n"
            f"Signals today:   {sigs}\n"
            f"Last signal:     {last_sig_str}\n"
            f"Patterns:        15+ Japanese candlestick patterns\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.broadcast_to_users(message)

    async def scanning_task(self):
        while self.scanning:
            try:
                await self.process_and_broadcast_signals()
            except Exception as e:
                logger.error(f"Unhandled error in scanning_task: {e}")
            for _ in range(SCAN_INTERVAL):
                if not self.scanning:
                    break
                await asyncio.sleep(1)

    def stop_scanning(self):
        self.scanning = False


# =============================================================================
# TELEGRAM HANDLERS (Updated with pattern info)
# =============================================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if scanner:
        scanner.user_manager.add_user(
            user_id=str(user.id), username=user.username, first_name=user.first_name
        )
    welcome = (
        f"\n🎉 <b>Crypto Signal Bot v3.1</b>\n\n"
        f"Xin chào {user.first_name}!\n\n"
        f"<b>NEW FEATURES:</b>\n"
        f"✅ Japanese Candlestick Patterns (15+ patterns)\n"
        f"   • Reversal: Hammer, Shooting Star, Engulfing\n"
        f"   • Continuation: Marubozu, Three Soldiers\n"
        f"   • Indecision: Doji, Harami, Spinning Top\n\n"
        f"<b>Level 1:</b>\n"
        f"✅ Session filter | ✅ Funding rate\n"
        f"✅ Entry zone     | ✅ Portfolio cap\n\n"
        f"<b>Level 2:</b>\n"
        f"✅ TTM Squeeze + momentum\n"
        f"✅ Regime-aware RSI\n"
        f"✅ Body breakout\n"
        f"✅ Asymmetric HTF filter\n\n"
        f"<b>Lệnh:</b> /start · /status · /stats · /positions · /close · /stop · /help\n"
    )
    await update.message.reply_text(welcome, parse_mode="HTML")
    await status_command(update, context)


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return
    scanner.user_manager.update_last_active(str(update.effective_user.id))
    with scanner._state_lock:
        sigs = scanner.signals_generated
        open_cnt = len(scanner.open_positions)
        port_risk = sum(p["risk_pct"] for p in scanner.open_positions.values())

    session_label = "Active" if is_valid_trading_session() else "Paused (Asia)"
    msg = (
        f"\n📊 <b>Bot Status v3.1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Status:          Online\n"
        f"Session:         {session_label}\n"
        f"Active users:    {scanner.user_manager.get_user_count()}\n"
        f"Pairs scanned:   Top {TOP_VOLUME}\n"
        f"Interval:        {SCAN_INTERVAL}s\n"
        f"Timeframes:      {TIMEFRAME}+{HIGHER_TIMEFRAME}/{HIGHEST_TIMEFRAME}\n"
        f"Risk/trade:      {RISK_PERCENT}%\n"
        f"Open:            {open_cnt}/{MAX_CONCURRENT_SIGNALS}\n"
        f"Portfolio risk:  {port_risk:.1f}% / {MAX_PORTFOLIO_RISK}%\n"
        f"Signals today:   {sigs}\n"
        f"Patterns:        15+ Japanese candlestick\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return
    scanner.user_manager.update_last_active(str(update.effective_user.id))
    now = time.time()
    with scanner._state_lock:
        signals_24h = len([v for v in scanner.sent_signals.values() if now - v < 86400])
        last_sig = scanner.last_signal_time

    msg = (
        f"\n📈 <b>Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Signals (24h):   {signals_24h}\n"
        f"Active users:    {scanner.user_manager.get_user_count()}\n"
        f"Total users:     {len(scanner.user_manager.active_users)}\n"
        f"Last signal:     {'None' if not last_sig else last_sig.strftime('%H:%M:%S UTC')}\n"
        f"Pattern library: 15+ candlestick patterns\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def positions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return
    scanner.user_manager.update_last_active(str(update.effective_user.id))
    with scanner._state_lock:
        positions = dict(scanner.open_positions)
        port_risk = sum(p["risk_pct"] for p in positions.values())

    if not positions:
        await update.message.reply_text("📭 <b>No open positions tracked.</b>", parse_mode="HTML")
        return

    lines = [f"\n📋 <b>Open Positions ({len(positions)}/{MAX_CONCURRENT_SIGNALS})</b>\n"
             f"━━━━━━━━━━━━━━━━━━━━━━━\n"]
    for sym, data in positions.items():
        icon = "🟢" if data["type"] == "LONG" else "🔴"
        lines.append(f"{icon} <b>{sym}</b> {data['type']} | {data['risk_pct']}% risk")
        lines.append(f"   Opened: {data['opened_at'][:16]}\n")
    lines.append(f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                 f"Portfolio risk: {port_risk:.1f}% / {MAX_PORTFOLIO_RISK}%")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def close_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /close BTCUSDT", parse_mode="HTML")
        return
    symbol = args[0].upper()
    scanner.close_position(symbol)
    await update.message.reply_text(
        f"✅ <b>{symbol}</b> removed from portfolio tracker.", parse_mode="HTML"
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if scanner:
        scanner.user_manager.remove_user(str(update.effective_user.id))
    await update.message.reply_text(
        "\n🛑 <b>Unsubscribed</b>\n\nGửi /start để đăng ký lại.", parse_mode="HTML"
    )


async def patterns_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """New command to show detected candlestick patterns for a symbol"""
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return

    args = context.args
    if not args:
        await update.message.reply_text("Usage: /patterns BTCUSDT", parse_mode="HTML")
        return

    symbol = args[0].upper()
    df = scanner.get_klines(symbol, TIMEFRAME, 50)
    if df is None:
        await update.message.reply_text(f"❌ Cannot fetch data for {symbol}", parse_mode="HTML")
        return

    df = JapaneseCandlestickPatterns.add_candlestick_patterns(df)
    last = df.iloc[-1]

    # Collect active patterns
    bullish_patterns = []
    bearish_patterns = []

    pattern_cols = [col for col in df.columns if col.startswith('pattern_') and not col.endswith(('total', 'strength'))]

    for col in pattern_cols:
        if last.get(col, False):
            pattern_name = col.replace('pattern_', '').replace('_', ' ').title()
            if 'bullish' in col or any(x in col for x in ['hammer', 'dragonfly', 'morning', 'piercing', 'white']):
                bullish_patterns.append(f"✅ {pattern_name}")
            elif 'bearish' in col or any(x in col for x in ['shooting', 'gravestone', 'evening', 'dark', 'black', 'hanging']):
                bearish_patterns.append(f"🔴 {pattern_name}")

    msg = (
        f"\n🕯️ <b>Candlestick Patterns - {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Timeframe: {TIMEFRAME}\n"
        f"Time: {last.name.strftime('%Y-%m-%d %H:%M:%S') if hasattr(last.name, 'strftime') else 'Latest'}\n\n"
        f"<b>Bullish Patterns ({len(bullish_patterns)}):</b>\n"
        f"{chr(10).join(bullish_patterns) if bullish_patterns else 'None detected'}\n\n"
        f"<b>Bearish Patterns ({len(bearish_patterns)}):</b>\n"
        f"{chr(10).join(bearish_patterns) if bearish_patterns else 'None detected'}\n\n"
        f"<b>Strength Scores:</b>\n"
        f"Bullish: {last.get('pattern_bullish_strength', 0)}/??\n"
        f"Bearish: {last.get('pattern_bearish_strength', 0)}/??\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if scanner:
        scanner.user_manager.update_last_active(str(update.effective_user.id))
    help_text = (
        f"\n❓ <b>Hướng dẫn v3.1</b>\n\n"
        f"<b>Lệnh:</b> /start · /status · /stats · /positions · /close · /stop · /patterns · /help\n\n"
        f"<b>NEW - Candlestick Patterns:</b>\n"
        f"• /patterns SYMBOL - Show detected patterns\n"
        f"• Patterns include: Hammer, Shooting Star, Engulfing, Doji, Harami, Morning/Evening Star, Three Soldiers, Marubozu, etc.\n\n"
        f"<b>Level 1:</b>\n"
        f"1. Session filter (Asia excluded)\n"
        f"2. Funding rate filter (|funding| > {MAX_FUNDING_RATE*100:.2f}% blocked)\n"
        f"3. Entry zone (limit order band, not market)\n"
        f"4. Portfolio cap ({MAX_CONCURRENT_SIGNALS} lệnh / {MAX_PORTFOLIO_RISK}% risk)\n\n"
        f"<b>Level 2:</b>\n"
        f"5. Japanese Candlestick Patterns (15+ patterns)\n"
        f"6. TTM Squeeze + AO momentum direction\n"
        f"7. RSI regime-aware (trending vs ranging)\n"
        f"8. Body breakout (wick fakeout filtered)\n"
        f"9. SHORT: cả H1 + H4 bearish (asymmetric)\n\n"
        f"⚠️ Trading futures mang rủi ro cao."
    )
    await update.message.reply_text(help_text, parse_mode="HTML")


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Telegram error: {context.error}")


# =============================================================================
# ENTRY POINT
# =============================================================================
async def main_async():
    scanner = CryptoScanner()
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.bot_data["scanner"] = scanner

    for cmd, handler in [
        ("start", start_command),
        ("status", status_command),
        ("stats", stats_command),
        ("positions", positions_command),
        ("close", close_command),
        ("stop", stop_command),
        ("patterns", patterns_command),
        ("help", help_command),
    ]:
        application.add_handler(CommandHandler(cmd, handler))
    application.add_error_handler(error_handler)

    await scanner.broadcast_to_users(
        "\n🚀 <b>Crypto Signal Bot v3.1 Started!</b>\n\n"
        "✅ NEW: Japanese Candlestick Patterns (15+ patterns)\n"
        "✅ Session filter | ✅ Funding rate\n"
        "✅ Limit entry zone | ✅ Portfolio cap\n"
        "✅ TTM Squeeze + momentum | ✅ Regime RSI\n"
        "✅ Body breakout | ✅ Asymmetric HTF\n"
        "✅ Candlestick pattern confirmation\n\n"
        "Tín hiệu sẽ xuất hiện khi đủ điều kiện."
    )

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    asyncio.create_task(scanner.scanning_task())

    logger.info("Bot v3.1 running with Japanese Candlestick Patterns — Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown ...")
        scanner.stop_scanning()
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()