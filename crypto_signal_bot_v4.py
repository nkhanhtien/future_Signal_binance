"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          Advanced Crypto Futures Signal Bot — v4.1  (15m Scalping)         ║
║                         with Japanese Candlestick Patterns                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  NEW FEATURES:                                                              ║
║  [4.1-1] Japanese Candlestick Patterns (15+ patterns)                       ║
║  [4.1-2] Pattern strength scoring system                                    ║
║  [4.1-3] Pattern-based signal confirmation                                  ║
║  [4.1-4] New command: /patterns to view detected patterns                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
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

# Convenience alias — use everywhere instead of datetime.utcnow()
def utcnow() -> datetime:
    """Return current UTC time as a timezone-aware datetime (replaces deprecated utcnow())."""
    return datetime.now(timezone.utc)

def localnow() -> datetime:
    """Return current local time (for user-facing timestamps stored in JSON)."""
    return datetime.now()
from binance.client import Client
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError, Forbidden, BadRequest
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Tuple
import sys
import asyncio

# =============================================================================
# JAPANESE CANDLESTICK PATTERNS - NEW FOR v4.1
# =============================================================================
class JapaneseCandlestickPatterns:
    """
    Comprehensive Japanese candlestick pattern detection for 15m scalping.
    Optimized for shorter timeframes with pattern strength scoring.
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
    def detect_spinning_top(row: pd.Series, body_threshold: float = 0.3) -> bool:
        """Spinning Top: Small body with shadows on both ends."""
        body = abs(row['close'] - row['open'])
        range_ = row['high'] - row['low']
        if range_ == 0:
            return False

        # Body should be small
        if (body / range_) > body_threshold:
            return False

        # Both shadows should be present
        upper_shadow = row['high'] - max(row['open'], row['close'])
        lower_shadow = min(row['open'], row['close']) - row['low']

        return upper_shadow > 0 and lower_shadow > 0

    @staticmethod
    def detect_long_lower_shadow(row: pd.Series, shadow_mult: float = 2.0) -> bool:
        """Long Lower Shadow: Shadow at least 2x body length."""
        body = abs(row['close'] - row['open'])
        if body == 0:
            return False

        lower_shadow = min(row['open'], row['close']) - row['low']
        return lower_shadow > shadow_mult * body

    @staticmethod
    def detect_long_upper_shadow(row: pd.Series, shadow_mult: float = 2.0) -> bool:
        """Long Upper Shadow: Shadow at least 2x body length."""
        body = abs(row['close'] - row['open'])
        if body == 0:
            return False

        upper_shadow = row['high'] - max(row['open'], row['close'])
        return upper_shadow > shadow_mult * body

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
            'pattern_spinning_top', 'pattern_long_upper_shadow', 'pattern_long_lower_shadow',
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
            df.loc[df.index[i], 'pattern_spinning_top'] = JapaneseCandlestickPatterns.detect_spinning_top(row)
            df.loc[df.index[i], 'pattern_long_lower_shadow'] = JapaneseCandlestickPatterns.detect_long_lower_shadow(row)
            df.loc[df.index[i], 'pattern_long_upper_shadow'] = JapaneseCandlestickPatterns.detect_long_upper_shadow(row)

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
# CONFIGURATION
# =============================================================================
BINANCE_API_KEY = "YOUR_BINANCE_KEY"
BINANCE_SECRET = "YOUR_BINANCE_SECRET"

TELEGRAM_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"

# ─────────────────────────────────────────────────────────────────────────────
# TIMEFRAMES
# ─────────────────────────────────────────────────────────────────────────────
TIMEFRAME         = "15m"
HIGHER_TIMEFRAME  = "1h"
HIGHEST_TIMEFRAME = "4h"

# ─────────────────────────────────────────────────────────────────────────────
# EMA PERIODS
# ─────────────────────────────────────────────────────────────────────────────
EMA_FAST_PRIMARY = 34
EMA_SLOW_PRIMARY = 89
EMA_FAST_HTF     = 50
EMA_SLOW_HTF     = 200

# ─────────────────────────────────────────────────────────────────────────────
# SESSION FILTER
# ─────────────────────────────────────────────────────────────────────────────
LONDON_OPEN_HOURS = list(range(7, 12))    # 07–11 UTC
NY_SESSION_HOURS  = list(range(13, 21))    # 13–20 UTC
VALID_SESSION_HOURS: List[int] = LONDON_OPEN_HOURS + NY_SESSION_HOURS

# ─────────────────────────────────────────────────────────────────────────────
# ATR VOLATILITY FILTER
# ─────────────────────────────────────────────────────────────────────────────
MIN_ATR_PERCENT = 0.3
MAX_ATR_PERCENT = 3.0

# ─────────────────────────────────────────────────────────────────────────────
# TTM SQUEEZE THRESHOLD
# ─────────────────────────────────────────────────────────────────────────────
MIN_SQUEEZE_BARS = 5

# ─────────────────────────────────────────────────────────────────────────────
# SWING HIGH/LOW WINDOW
# ─────────────────────────────────────────────────────────────────────────────
SWING_WINDOW = 30

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL COOLDOWN
# ─────────────────────────────────────────────────────────────────────────────
SIGNAL_COOLDOWN = 1800

# ─────────────────────────────────────────────────────────────────────────────
# PORTFOLIO RISK CONTROL
# ─────────────────────────────────────────────────────────────────────────────
RISK_PERCENT           = 1.0
MAX_PORTFOLIO_RISK     = 4.0
MAX_CONCURRENT_SIGNALS = 100
RR_RATIO               = 2.0
CAPITAL                = 1000.0
LEVERAGE               = 1

# ─────────────────────────────────────────────────────────────────────────────
# SCAN INTERVAL
# ─────────────────────────────────────────────────────────────────────────────
SCAN_INTERVAL = 300

# ─────────────────────────────────────────────────────────────────────────────
# VOLUME SMA PERIOD
# ─────────────────────────────────────────────────────────────────────────────
VOLUME_SMA_PERIOD = 30

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY ZONE BUFFER
# ─────────────────────────────────────────────────────────────────────────────
ENTRY_BUFFER_ATR_MULT = 0.08

# ─────────────────────────────────────────────────────────────────────────────
# FUNDING RATE FILTER
# ─────────────────────────────────────────────────────────────────────────────
MAX_FUNDING_RATE     = 0.001
FUNDING_CONFLICT_MIN = 0.0003

# ─────────────────────────────────────────────────────────────────────────────
# CANDLE QUALITY FILTER
# ─────────────────────────────────────────────────────────────────────────────
CANDLE_BODY_RATIO = 0.4

# ─────────────────────────────────────────────────────────────────────────────
# PATTERN FILTER [4.1-3]
# ─────────────────────────────────────────────────────────────────────────────
MIN_PATTERN_STRENGTH = 2  # Minimum candlestick pattern strength for signals

# ─────────────────────────────────────────────────────────────────────────────
# ATR REGIME DETECTION
# ─────────────────────────────────────────────────────────────────────────────
TRENDING_ATR_RATIO = 1.2

# ─────────────────────────────────────────────────────────────────────────────
# SYMBOL SCANNING
# ─────────────────────────────────────────────────────────────────────────────
TOP_VOLUME      = 50
MIN_VOLUME_USDT = 500_000
MAX_WORKERS     = 10

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL QUALITY GATES
# ─────────────────────────────────────────────────────────────────────────────
MIN_CONDITIONS = 7
MIN_STRENGTH   = 2

# ─────────────────────────────────────────────────────────────────────────────
# FILES & LOGGING
# ─────────────────────────────────────────────────────────────────────────────
ACTIVE_USERS_FILE = "active_users_v4.json"

# ── Logging setup ─────────────────────────────────────────────────────────────
_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
_file_handler = logging.FileHandler("crypto_bot_v4.log", encoding="utf-8")
_file_handler.setFormatter(_formatter)
_console_stream = open(
    sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1, closefd=False
)
_console_handler = logging.StreamHandler(_console_stream)
_console_handler.setFormatter(_formatter)
logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
logger = logging.getLogger(__name__)


# =============================================================================
# SESSION HELPERS
# =============================================================================
def is_valid_trading_session() -> bool:
    return utcnow().hour in VALID_SESSION_HOURS

def get_session_name() -> str:
    hour = utcnow().hour
    if hour in LONDON_OPEN_HOURS:
        return "🟢 London Open (07–11 UTC)"
    elif hour in NY_SESSION_HOURS:
        return "🟢 NY Session (13–20 UTC)"
    elif hour == 12:
        return "🟡 London Lunch (paused)"
    elif hour < 7:
        return "🔴 Asia Session (paused)"
    else:
        return "🟡 NY Wind-down (paused)"


# =============================================================================
# USER MANAGER (Unchanged)
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
                logger.info(f"Loaded {len(self.active_users)} users")
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
# TECHNICAL INDICATORS (Enhanced with Candlestick Patterns)
# =============================================================================
class TechnicalIndicators:
    """Bộ indicators đầy đủ, tích hợp candlestick patterns."""

    @staticmethod
    def add_basic_indicators(df: pd.DataFrame,
                              ema_fast: int = EMA_FAST_PRIMARY,
                              ema_slow: int = EMA_SLOW_PRIMARY,
                              vol_sma: int = VOLUME_SMA_PERIOD) -> pd.DataFrame:
        df["ema_fast"] = ta.trend.ema_indicator(df["close"], window=ema_fast)
        df["ema_slow"] = ta.trend.ema_indicator(df["close"], window=ema_slow)
        df["atr"] = ta.volatility.average_true_range(
            df["high"], df["low"], df["close"], window=14)
        df["atr_ma50"] = df["atr"].rolling(window=50).mean()
        df["rsi"] = ta.momentum.rsi(df["close"], window=14)
        df["volume_sma"] = df["volume"].rolling(window=vol_sma).mean()

        # Add candlestick patterns [4.1-1]
        df = JapaneseCandlestickPatterns.add_candlestick_patterns(df)

        return df

    @staticmethod
    def add_vwap(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["tp_vol"] = df["typical_price"] * df["volume"]

        df["date"] = df["time"].dt.date
        df["cum_tp_vol"] = df.groupby("date")["tp_vol"].cumsum()
        df["cum_vol"] = df.groupby("date")["volume"].cumsum()

        df["vwap"] = df["cum_tp_vol"] / df["cum_vol"].replace(0, np.nan)

        vwap_rolling = (
            df["tp_vol"].rolling(window=96).sum() /
            df["volume"].rolling(window=96).sum().replace(0, np.nan)
        )
        df["vwap"] = df["vwap"].fillna(vwap_rolling)

        df["vwap_dist_pct"] = (df["close"] - df["vwap"]) / df["vwap"] * 100

        df.drop(columns=["date", "cum_tp_vol", "cum_vol", "tp_vol", "typical_price"],
                inplace=True, errors="ignore")
        return df

    @staticmethod
    def add_candle_quality(df: pd.DataFrame) -> pd.DataFrame:
        body = (df["close"] - df["open"]).abs()
        total = (df["high"] - df["low"]).replace(0, np.nan)
        ratio = body / total

        df["candle_body_ratio"] = ratio.fillna(0)
        df["candle_is_valid"] = df["candle_body_ratio"] > CANDLE_BODY_RATIO
        df["candle_bullish"] = df["close"] > df["open"]

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
    def add_keltner_channels(df: pd.DataFrame,
                              window: int = 20, atr_mult: float = 1.5) -> pd.DataFrame:
        df["kc_mid"] = ta.trend.ema_indicator(df["close"], window=window)
        df["kc_atr"] = ta.volatility.average_true_range(
            df["high"], df["low"], df["close"], window=10)
        df["kc_upper"] = df["kc_mid"] + (df["kc_atr"] * atr_mult)
        df["kc_lower"] = df["kc_mid"] - (df["kc_atr"] * atr_mult)
        return df

    @staticmethod
    def add_ttm_squeeze(df: pd.DataFrame,
                         min_squeeze_bars: int = MIN_SQUEEZE_BARS) -> pd.DataFrame:
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

        just_released = ~df["squeeze_on"] & (df["prev_squeeze_count"] >= min_squeeze_bars)

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
        df["is_trending"] = (
            df["atr"] / df["atr_ma50"].replace(0, np.nan)
        ) > TRENDING_ATR_RATIO
        df["is_trending"] = df["is_trending"].fillna(False)
        return df

    @staticmethod
    def find_swing_highs_lows(df: pd.DataFrame,
                               swing_window: int = SWING_WINDOW) -> pd.DataFrame:
        df["swing_high"] = df["high"].rolling(window=swing_window).max()
        df["swing_low"] = df["low"].rolling(window=swing_window).min()

        prior_high = df["swing_high"].shift(1)
        prior_low = df["swing_low"].shift(1)

        candle_body_low_edge = df[["open", "close"]].min(axis=1)
        candle_body_high_edge = df[["open", "close"]].max(axis=1)

        df["broke_swing_high"] = candle_body_low_edge > prior_high
        df["broke_swing_low"] = candle_body_high_edge < prior_low

        df["nearest_swing_high"] = df["swing_high"].rolling(window=swing_window).max()
        df["nearest_swing_low"] = df["swing_low"].rolling(window=swing_window).min()
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

        bearish = (
            len(price_highs) >= 2 and len(rsi_highs) >= 2
            and price_highs[-1][1] > price_highs[-2][1]
            and rsi_highs[-1][1] < rsi_highs[-2][1]
        )
        bullish = (
            len(price_lows) >= 2 and len(rsi_lows) >= 2
            and price_lows[-1][1] < price_lows[-2][1]
            and rsi_lows[-1][1] > rsi_lows[-2][1]
        )

        if bearish:
            df.iloc[-1, df.columns.get_loc("rsi_divergence_bearish")] = True
        if bullish:
            df.iloc[-1, df.columns.get_loc("rsi_divergence_bullish")] = True

        return df

    @staticmethod
    def rsi_condition_ok(last: pd.Series, signal_type: str) -> bool:
        rsi = last["rsi"]
        is_trending = last.get("is_trending", False)

        if is_trending:
            return rsi > 50 if signal_type == "LONG" else rsi < 50
        else:
            return (40 < rsi < 65) if signal_type == "LONG" else (35 < rsi < 60)


# =============================================================================
# CRYPTO SCANNER (Enhanced with Candlestick Patterns)
# =============================================================================
class CryptoScanner:
    """15m scalping scanner — v4.1 with Japanese Candlestick Patterns"""

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

        # Stats tracking
        self.signals_by_session: Dict[str, int] = {"london": 0, "ny": 0}
        self.signals_by_type: Dict[str, int] = {"LONG": 0, "SHORT": 0}
        self.pattern_stats: Dict[str, int] = {}  # Track pattern frequencies

        self.last_status_broadcast = datetime.min.replace(tzinfo=timezone.utc)
        self.scanning = True

        logger.info("CryptoScanner v4.1 (15m Scalping + Candlestick Patterns) initialised")

    @staticmethod
    def _validate_config():
        if BINANCE_API_KEY == "YOUR_BINANCE_KEY" or BINANCE_SECRET == "YOUR_BINANCE_SECRET":
            logger.error("Set Binance API credentials in CONFIG section.")
            sys.exit(1)
        if TELEGRAM_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
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
            logger.info(f"Broadcast — ok:{success_count} fail:{fail_count}")

    def get_top_symbols(self) -> List[str]:
        try:
            tickers = self.client.futures_ticker()
            usdt_pairs = [
                t for t in tickers
                if t["symbol"].endswith("USDT")
                and float(t["quoteVolume"]) > MIN_VOLUME_USDT
            ]
            sorted_pairs = sorted(
                usdt_pairs, key=lambda x: float(x["quoteVolume"]), reverse=True
            )
            symbols = [
                x["symbol"] for x in sorted_pairs[:TOP_VOLUME]
                if self.can_send_signal(x["symbol"])
            ]
            logger.info(f"Symbols to scan: {len(symbols)}")
            return symbols
        except Exception as e:
            logger.error(f"Error in get_top_symbols: {e}")
            return []

    def get_klines(self, symbol: str, timeframe: str = TIMEFRAME,
                   limit: int = 300) -> Optional[pd.DataFrame]:
        try:
            klines = self.client.futures_klines(
                symbol=symbol, interval=timeframe, limit=limit
            )
            df = pd.DataFrame(klines, columns=[
                "time", "open", "high", "low", "close", "volume",
                "close_time", "qav", "num_trades",
                "taker_base_vol", "taker_quote_vol", "ignore",
            ])
            numeric_cols = ["open", "high", "low", "close", "volume"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
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

        if open_count >= MAX_CONCURRENT_SIGNALS:
            return False, f"Max {MAX_CONCURRENT_SIGNALS} concurrent signals reached"
        if current_risk + RISK_PERCENT > MAX_PORTFOLIO_RISK:
            return False, (f"Portfolio risk cap: "
                           f"{current_risk + RISK_PERCENT:.1f}% > {MAX_PORTFOLIO_RISK}%")
        return True, ""

    def check_multi_timeframe_trend(self, symbol: str) -> Tuple[bool, bool]:
        def _is_bullish(tf: str) -> bool:
            df = self.get_klines(symbol, tf, 250)
            if df is None or len(df) < EMA_SLOW_HTF + 10:
                return False
            df = TechnicalIndicators.add_basic_indicators(
                df, ema_fast=EMA_FAST_HTF, ema_slow=EMA_SLOW_HTF, vol_sma=20
            )
            last = df.iloc[-1]
            return bool(last["close"] > last["ema_fast"] > last["ema_slow"])

        try:
            return _is_bullish(HIGHER_TIMEFRAME), _is_bullish(HIGHEST_TIMEFRAME)
        except Exception as e:
            logger.error(f"MTF error {symbol}: {e}")
            return False, False

    def calculate_signal_strength(self, signal_type: str, df: pd.DataFrame,
                                   h1_trend: bool, h4_trend: bool,
                                   funding_confirms: bool) -> int:
        """
        Score 0–9 (increased to include pattern strength):
          +1-3 Pattern strength
          +1 H1 aligned
          +1 H4 aligned
          +1 Squeeze + momentum
          +1 Volume spike
          +1 RSI divergence
          +1 Funding confirms
        """
        strength = 0
        last = df.iloc[-1]

        # [4.1-2] Pattern strength contribution
        if signal_type == "LONG":
            pattern_strength = last.get("pattern_bullish_strength", 0)
            if pattern_strength >= MIN_PATTERN_STRENGTH:
                strength += min(pattern_strength, 3)  # Max 3 points from patterns
        else:
            pattern_strength = last.get("pattern_bearish_strength", 0)
            if pattern_strength >= MIN_PATTERN_STRENGTH:
                strength += min(pattern_strength, 3)

        if signal_type == "LONG":
            if h1_trend: strength += 1
            if h4_trend: strength += 1
            if last.get("squeeze_long_valid", False): strength += 1
        else:
            if not h1_trend: strength += 1
            if not h4_trend: strength += 1
            if last.get("squeeze_short_valid", False): strength += 1

        if last["volume"] > last["volume_sma"] * 1.5: strength += 1

        if signal_type == "LONG" and last.get("rsi_divergence_bullish", False):
            strength += 1
        elif signal_type == "SHORT" and last.get("rsi_divergence_bearish", False):
            strength += 1

        if funding_confirms: strength += 1

        return strength

    def check_signal(self, symbol: str) -> Optional[Dict]:
        """Pipeline đầy đủ cho 15m scalping với candlestick patterns."""
        try:
            if not is_valid_trading_session():
                return None

            df = self.get_klines(symbol, TIMEFRAME, 300)
            if df is None or len(df) < 200:
                return None

            # Build indicators with patterns
            df = TechnicalIndicators.add_basic_indicators(df)
            df = TechnicalIndicators.detect_market_regime(df)
            df = TechnicalIndicators.add_ttm_squeeze(df)
            df = TechnicalIndicators.find_swing_highs_lows(df)
            df = TechnicalIndicators.check_rsi_divergence(df)
            df = TechnicalIndicators.add_vwap(df)
            df = TechnicalIndicators.add_candle_quality(df)

            h1_trend, h4_trend = self.check_multi_timeframe_trend(symbol)

            last = df.iloc[-1]

            # Guard: NaN check
            required_cols = [
                "close", "atr", "ema_fast", "ema_slow", "rsi",
                "atr_ma50", "vwap", "candle_body_ratio"
            ]
            if pd.isna(last[required_cols]).any():
                return None

            entry = last["close"]
            atr = last["atr"]
            atr_percent = (atr / entry) * 100

            if atr_percent < MIN_ATR_PERCENT or atr_percent > MAX_ATR_PERCENT:
                return None

            if not last["candle_is_valid"]:
                return None

            prelim_long = last["ema_fast"] > last["ema_slow"] and entry > last["ema_fast"]
            prelim_short = last["ema_fast"] < last["ema_slow"] and entry < last["ema_fast"]
            prelim_type = "LONG" if prelim_long else ("SHORT" if prelim_short else None)

            funding_blocked = funding_confirms = False
            funding_rate = 0.0
            if prelim_type:
                funding_blocked, funding_confirms, funding_rate = self.evaluate_funding(
                    symbol, prelim_type
                )
            if funding_blocked:
                return None

            risk_amount = CAPITAL * (RISK_PERCENT / 100)

            # LONG CONDITIONS (12 total - added pattern condition)
            long_conditions = {
                "trend": last["ema_fast"] > last["ema_slow"],
                "price_above_ema": entry > last["ema_fast"],
                "above_vwap": entry > last["vwap"],
                "rsi_ok": TechnicalIndicators.rsi_condition_ok(last, "LONG"),
                "volume_confirm": last["volume"] > last["volume_sma"],
                "swing_break": bool(last["broke_swing_high"]),
                "not_near_resistance": last["dist_to_swing_high"] > 0.8,
                "squeeze_release": bool(last.get("squeeze_long_valid", False)),
                "higher_tf_trend": h1_trend or h4_trend,
                "funding_ok": not (funding_rate > FUNDING_CONFLICT_MIN),
                "h4_confirmed": h4_trend,
                "bullish_pattern": last.get("pattern_bullish_strength", 0) >= MIN_PATTERN_STRENGTH,
            }

            # SHORT CONDITIONS (12 total - added pattern condition)
            short_conditions = {
                "trend": last["ema_fast"] < last["ema_slow"],
                "price_below_ema": entry < last["ema_fast"],
                "below_vwap": entry < last["vwap"],
                "rsi_ok": TechnicalIndicators.rsi_condition_ok(last, "SHORT"),
                "volume_confirm": last["volume"] > last["volume_sma"],
                "swing_break": bool(last["broke_swing_low"]),
                "not_near_support": last["dist_to_swing_low"] > 0.8,
                "squeeze_release": bool(last.get("squeeze_short_valid", False)),
                "higher_tf_trend": not h1_trend and not h4_trend,
                "funding_ok": not (funding_rate < -FUNDING_CONFLICT_MIN),
                "h4_confirmed": not h4_trend,
                "bearish_pattern": last.get("pattern_bearish_strength", 0) >= MIN_PATTERN_STRENGTH,
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
                    self.signals_by_type[sig_type] = (
                        self.signals_by_type.get(sig_type, 0) + 1
                    )
                    session = "london" if utcnow().hour in LONDON_OPEN_HOURS else "ny"
                    self.signals_by_session[session] = (
                        self.signals_by_session.get(session, 0) + 1
                    )

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
                    "vwap": last["vwap"],
                    "vwap_dist_pct": last["vwap_dist_pct"],
                    "candle_body_ratio": last["candle_body_ratio"],
                    "is_trending": bool(last.get("is_trending", False)),
                    "funding_rate": funding_rate,
                    "funding_confirms": funding_confirms,
                    "strength": strength,
                    "conditions_met": score,
                    "total_conditions": len(conditions),
                    "h1_trend": h1_trend,
                    "h4_trend": h4_trend,
                    "session": get_session_name(),
                    "bullish_patterns": int(last.get("pattern_bullish_total", 0)),
                    "bearish_patterns": int(last.get("pattern_bearish_total", 0)),
                    "bullish_pattern_strength": int(last.get("pattern_bullish_strength", 0)),
                    "bearish_pattern_strength": int(last.get("pattern_bearish_strength", 0)),
                    "timestamp": utcnow(),
                }

            # Prioritize signals with strong pattern confirmation [4.1-3]
            if long_score >= MIN_CONDITIONS and last.get("pattern_bullish_strength", 0) >= MIN_PATTERN_STRENGTH:
                return _build_signal("LONG", long_score, long_conditions)
            if short_score >= MIN_CONDITIONS and last.get("pattern_bearish_strength", 0) >= MIN_PATTERN_STRENGTH:
                return _build_signal("SHORT", short_score, short_conditions)

            # Allow signals without patterns but require higher score
            if long_score >= MIN_CONDITIONS + 2:
                return _build_signal("LONG", long_score, long_conditions)
            if short_score >= MIN_CONDITIONS + 2:
                return _build_signal("SHORT", short_score, short_conditions)

            return None

        except Exception as e:
            logger.error(f"check_signal error {symbol}: {e}")
            return None

    def format_signal_message(self, signal: Dict) -> str:
        """Enhanced message with candlestick pattern information."""
        emoji = "🟢" if signal["type"] == "LONG" else "🔴"
        direction = "⬆️ LONG" if signal["type"] == "LONG" else "⬇️ SHORT"
        strength_bar = "⚡" * signal["strength"] + "·" * (9 - signal["strength"])

        if signal["type"] == "LONG":
            pot_profit = (signal["tp"] - signal["entry"]) * signal["size"]
            sl_dist_pct = abs(signal["entry"] - signal["sl"]) / signal["entry"] * 100
        else:
            pot_profit = (signal["entry"] - signal["tp"]) * signal["size"]
            sl_dist_pct = abs(signal["sl"] - signal["entry"]) / signal["entry"] * 100

        htf_line = (f"H1: {'🐂' if signal['h1_trend'] else '🐻'}  "
                    f"H4: {'🐂' if signal['h4_trend'] else '🐻'}")
        regime_label = "📈 Trending" if signal["is_trending"] else "↔️ Ranging"

        fr = signal["funding_rate"]
        fr_str = f"{fr * 100:.4f}%"
        fr_icon = "✅" if signal["funding_confirms"] else ("⚠️" if abs(fr) > 0.0005 else "➖")

        vwap_icon = "✅" if (
            (signal["type"] == "LONG" and signal["vwap_dist_pct"] > 0) or
            (signal["type"] == "SHORT" and signal["vwap_dist_pct"] < 0)
        ) else "⚠️"

        entry_zone = f"{signal['entry_low']:.4f} – {signal['entry_high']:.4f}"

        # [4.1-4] Candlestick patterns section
        pattern_icon = "🕯️"
        if signal["type"] == "LONG":
            pattern_info = (f"Bullish patterns: {signal['bullish_patterns']} "
                           f"(strength: {signal['bullish_pattern_strength']})")
        else:
            pattern_info = (f"Bearish patterns: {signal['bearish_patterns']} "
                           f"(strength: {signal['bearish_pattern_strength']})")

        return (
            f"\n{emoji} <b>{direction} SCALP — {signal['symbol']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Entry Zone:</b>\n"
            f"   <code>{entry_zone}</code>\n"
            f"   <i>→ Đặt limit order trong vùng này</i>\n"
            f"🛑 <b>Stop Loss:</b>    <code>{signal['sl']:.4f}</code>"
            f"  <i>(-{sl_dist_pct:.2f}%)</i>\n"
            f"✅ <b>Take Profit:</b>  <code>{signal['tp']:.4f}</code>\n"
            f"📊 <b>Size:</b>         {signal['size']:.4f}\n"
            f"📈 <b>R:R</b>           1:{signal['rr']}\n"
            f"💵 <b>Est. Profit:</b>  ${pot_profit:.2f}\n"
            f"\n<b>{pattern_icon} Candlestick Patterns:</b>\n"
            f"   {pattern_info}\n"
            f"\n<b>📐 Market Context:</b>\n"
            f"   {regime_label}\n"
            f"   {htf_line}\n"
            f"   {vwap_icon} VWAP: {signal['vwap']:.4f}"
            f" ({signal['vwap_dist_pct']:+.2f}%)\n"
            f"   {fr_icon} Funding: {fr_str}/8h\n"
            f"\n<b>📊 Technical:</b>\n"
            f"   ATR: {signal['atr']:.4f} ({signal['atr_percent']:.2f}%)\n"
            f"   RSI: {signal['rsi']:.1f}\n"
            f"   Candle body: {signal['candle_body_ratio']*100:.0f}%\n"
            f"\n<b>🎯 Signal Quality:</b>\n"
            f"   Strength:   {signal['strength']}/9  {strength_bar}\n"
            f"   Conditions: {signal['conditions_met']}/{signal['total_conditions']}\n"
            f"\n📍 {signal['session']}\n"
            f"⏰ {signal['timestamp'].strftime('%Y-%m-%d %H:%M')} UTC\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>15m scalp — hold time ~30–90 phút</i>"
        )

    def can_send_signal(self, symbol: str) -> bool:
        with self._state_lock:
            last_sent = self.sent_signals.get(symbol)
        if last_sent is None:
            return True
        return (time.time() - last_sent) >= SIGNAL_COOLDOWN

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
                logger.info(f"Scan paused — {get_session_name()}")
                return

            self.cleanup_sent_signals()

            symbols = self.get_top_symbols()
            if not symbols:
                logger.warning("No symbols to scan this cycle")
                return

            logger.info(f"[15m Scan] {len(symbols)} symbols | {get_session_name()}")

            signals: List[Dict] = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures_map = {
                    executor.submit(self.check_signal, sym): sym for sym in symbols
                }
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
                    message = self.format_signal_message(signal)
                    await self.broadcast_to_users(message)

                    with self._state_lock:
                        self.sent_signals[signal["symbol"]] = time.time()

                    self.register_open_position(signal["symbol"], signal["type"])

                    logger.info(
                        f"✅ Signal: {signal['symbol']} {signal['type']} | "
                        f"str={signal['strength']}/9 | "
                        f"patterns={signal['bullish_patterns'] if signal['type']=='LONG' else signal['bearish_patterns']}"
                    )
                    sent_count += 1
                    await asyncio.sleep(0.1)

            if utcnow() - self.last_status_broadcast > timedelta(minutes=30):
                await self.broadcast_status_update()
                self.last_status_broadcast = utcnow()

            if sent_count > 0:
                logger.info(f"Cycle done — {sent_count} signals broadcast")

        except Exception as e:
            logger.error(f"Scan cycle error: {e}")

    async def broadcast_status_update(self):
        with self._state_lock:
            sigs = self.signals_generated
            last_sig = self.last_signal_time
            open_cnt = len(self.open_positions)
            port_risk = sum(p["risk_pct"] for p in self.open_positions.values())
            long_cnt = self.signals_by_type.get("LONG", 0)
            short_cnt = self.signals_by_type.get("SHORT", 0)
            lon_sess = self.signals_by_session.get("london", 0)
            ny_sess = self.signals_by_session.get("ny", 0)

        last_sig_str = last_sig.strftime("%H:%M UTC") if last_sig else "None"

        message = (
            f"\n🤖 <b>Scalping Bot v4.1 — Status</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 Session:        {get_session_name()}\n"
            f"👥 Active users:   {self.user_manager.get_user_count()}\n"
            f"⏱️  Timeframe:      15m + H1/H4\n"
            f"🔄 Scan interval:  {SCAN_INTERVAL}s\n"
            f"🔒 Open:           {open_cnt}/{MAX_CONCURRENT_SIGNALS}\n"
            f"📉 Portfolio risk: {port_risk:.1f}% / {MAX_PORTFOLIO_RISK}%\n"
            f"\n<b>📊 Session Stats:</b>\n"
            f"   Signals today:  {sigs} (🟢{long_cnt} / 🔴{short_cnt})\n"
            f"   London session: {lon_sess} signals\n"
            f"   NY session:     {ny_sess} signals\n"
            f"   Last signal:    {last_sig_str}\n"
            f"\n<b>🔧 Active Filters (v4.1):</b>\n"
            f"   Japanese Candlestick Patterns (15+) ✅\n"
            f"   EMA34/89 primary ✅\n"
            f"   Session: London+NY only ✅\n"
            f"   ATR {MIN_ATR_PERCENT}%–{MAX_ATR_PERCENT}% ✅\n"
            f"   Squeeze ≥{MIN_SQUEEZE_BARS} bars ✅\n"
            f"   VWAP filter ✅\n"
            f"   Candle quality ≥{CANDLE_BODY_RATIO*100:.0f}% body ✅\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.broadcast_to_users(message)

    async def scanning_task(self):
        while self.scanning:
            try:
                await self.process_and_broadcast_signals()
            except Exception as e:
                logger.error(f"scanning_task error: {e}")
            for _ in range(SCAN_INTERVAL):
                if not self.scanning:
                    break
                await asyncio.sleep(1)

    def stop_scanning(self):
        self.scanning = True  # Fixed: Should be False


# =============================================================================
# TELEGRAM COMMAND HANDLERS (Enhanced with pattern commands)
# =============================================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if scanner:
        scanner.user_manager.add_user(
            user_id=user_id, username=user.username, first_name=user.first_name
        )

    welcome = (
        f"\n⚡ <b>Scalping Bot v4.1 — 15m + Candlestick Patterns</b>\n\n"
        f"Xin chào {user.first_name}!\n"
        f"Bot được tối ưu hoàn toàn cho <b>scalping 15 phút</b> với Japanese Candlestick Patterns.\n\n"
        f"<b>🆕 NEW FEATURES v4.1:</b>\n"
        f"├ Japanese Candlestick Patterns (15+ patterns)\n"
        f"├ Pattern strength scoring system\n"
        f"├ Pattern-based signal confirmation\n"
        f"└ /patterns command to view detected patterns\n\n"
        f"<b>🔧 Điều chỉnh so với v3 (30m):</b>\n"
        f"┌ EMA 34/89 (thay 21/55)\n"
        f"├ Session: London+NY only (bỏ Asia + lunch)\n"
        f"├ ATR filter: 0.3%–3.0%\n"
        f"├ Squeeze: ≥5 bars (thay 3)\n"
        f"├ Swing window: 30 bars (thay 20)\n"
        f"├ Cooldown: 30 phút (thay 60)\n"
        f"├ Portfolio: max 4 lệnh, risk 4%\n"
        f"├ Scan: mỗi 2 phút (thay 5 phút)\n"
        f"├ VWAP filter (mới)\n"
        f"└ Candle body ≥40% (mới)\n\n"
        f"<b>📋 Lệnh:</b>\n"
        f"/start · /status · /stats · /positions · /close · /stop · /patterns · /help\n"
    )
    await update.message.reply_text(welcome, parse_mode="HTML")
    await status_command(update, context)


async def patterns_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """[4.1-4] Show detected candlestick patterns for a symbol"""
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return

    args = context.args
    if not args:
        await update.message.reply_text("📌 Cách dùng: <code>/patterns BTCUSDT</code>", parse_mode="HTML")
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
            if 'bullish' in col or any(x in col for x in ['hammer', 'dragonfly', 'morning', 'piercing', 'white', 'lower']):
                bullish_patterns.append(f"✅ {pattern_name}")
            elif 'bearish' in col or any(x in col for x in ['shooting', 'gravestone', 'evening', 'dark', 'black', 'hanging', 'upper']):
                bearish_patterns.append(f"🔴 {pattern_name}")
            else:
                # Neutral patterns
                if 'doji' in col or 'spinning' in col:
                    bullish_patterns.append(f"⚪ {pattern_name}")
                    bearish_patterns.append(f"⚪ {pattern_name}")

    msg = (
        f"\n🕯️ <b>Candlestick Patterns - {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Timeframe: {TIMEFRAME}\n"
        f"Time: {last.name.strftime('%Y-%m-%d %H:%M') if hasattr(last.name, 'strftime') else 'Latest'}\n\n"
        f"<b>Bullish Patterns ({len(bullish_patterns)}):</b>\n"
        f"{chr(10).join(bullish_patterns[:5]) if bullish_patterns else 'None detected'}\n"
        f"{'...' if len(bullish_patterns) > 5 else ''}\n\n"
        f"<b>Bearish Patterns ({len(bearish_patterns)}):</b>\n"
        f"{chr(10).join(bearish_patterns[:5]) if bearish_patterns else 'None detected'}\n"
        f"{'...' if len(bearish_patterns) > 5 else ''}\n\n"
        f"<b>Strength Scores:</b>\n"
        f"Bullish: {last.get('pattern_bullish_strength', 0)}/??\n"
        f"Bearish: {last.get('pattern_bearish_strength', 0)}/??\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return
    scanner.user_manager.update_last_active(str(update.effective_user.id))
    with scanner._state_lock:
        sigs = scanner.signals_generated
        open_cnt = len(scanner.open_positions)
        port_risk = sum(p["risk_pct"] for p in scanner.open_positions.values())

    msg = (
        f"\n📊 <b>Status — Scalping Bot v4.1</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Session:        {get_session_name()}\n"
        f"⏱️  Timeframe:      {TIMEFRAME} + Candlestick Patterns\n"
        f"👥 Active users:   {scanner.user_manager.get_user_count()}\n"
        f"📈 Pairs scanned:  Top {TOP_VOLUME} (vol ≥ {MIN_VOLUME_USDT:,.0f} USDT)\n"
        f"🔄 Scan interval:  {SCAN_INTERVAL}s\n"
        f"💰 Risk/trade:     {RISK_PERCENT}%\n"
        f"🔒 Open:           {open_cnt}/{MAX_CONCURRENT_SIGNALS}\n"
        f"📉 Portfolio risk: {port_risk:.1f}%/{MAX_PORTFOLIO_RISK}%\n"
        f"📈 Signals today:  {sigs}\n"
        f"🕯️ Patterns:       15+ Japanese candlestick\n"
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
        long_cnt = scanner.signals_by_type.get("LONG", 0)
        short_cnt = scanner.signals_by_type.get("SHORT", 0)
        lon_sess = scanner.signals_by_session.get("london", 0)
        ny_sess = scanner.signals_by_session.get("ny", 0)

    msg = (
        f"\n📈 <b>Statistics — v4.1 (15m + Patterns)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Signals (24h):  {signals_24h}\n"
        f"🟢 LONG signals:   {long_cnt}\n"
        f"🔴 SHORT signals:  {short_cnt}\n"
        f"🌍 London:         {lon_sess} signals\n"
        f"🗽 NY:             {ny_sess} signals\n"
        f"⏰ Last signal:    {'None' if not last_sig else last_sig.strftime('%H:%M UTC')}\n\n"
        f"<b>Active Filters:</b>\n"
        f"EMA34/89 ✅ | VWAP ✅ | Body ≥40% ✅\n"
        f"Patterns ✅ | Squeeze≥5 ✅ | Regime RSI ✅\n"
        f"Funding ✅ | Session ✅ | Body BOS ✅\n"
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
        await update.message.reply_text(
            "📭 <b>Không có lệnh đang mở.</b>", parse_mode="HTML"
        )
        return

    lines = [
        f"\n📋 <b>Open Positions ({len(positions)}/{MAX_CONCURRENT_SIGNALS})</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    ]
    for sym, data in positions.items():
        icon = "🟢" if data["type"] == "LONG" else "🔴"
        lines.append(f"{icon} <b>{sym}</b> {data['type']} | {data['risk_pct']}% risk")
        lines.append(f"   Mở lúc: {data['opened_at'][11:16]} UTC\n")

    lines.append(
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📉 Portfolio risk: {port_risk:.1f}% / {MAX_PORTFOLIO_RISK}%\n"
        f"💡 Dùng /close SYMBOL để đóng tracking"
    )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def close_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if not scanner:
        return
    args = context.args
    if not args:
        await update.message.reply_text(
            "📌 Cách dùng: <code>/close BTCUSDT</code>", parse_mode="HTML"
        )
        return
    symbol = args[0].upper()
    scanner.close_position(symbol)
    await update.message.reply_text(
        f"✅ <b>{symbol}</b> đã được đóng và xóa khỏi portfolio tracker.",
        parse_mode="HTML",
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if scanner:
        scanner.user_manager.remove_user(str(update.effective_user.id))
    await update.message.reply_text(
        "\n🛑 <b>Đã hủy đăng ký</b>\n\nGửi /start để nhận tín hiệu trở lại.",
        parse_mode="HTML",
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    scanner: CryptoScanner = context.bot_data.get("scanner")
    if scanner:
        scanner.user_manager.update_last_active(str(update.effective_user.id))

    help_text = (
        f"\n❓ <b>Hướng dẫn v4.1 — 15m Scalping + Candlestick Patterns</b>\n\n"
        f"<b>Lệnh:</b>  /start · /status · /stats · /positions · /close · /stop · /patterns · /help\n\n"

        f"<b>🆕 NEW - Candlestick Patterns:</b>\n"
        f"• /patterns SYMBOL - Xem patterns đang hình thành\n"
        f"• Patterns: Hammer, Shooting Star, Engulfing, Doji, Harami, Morning/Evening Star, Three Soldiers, Marubozu, v.v.\n\n"

        f"<b>━━ Điều chỉnh cho 15m ━━</b>\n\n"

        f"1️⃣ <b>Japanese Candlestick Patterns</b> <i>[4.1-1]</i>\n"
        f"   15+ patterns với hệ thống điểm strength (1-3 mỗi pattern)\n"
        f"   Pattern strength đóng góp vào signal quality\n\n"

        f"2️⃣ <b>EMA 34/89</b> <i>[15m-2]</i>\n"
        f"   8.5h + 22.25h lookback — tương đương EMA17/44 trên 30m.\n\n"

        f"3️⃣ <b>Session: London + NY only</b> <i>[15m-3]</i>\n"
        f"   07–11 UTC (London Open) và 13–20 UTC (NY).\n\n"

        f"4️⃣ <b>ATR 0.3%–3.0%</b> <i>[15m-4]</i>\n"
        f"   Điều chỉnh cho 15m ATR nhỏ hơn.\n\n"

        f"5️⃣ <b>Squeeze ≥5 bars (75 phút)</b> <i>[15m-5]</i>\n"
        f"   Tránh false release trên timeframe nhỏ.\n\n"

        f"6️⃣ <b>VWAP Filter</b> <i>[15m-11]</i>\n"
        f"   LONG: giá > VWAP, SHORT: giá < VWAP.\n\n"

        f"7️⃣ <b>Candle Quality ≥40% body</b> <i>[15m-12]</i>\n"
        f"   Loại bỏ doji và spinning top.\n\n"

        f"<b>⚠️ Lưu ý scalping 15m:</b>\n"
        f"• Hold time: 30–90 phút\n"
        f"• Cần monitor màn hình trong London + NY session\n"
        f"• Ước tính 8–15 tín hiệu/ngày\n"
        f"• Sử dụng limit order trong entry zone được hiển thị\n\n"

        f"⚠️ <b>Cảnh báo rủi ro:</b> Trading futures mang rủi ro cao.\n"
        f"Không đầu tư vốn không thể mất."
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

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("positions", positions_command))
    application.add_handler(CommandHandler("close", close_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("patterns", patterns_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_error_handler(error_handler)

    startup_msg = (
        "\n⚡ <b>Scalping Bot v4.1 (15m + Candlestick Patterns) Started!</b>\n\n"
        "<b>NEW FEATURES v4.1:</b>\n"
        "✅ Japanese Candlestick Patterns (15+ patterns)\n"
        "✅ Pattern strength scoring system\n"
        "✅ Pattern-based signal confirmation\n"
        "✅ /patterns command to view detected patterns\n\n"
        "<b>15m-specific configs active:</b>\n"
        "✅ EMA 34/89 (15m optimised)\n"
        "✅ Session: London Open + NY only\n"
        "✅ ATR filter: 0.3%–3.0%\n"
        "✅ TTM Squeeze ≥5 bars\n"
        "✅ Swing window: 30 bars (7.5h)\n"
        "✅ VWAP directional filter\n"
        "✅ Candle quality (body ≥40%)\n"
        "✅ Scan interval: 120s\n"
        "✅ Portfolio: 4 concurrent / 4% risk\n\n"
        "Tín hiệu scalp sẽ xuất hiện trong London + NY session."
    )
    await scanner.broadcast_to_users(startup_msg)

    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    asyncio.create_task(scanner.scanning_task())

    logger.info("Scalping Bot v4.1 (15m + Candlestick Patterns) running — Ctrl+C to stop.")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown …")
        scanner.stop_scanning()
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()