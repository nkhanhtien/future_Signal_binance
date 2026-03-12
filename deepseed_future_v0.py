#!/usr/bin/env python3
"""
Advanced Crypto Trading Signal Bot
Optimized version with multi-timeframe analysis and advanced indicators
"""

import sys
import logging
import warnings

# Suppress OpenSSL warning
warnings.filterwarnings("ignore", category=UserWarning, module='urllib3')

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Now import everything else
import pandas as pd
import ta
import time
import json
import os
import numpy as np
from datetime import datetime, timedelta
from binance.client import Client
from binance.exceptions import BinanceAPIException
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Tuple
import asyncio
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
from collections import deque
import signal
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =========================
# CONFIG
# =========================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET = os.getenv("BINANCE_SECRET", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

# Validate credentials
if not all([BINANCE_API_KEY, BINANCE_SECRET, TELEGRAM_TOKEN]):
    logger.error("Missing credentials in .env file")
    logger.info("Please create a .env file with:")
    logger.info("BINANCE_API_KEY=your_key")
    logger.info("BINANCE_SECRET=your_secret")
    logger.info("TELEGRAM_TOKEN=your_token")
    sys.exit(1)

# Trading parameters
TIMEFRAME = "15m"
HIGHER_TIMEFRAME = "1h"
HIGHEST_TIMEFRAME = "4h"
RISK_PERCENT = 1.0
RR_RATIO = 2.0
CAPITAL = 1000.0
TOP_VOLUME = 50
SCAN_INTERVAL = 300
MIN_VOLUME_USDT = 100000
MAX_WORKERS = 10
MIN_ATR_PERCENT = 0.5
MAX_ATR_PERCENT = 5.0
CACHE_TTL = 60
SIGNAL_COOLDOWN = 3600
MAX_SIGNALS_HISTORY = 1000
ACTIVE_USERS_FILE = "active_users.json"
SIGNALS_HISTORY_FILE = "signals_history.json"

# =========================
# ENUMS & DATACLASSES
# =========================

class SignalType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

@dataclass
class Signal:
    type: SignalType
    symbol: str
    entry: float
    sl: float
    tp: float
    size: float
    rr: float
    atr: float
    atr_percent: float
    rsi: float
    strength: int
    conditions_met: int
    total_conditions: int
    timestamp: datetime
    signal_id: str = None
    
    def __post_init__(self):
        if self.signal_id is None:
            self.signal_id = self.generate_signal_id()
    
    def generate_signal_id(self) -> str:
        data = f"{self.symbol}_{self.timestamp.timestamp()}_{self.type.value}"
        return hashlib.md5(data.encode()).hexdigest()[:8]
    
    def to_dict(self):
        data = asdict(self)
        data['type'] = self.type.value
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data):
        data['type'] = SignalType(data['type'])
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

# =========================
# RATE LIMITER
# =========================

class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, rate: float, per: float = 1.0):
        self.rate = rate
        self.per = per
        self.tokens = rate
        self.last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: float = 1.0) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per))
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

# =========================
# CACHE MANAGER
# =========================

class CacheManager:
    """Thread-safe cache manager with TTL"""
    
    def __init__(self, ttl: int = CACHE_TTL):
        self.cache = {}
        self.ttl = ttl
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[any]:
        async with self._lock:
            if key in self.cache:
                data, timestamp = self.cache[key]
                if time.time() - timestamp < self.ttl:
                    return data
                else:
                    del self.cache[key]
            return None
    
    async def set(self, key: str, value: any):
        async with self._lock:
            self.cache[key] = (value, time.time())
    
    async def clear_expired(self):
        async with self._lock:
            now = time.time()
            expired = [k for k, (_, t) in self.cache.items() if now - t >= self.ttl]
            for k in expired:
                del self.cache[k]

# =========================
# USER MANAGER
# =========================

class UserManager:
    """Enhanced user manager with preferences and stats"""
    
    def __init__(self, storage_file: str):
        self.storage_file = storage_file
        self.active_users: Dict[str, dict] = {}
        self._lock = asyncio.Lock()
        self.load_users()
    
    def load_users(self):
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as f:
                    self.active_users = json.load(f)
                logger.info(f"Loaded {len(self.active_users)} users")
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            self.active_users = {}
    
    async def save_users(self):
        async with self._lock:
            try:
                with open(self.storage_file, 'w') as f:
                    json.dump(self.active_users, f, indent=2)
            except Exception as e:
                logger.error(f"Error saving users: {e}")
    
    async def add_user(self, user_id: str, username: str = None, first_name: str = None):
        async with self._lock:
            self.active_users[user_id] = {
                'username': username,
                'first_name': first_name,
                'joined_at': datetime.now().isoformat(),
                'last_active': datetime.now().isoformat(),
                'active': True,
                'preferences': {
                    'receive_signals': True,
                    'receive_updates': True,
                    'min_strength': 3
                },
                'stats': {
                    'signals_received': 0,
                    'last_signal_time': None
                }
            }
        await self.save_users()
        logger.info(f"User {user_id} added/updated")
    
    async def remove_user(self, user_id: str):
        async with self._lock:
            if user_id in self.active_users:
                self.active_users[user_id]['active'] = False
                self.active_users[user_id]['left_at'] = datetime.now().isoformat()
        await self.save_users()
        logger.info(f"User {user_id} deactivated")
    
    async def get_active_users(self, min_strength: int = None) -> List[Tuple[str, dict]]:
        async with self._lock:
            users = []
            for uid, data in self.active_users.items():
                if data.get('active', False):
                    prefs = data.get('preferences', {})
                    if prefs.get('receive_signals', True):
                        if min_strength is None or prefs.get('min_strength', 1) <= min_strength:
                            users.append((uid, data))
            return users
    
    async def update_last_active(self, user_id: str):
        async with self._lock:
            if user_id in self.active_users:
                self.active_users[user_id]['last_active'] = datetime.now().isoformat()
        await self.save_users()
    
    async def increment_signals_received(self, user_id: str):
        async with self._lock:
            if user_id in self.active_users:
                stats = self.active_users[user_id].setdefault('stats', {})
                stats['signals_received'] = stats.get('signals_received', 0) + 1
                stats['last_signal_time'] = datetime.now().isoformat()
        await self.save_users()
    
    async def get_user_count(self) -> int:
        async with self._lock:
            return sum(1 for data in self.active_users.values() if data.get('active', False))

# =========================
# TECHNICAL INDICATORS
# =========================

class TechnicalIndicators:
    """Optimized technical indicators"""
    
    @classmethod
    def add_basic_indicators(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Add basic indicators with vectorized operations"""
        # EMAs - vectorized
        df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
        
        # ATR - optimized calculation
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        df['atr'] = true_range.rolling(window=14).mean()
        
        # RSI - optimized
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Volume SMA
        df['volume_sma'] = df['volume'].rolling(window=20).mean()
        
        return df
    
    @classmethod
    def add_bollinger_bands(cls, df: pd.DataFrame, window: int = 20, std: int = 2) -> pd.DataFrame:
        """Optimized Bollinger Bands"""
        rolling_mean = df['close'].rolling(window=window).mean()
        rolling_std = df['close'].rolling(window=window).std()
        
        df['bb_high'] = rolling_mean + (rolling_std * std)
        df['bb_low'] = rolling_mean - (rolling_std * std)
        df['bb_mid'] = rolling_mean
        df['bb_width'] = (df['bb_high'] - df['bb_low']) / df['bb_mid']
        df['bb_percent'] = (df['close'] - df['bb_low']) / (df['bb_high'] - df['bb_low'])
        
        return df
    
    @classmethod
    def add_keltner_channels(cls, df: pd.DataFrame, window: int = 20, atr_mult: float = 1.5) -> pd.DataFrame:
        """Optimized Keltner Channels"""
        df['kc_mid'] = df['close'].ewm(span=window, adjust=False).mean()
        
        if 'atr' not in df.columns:
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            df['kc_atr'] = true_range.rolling(window=10).mean()
        else:
            df['kc_atr'] = df['atr']
        
        df['kc_upper'] = df['kc_mid'] + (df['kc_atr'] * atr_mult)
        df['kc_lower'] = df['kc_mid'] - (df['kc_atr'] * atr_mult)
        
        return df
    
    @classmethod
    def add_ttm_squeeze(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Optimized TTM Squeeze"""
        df = cls.add_bollinger_bands(df)
        df = cls.add_keltner_channels(df)
        
        df['squeeze_on'] = (df['bb_low'] > df['kc_lower']) & (df['bb_high'] < df['kc_upper'])
        df['squeeze_count'] = df['squeeze_on'].groupby((~df['squeeze_on']).cumsum()).cumcount()
        
        median_prices = (df['high'] + df['low']) / 2
        fast_ma = median_prices.rolling(window=5).mean()
        slow_ma = median_prices.rolling(window=34).mean()
        df['squeeze_momentum'] = fast_ma - slow_ma
        
        return df
    
    @classmethod
    def find_swing_highs_lows(cls, df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
        """Optimized swing point detection"""
        df['swing_high'] = df['high'].rolling(window=window, center=True).max()
        df['swing_low'] = df['low'].rolling(window=window, center=True).min()
        
        df['broke_swing_high'] = df['close'] > df['swing_high'].shift(2)
        df['broke_swing_low'] = df['close'] < df['swing_low'].shift(2)
        
        df['nearest_swing_high'] = df['high'].expanding().max()
        df['nearest_swing_low'] = df['low'].expanding().min()
        
        df['dist_to_swing_high'] = (df['nearest_swing_high'] - df['close']) / df['close'] * 100
        df['dist_to_swing_low'] = (df['close'] - df['nearest_swing_low']) / df['close'] * 100
        
        return df
    
    @classmethod
    def check_rsi_divergence(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Optimized RSI divergence detection"""
        df['rsi_divergence_bullish'] = False
        df['rsi_divergence_bearish'] = False
        
        if len(df) < 20:
            return df
        
        price_peaks = (df['high'] > df['high'].shift(1)) & (df['high'] > df['high'].shift(-1))
        price_troughs = (df['low'] < df['low'].shift(1)) & (df['low'] < df['low'].shift(-1))
        rsi_peaks = price_peaks & (df['rsi'] > df['rsi'].shift(1)) & (df['rsi'] > df['rsi'].shift(-1))
        rsi_troughs = price_troughs & (df['rsi'] < df['rsi'].shift(1)) & (df['rsi'] < df['rsi'].shift(-1))
        
        if price_peaks.iloc[-5:].any() and rsi_peaks.iloc[-5:].any():
            last_price_peak_idx = price_peaks.iloc[-20:][price_peaks.iloc[-20:]].index[-1]
            last_rsi_peak_idx = rsi_peaks.iloc[-20:][rsi_peaks.iloc[-20:]].index[-1]
            
            if last_price_peak_idx == last_rsi_peak_idx:
                prev_price_peak_idx = price_peaks.iloc[:last_price_peak_idx][price_peaks.iloc[:last_price_peak_idx]].index[-1] if len(price_peaks.iloc[:last_price_peak_idx][price_peaks.iloc[:last_price_peak_idx]]) > 0 else None
                
                if prev_price_peak_idx:
                    if (df.loc[last_price_peak_idx, 'high'] > df.loc[prev_price_peak_idx, 'high'] and
                        df.loc[last_rsi_peak_idx, 'rsi'] < df.loc[prev_price_peak_idx, 'rsi']):
                        df.loc[last_price_peak_idx, 'rsi_divergence_bearish'] = True
        
        if price_troughs.iloc[-5:].any() and rsi_troughs.iloc[-5:].any():
            last_price_trough_idx = price_troughs.iloc[-20:][price_troughs.iloc[-20:]].index[-1]
            last_rsi_trough_idx = rsi_troughs.iloc[-20:][rsi_troughs.iloc[-20:]].index[-1]
            
            if last_price_trough_idx == last_rsi_trough_idx:
                prev_price_trough_idx = price_troughs.iloc[:last_price_trough_idx][price_troughs.iloc[:last_price_trough_idx]].index[-1] if len(price_troughs.iloc[:last_price_trough_idx][price_troughs.iloc[:last_price_trough_idx]]) > 0 else None
                
                if prev_price_trough_idx:
                    if (df.loc[last_price_trough_idx, 'low'] < df.loc[prev_price_trough_idx, 'low'] and
                        df.loc[last_rsi_trough_idx, 'rsi'] > df.loc[prev_price_trough_idx, 'rsi']):
                        df.loc[last_price_trough_idx, 'rsi_divergence_bullish'] = True
        
        return df

# =========================
# CRYPTO SCANNER
# =========================

class CryptoScanner:
    """Enhanced scanner with proper error handling and rate limiting"""
    
    def __init__(self):
        # Initialize clients with timeouts
        self.client = Client(BINANCE_API_KEY, BINANCE_SECRET, {"timeout": 30})
        
        # Rate limiters
        self.binance_rate_limiter = RateLimiter(rate=20, per=1.0)
        self.telegram_rate_limiter = RateLimiter(rate=30, per=1.0)
        
        # Cache manager
        self.cache_manager = CacheManager()
        
        # User manager
        self.user_manager = UserManager(ACTIVE_USERS_FILE)
        
        # Signal tracking
        self.sent_signals: Dict[str, float] = {}
        self.signals_history: deque = deque(maxlen=MAX_SIGNALS_HISTORY)
        self.signals_generated = 0
        self.last_signal_time = None
        
        # Control flag
        self.scanning = True
        self.last_status_broadcast = datetime.min
        
        # Performance metrics
        self.metrics = {
            'total_scans': 0,
            'successful_scans': 0,
            'failed_scans': 0,
            'avg_scan_time': 0,
            'total_api_calls': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
        
        # Load signal history
        self._load_signal_history()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("CryptoScanner initialized successfully")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.scanning = False
    
    def _load_signal_history(self):
        """Load signal history from file"""
        try:
            if os.path.exists(SIGNALS_HISTORY_FILE):
                with open(SIGNALS_HISTORY_FILE, 'r') as f:
                    data = json.load(f)
                    for signal_data in data[-MAX_SIGNALS_HISTORY:]:
                        try:
                            signal = Signal.from_dict(signal_data)
                            self.signals_history.append(signal)
                        except:
                            continue
                logger.info(f"Loaded {len(self.signals_history)} historical signals")
        except Exception as e:
            logger.error(f"Error loading signal history: {e}")
    
    async def _save_signal_history(self):
        """Save signal history to file"""
        try:
            data = [s.to_dict() for s in self.signals_history]
            with open(SIGNALS_HISTORY_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving signal history: {e}")
    
    async def broadcast_to_users(self, message: str, parse_mode: str = 'HTML', min_strength: int = None):
        """Enhanced broadcast with rate limiting"""
        users = await self.user_manager.get_active_users(min_strength)
        success_count = 0
        fail_count = 0
        
        for user_id, user_data in users:
            if not await self.telegram_rate_limiter.acquire():
                await asyncio.sleep(0.1)
            
            try:
                bot = Bot(token=TELEGRAM_TOKEN)
                await bot.send_message(
                    chat_id=int(user_id),
                    text=message,
                    parse_mode=parse_mode
                )
                success_count += 1
                await asyncio.sleep(0.05)
            except TelegramError as e:
                logger.error(f"Failed to send to user {user_id}: {e}")
                fail_count += 1
                
                if "blocked" in str(e) or "deactivated" in str(e):
                    await self.user_manager.remove_user(user_id)
                elif "rate limit" in str(e).lower():
                    await asyncio.sleep(1)
        
        if success_count > 0:
            logger.info(f"Broadcast complete. Success: {success_count}, Failed: {fail_count}")
    
    async def get_top_symbols(self) -> List[str]:
        """Get top volume USDT pairs with caching"""
        cache_key = "top_symbols"
        cached = await self.cache_manager.get(cache_key)
        if cached:
            self.metrics['cache_hits'] += 1
            return cached
        
        self.metrics['cache_misses'] += 1
        self.metrics['total_api_calls'] += 1
        
        try:
            if not await self.binance_rate_limiter.acquire():
                await asyncio.sleep(0.1)
            
            tickers = self.client.futures_ticker()
            
            usdt_pairs = [
                t for t in tickers 
                if t['symbol'].endswith("USDT") 
                and float(t['quoteVolume']) > MIN_VOLUME_USDT
            ]
            
            sorted_pairs = sorted(
                usdt_pairs, 
                key=lambda x: float(x['quoteVolume']), 
                reverse=True
            )
            
            top_symbols = [x['symbol'] for x in sorted_pairs[:TOP_VOLUME]]
            
            await self.cache_manager.set(cache_key, top_symbols)
            
            logger.info(f"Found {len(top_symbols)} top volume symbols")
            return top_symbols
            
        except BinanceAPIException as e:
            logger.error(f"Binance API error in get_top_symbols: {e}")
            return []
        except Exception as e:
            logger.error(f"Error in get_top_symbols: {e}")
            return []
    
    async def get_klines(self, symbol: str, timeframe: str = TIMEFRAME, limit: int = 200) -> Optional[pd.DataFrame]:
        """Get klines with caching and rate limiting"""
        cache_key = f"klines_{symbol}_{timeframe}_{limit}"
        cached = await self.cache_manager.get(cache_key)
        if cached is not None:
            self.metrics['cache_hits'] += 1
            return cached
        
        self.metrics['cache_misses'] += 1
        self.metrics['total_api_calls'] += 1
        
        try:
            max_retries = 3
            for attempt in range(max_retries):
                if await self.binance_rate_limiter.acquire():
                    break
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))
            else:
                logger.warning(f"Rate limit exceeded for {symbol}")
                return None
            
            klines = self.client.futures_klines(
                symbol=symbol, 
                interval=timeframe, 
                limit=limit
            )
            
            df = pd.DataFrame(klines, columns=[
                'time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'qav', 'num_trades',
                'taker_base_vol', 'taker_quote_vol', 'ignore'
            ])
            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            df = df.dropna().reset_index(drop=True)
            
            await self.cache_manager.set(cache_key, df)
            
            return df
            
        except BinanceAPIException as e:
            logger.error(f"Binance API error for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting klines for {symbol}: {e}")
            return None
    
    async def check_multi_timeframe_trend(self, symbol: str) -> Tuple[bool, bool]:
        """Check multi-timeframe trend with caching"""
        cache_key = f"mft_{symbol}"
        cached = await self.cache_manager.get(cache_key)
        if cached is not None:
            return cached
        
        try:
            df_h1 = await self.get_klines(symbol, HIGHER_TIMEFRAME, 100)
            if df_h1 is not None and len(df_h1) > 50:
                df_h1 = TechnicalIndicators.add_basic_indicators(df_h1)
                last_h1 = df_h1.iloc[-1]
                is_bullish_h1 = last_h1['close'] > last_h1['ema50'] > last_h1['ema200']
            else:
                is_bullish_h1 = False
            
            df_h4 = await self.get_klines(symbol, HIGHEST_TIMEFRAME, 100)
            if df_h4 is not None and len(df_h4) > 50:
                df_h4 = TechnicalIndicators.add_basic_indicators(df_h4)
                last_h4 = df_h4.iloc[-1]
                is_bullish_h4 = last_h4['close'] > last_h4['ema50'] > last_h4['ema200']
            else:
                is_bullish_h4 = False
            
            result = (is_bullish_h1, is_bullish_h4)
            await self.cache_manager.set(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking multi-timeframe trend for {symbol}: {e}")
            return False, False
    
    def calculate_signal_strength(self, signal_type: SignalType, df: pd.DataFrame, 
                                   h1_trend: bool, h4_trend: bool) -> int:
        """Calculate signal strength with weighted scoring"""
        strength = 0
        last = df.iloc[-1]
        
        if signal_type == SignalType.LONG:
            if h1_trend:
                strength += 1
            if h4_trend:
                strength += 1
        else:
            if not h1_trend:
                strength += 1
            if not h4_trend:
                strength += 1
        
        if not last['squeeze_on'] and last['squeeze_count'] > 3:
            strength += 1.5
        
        if last['volume'] > last['volume_sma'] * 1.5:
            strength += 1
        
        if signal_type == SignalType.LONG and last.get('rsi_divergence_bullish', False):
            strength += 1.5
        elif signal_type == SignalType.SHORT and last.get('rsi_divergence_bearish', False):
            strength += 1.5
        
        if signal_type == SignalType.LONG:
            if 40 < last['rsi'] < 60:
                strength += 0.5
        else:
            if 40 < last['rsi'] < 60:
                strength += 0.5
        
        return min(5, max(1, int(round(strength))))
    
    async def check_signal(self, symbol: str) -> Optional[Signal]:
        """Enhanced signal checking with all optimizations"""
        try:
            if symbol in self.sent_signals:
                if time.time() - self.sent_signals[symbol] < SIGNAL_COOLDOWN:
                    return None
            
            df = await self.get_klines(symbol, TIMEFRAME, 200)
            if df is None or len(df) < 200:
                return None
            
            df = TechnicalIndicators.add_basic_indicators(df)
            df = TechnicalIndicators.add_ttm_squeeze(df)
            df = TechnicalIndicators.find_swing_highs_lows(df)
            
            h1_trend, h4_trend = await self.check_multi_timeframe_trend(symbol)
            
            df = TechnicalIndicators.check_rsi_divergence(df)
            
            last = df.iloc[-1]
            
            if pd.isna(last[['close', 'atr', 'ema50', 'ema200']]).any():
                return None
            
            entry = last['close']
            atr = last['atr']
            
            atr_percent = (atr / entry) * 100
            if atr_percent < MIN_ATR_PERCENT or atr_percent > MAX_ATR_PERCENT:
                return None
            
            risk_amount = CAPITAL * (RISK_PERCENT / 100)
            
            long_conditions = {
                'trend': last['ema50'] > last['ema200'],
                'price_above_ema': last['close'] > last['ema50'],
                'rsi_not_overbought': last['rsi'] < 70,
                'rsi_not_weak': last['rsi'] > 40,
                'volume_confirm': last['volume'] > last['volume_sma'],
                'swing_break': last['broke_swing_high'],
                'not_near_resistance': last['dist_to_swing_high'] > 1.0,
                'squeeze_release': not last['squeeze_on'] and last['squeeze_count'] > 0,
                'higher_tf_trend': h1_trend or h4_trend
            }
            
            short_conditions = {
                'trend': last['ema50'] < last['ema200'],
                'price_below_ema': last['close'] < last['ema50'],
                'rsi_not_oversold': last['rsi'] > 30,
                'rsi_not_strong': last['rsi'] < 60,
                'volume_confirm': last['volume'] > last['volume_sma'],
                'swing_break': last['broke_swing_low'],
                'not_near_support': last['dist_to_swing_low'] > 1.0,
                'squeeze_release': not last['squeeze_on'] and last['squeeze_count'] > 0,
                'higher_tf_trend': not (h1_trend or h4_trend)
            }
            
            long_score = sum(long_conditions.values())
            short_score = sum(short_conditions.values())
            min_conditions = 6
            
            if long_score >= min_conditions:
                sl = entry - (atr * 1.5)
                tp = entry + (atr * RR_RATIO * 1.5)
                position_size = risk_amount / (entry - sl)
                strength = self.calculate_signal_strength(SignalType.LONG, df, h1_trend, h4_trend)
                
                return Signal(
                    type=SignalType.LONG,
                    symbol=symbol,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    size=position_size,
                    rr=RR_RATIO,
                    atr=atr,
                    atr_percent=atr_percent,
                    rsi=last['rsi'],
                    strength=strength,
                    conditions_met=long_score,
                    total_conditions=len(long_conditions),
                    timestamp=datetime.now()
                )
            
            elif short_score >= min_conditions:
                sl = entry + (atr * 1.5)
                tp = entry - (atr * RR_RATIO * 1.5)
                position_size = risk_amount / (sl - entry)
                strength = self.calculate_signal_strength(SignalType.SHORT, df, h1_trend, h4_trend)
                
                return Signal(
                    type=SignalType.SHORT,
                    symbol=symbol,
                    entry=entry,
                    sl=sl,
                    tp=tp,
                    size=position_size,
                    rr=RR_RATIO,
                    atr=atr,
                    atr_percent=atr_percent,
                    rsi=last['rsi'],
                    strength=strength,
                    conditions_met=short_score,
                    total_conditions=len(short_conditions),
                    timestamp=datetime.now()
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking signal for {symbol}: {e}")
            return None
    
    def format_signal_message(self, signal: Signal) -> str:
        """Format signal message"""
        emoji = "🟢" if signal.type == SignalType.LONG else "🔴"
        
        if signal.type == SignalType.LONG:
            potential_profit = (signal.tp - signal.entry) * signal.size
        else:
            potential_profit = (signal.entry - signal.tp) * signal.size
        
        strength_emoji = "💪" * signal.strength
        strength_text = {
            1: "WEAK", 2: "MODERATE", 3: "STRONG",
            4: "VERY STRONG", 5: "EXCEPTIONAL"
        }.get(signal.strength, "UNKNOWN")
        
        message = f"""
{emoji} <b>{signal.type.value} SIGNAL - {signal.symbol}</b>
━━━━━━━━━━━━━━━━━━━
💰 <b>Entry:</b> {signal.entry:.4f}
🛑 <b>Stop Loss:</b> {signal.sl:.4f}
✅ <b>Take Profit:</b> {signal.tp:.4f}
📊 <b>Position Size:</b> {signal.size:.4f}
📈 <b>Risk/Reward:</b> 1:{signal.rr}
💵 <b>Potential Profit:</b> ${potential_profit:.2f}

<b>Technical Analysis:</b>
📉 <b>ATR:</b> {signal.atr:.4f} ({signal.atr_percent:.2f}%)
📊 <b>RSI:</b> {signal.rsi:.1f}
🎯 <b>Signal Strength:</b> {signal.strength}/5 - {strength_text} {strength_emoji}
✅ <b>Conditions Met:</b> {signal.conditions_met}/{signal.total_conditions}

🆔 <b>Signal ID:</b> <code>{signal.signal_id}</code>
⏰ <b>Time:</b> {signal.timestamp.strftime('%H:%M:%S')}
━━━━━━━━━━━━━━━━━━━
        """
        return message
    
    async def process_and_broadcast_signals(self):
        """Main processing method"""
        try:
            start_time = time.time()
            self.metrics['total_scans'] += 1
            
            symbols = await self.get_top_symbols()
            if not symbols:
                logger.warning("No symbols found")
                return
            
            logger.info(f"Scanning {len(symbols)} symbols...")
            
            semaphore = asyncio.Semaphore(MAX_WORKERS)
            
            async def process_with_semaphore(symbol):
                async with semaphore:
                    return await self.check_signal(symbol)
            
            tasks = [process_with_semaphore(symbol) for symbol in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            signals = []
            for result in results:
                if isinstance(result, Signal):
                    signals.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Error processing symbol: {result}")
            
            signals.sort(key=lambda x: x.strength, reverse=True)
            
            for signal in signals:
                if signal.symbol not in self.sent_signals or \
                   time.time() - self.sent_signals[signal.symbol] >= SIGNAL_COOLDOWN:
                    
                    message = self.format_signal_message(signal)
                    
                    await self.broadcast_to_users(message, min_strength=signal.strength)
                    
                    self.sent_signals[signal.symbol] = time.time()
                    self.signals_history.append(signal)
                    self.signals_generated += 1
                    self.last_signal_time = datetime.now()
                    
                    users = await self.user_manager.get_active_users()
                    for user_id, _ in users:
                        await self.user_manager.increment_signals_received(user_id)
                    
                    logger.info(f"Signal broadcast for {signal.symbol} (Strength: {signal.strength}/5)")
                    await asyncio.sleep(0.2)
            
            if datetime.now() - self.last_status_broadcast > timedelta(minutes=30):
                await self.broadcast_status_update()
                self.last_status_broadcast = datetime.now()
            
            elapsed = time.time() - start_time
            self.metrics['successful_scans'] += 1
            self.metrics['avg_scan_time'] = (self.metrics['avg_scan_time'] * (self.metrics['successful_scans'] - 1) + elapsed) / self.metrics['successful_scans']
            
            await self.cache_manager.clear_expired()
            
            if signals:
                logger.info(f"Scan complete. Found {len(signals)} signals in {elapsed:.2f}s")
            
        except Exception as e:
            logger.error(f"Error in process_and_broadcast_signals: {e}")
            self.metrics['failed_scans'] += 1
    
    async def broadcast_status_update(self):
        """Send enhanced status update"""
        users_count = await self.user_manager.get_user_count()
        
        last_signal_str = "Never"
        if self.last_signal_time:
            last_signal_str = self.last_signal_time.strftime('%H:%M:%S')
        
        signals_24h = sum(1 for s in self.signals_history 
                         if s.timestamp > datetime.now() - timedelta(days=1))
        
        total_cache = self.metrics['cache_hits'] + self.metrics['cache_misses']
        cache_hit_rate = (self.metrics['cache_hits'] / total_cache * 100) if total_cache > 0 else 0
        
        message = f"""
🤖 <b>Bot Status Update</b>
━━━━━━━━━━━━━━━━━━━
✅ Running normally
👥 Active users: {users_count}
📊 Timeframe: {TIMEFRAME} (with {HIGHER_TIMEFRAME}/{HIGHEST_TIMEFRAME})
💰 Risk per trade: {RISK_PERCENT}%
📈 Top {TOP_VOLUME} volume pairs
⏱️ Scan interval: {SCAN_INTERVAL}s

<b>Performance Metrics:</b>
📊 Signals today: {signals_24h}
⏰ Last signal: {last_signal_str}
⚡ Avg scan time: {self.metrics['avg_scan_time']:.2f}s
💾 Cache hit rate: {cache_hit_rate:.1f}%

<b>Active Techniques:</b>
• Multi-timeframe filter ✅
• TTM Squeeze ✅
• S/R levels ✅
• RSI Divergence ✅
• Volatility filter ✅
━━━━━━━━━━━━━━━━━━━
        """
        await self.broadcast_to_users(message)
    
    async def scanning_task(self):
        """Background scanning task"""
        while self.scanning:
            try:
                await self.process_and_broadcast_signals()
            except Exception as e:
                logger.error(f"Error in scanning task: {e}")
            
            if self.signals_generated % 10 == 0:
                await self._save_signal_history()
            
            for _ in range(SCAN_INTERVAL):
                if not self.scanning:
                    break
                await asyncio.sleep(1)
        
        await self._save_signal_history()
        logger.info("Scanning task stopped")
    
    def stop_scanning(self):
        """Stop the scanning task"""
        self.scanning = False

# =========================
# TELEGRAM COMMAND HANDLERS
# =========================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    user_id = str(user.id)
    
    scanner = context.bot_data.get('scanner')
    if scanner:
        await scanner.user_manager.add_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name
        )
    
    welcome_message = f"""
🎉 <b>Welcome to Advanced Crypto Signal Bot!</b>

Hello {user.first_name}! I'm your automated crypto trading signal scanner with advanced techniques.

<b>Features:</b>
• Multi-timeframe analysis ({TIMEFRAME} + {HIGHER_TIMEFRAME}/{HIGHEST_TIMEFRAME})
• TTM Squeeze for compression detection
• Support/Resistance levels
• RSI Divergence detection
• Volatility filtering
• Signal strength scoring (1-5)

<b>Commands:</b>
/start - Show this welcome message
/status - Check bot status
/stats - View scanning statistics
/settings - Adjust your preferences
/lastsignal - Get the last signal
/stop - Stop receiving signals
/help - Show help message

You'll automatically receive high-probability trading signals!
    """
    
    await update.message.reply_text(welcome_message, parse_mode='HTML')
    await status_command(update, context)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner:
        await scanner.user_manager.update_last_active(user_id)
        
        users_count = await scanner.user_manager.get_user_count()
        message = f"""
📊 <b>Bot Status</b>
━━━━━━━━━━━━━━━━━━━
🟢 Status: <b>Active</b>
👥 Active Users: {users_count}
📈 Scanning: Top {TOP_VOLUME} pairs
⏱️ Interval: {SCAN_INTERVAL}s
📊 Timeframes: {TIMEFRAME} + {HIGHER_TIMEFRAME}/{HIGHEST_TIMEFRAME}
💰 Risk: {RISK_PERCENT}% per trade
━━━━━━━━━━━━━━━━━━━
        """
        await update.message.reply_text(message, parse_mode='HTML')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner:
        await scanner.user_manager.update_last_active(user_id)
        
        signals_24h = sum(1 for s in scanner.signals_history 
                         if s.timestamp > datetime.now() - timedelta(days=1))
        signals_7d = sum(1 for s in scanner.signals_history 
                        if s.timestamp > datetime.now() - timedelta(days=7))
        
        strength_dist = {}
        for s in scanner.signals_history:
            strength_dist[s.strength] = strength_dist.get(s.strength, 0) + 1
        
        dist_text = ", ".join([f"{k}: {v}" for k, v in sorted(strength_dist.items())])
        
        message = f"""
📈 <b>Bot Statistics</b>
━━━━━━━━━━━━━━━━━━━
📊 Signals (24h): {signals_24h}
📊 Signals (7d): {signals_7d}
👥 Active users: {await scanner.user_manager.get_user_count()}
💹 Total users all time: {len(scanner.user_manager.active_users)}

<b>Signal Strength Distribution:</b>
{dist_text if dist_text else "No signals yet"}

<b>Performance:</b>
⚡ Avg scan time: {scanner.metrics['avg_scan_time']:.2f}s
✅ Successful scans: {scanner.metrics['successful_scans']}
💾 Cache hit rate: {(scanner.metrics['cache_hits'] / (scanner.metrics['cache_hits'] + scanner.metrics['cache_misses']) * 100) if scanner.metrics['cache_hits'] + scanner.metrics['cache_misses'] > 0 else 0:.1f}%
━━━━━━━━━━━━━━━━━━━
        """
        await update.message.reply_text(message, parse_mode='HTML')

async def lastsignal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /lastsignal command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner and scanner.signals_history:
        await scanner.user_manager.update_last_active(user_id)
        
        last_signal = scanner.signals_history[-1]
        message = scanner.format_signal_message(last_signal)
        
        await update.message.reply_text(message, parse_mode='HTML')
    else:
        await update.message.reply_text("No signals generated yet.")

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /settings command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner:
        await scanner.user_manager.update_last_active(user_id)
        
        user_data = scanner.user_manager.active_users.get(user_id, {})
        prefs = user_data.get('preferences', {})
        min_strength = prefs.get('min_strength', 3)
        receive_signals = prefs.get('receive_signals', True)
        
        settings_message = f"""
⚙️ <b>Your Settings</b>
━━━━━━━━━━━━━━━━━━━
📊 <b>Minimum signal strength:</b> {min_strength}/5
🔔 <b>Receive signals:</b> {'✅ Yes' if receive_signals else '❌ No'}

<b>To change settings:</b>
/minstrength [1-5] - Set minimum signal strength
/toggle - Toggle signal reception on/off
━━━━━━━━━━━━━━━━━━━
        """
        await update.message.reply_text(settings_message, parse_mode='HTML')

async def minstrength_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /minstrength command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner and context.args:
        try:
            strength = int(context.args[0])
            if 1 <= strength <= 5:
                if user_id in scanner.user_manager.active_users:
                    prefs = scanner.user_manager.active_users[user_id].setdefault('preferences', {})
                    prefs['min_strength'] = strength
                    await scanner.user_manager.save_users()
                    
                    await update.message.reply_text(
                        f"✅ Minimum signal strength set to {strength}/5",
                        parse_mode='HTML'
                    )
                else:
                    await update.message.reply_text("User not found")
            else:
                await update.message.reply_text("Please provide a number between 1 and 5")
        except ValueError:
            await update.message.reply_text("Please provide a valid number")
    else:
        await update.message.reply_text("Usage: /minstrength [1-5]")

async def toggle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /toggle command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner and user_id in scanner.user_manager.active_users:
        prefs = scanner.user_manager.active_users[user_id].setdefault('preferences', {})
        current = prefs.get('receive_signals', True)
        prefs['receive_signals'] = not current
        await scanner.user_manager.save_users()
        
        status = "enabled" if not current else "disabled"
        await update.message.reply_text(
            f"✅ Signal reception {status}",
            parse_mode='HTML'
        )

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner:
        await scanner.user_manager.remove_user(user_id)
    
    message = """
🛑 <b>You have been unsubscribed</b>

You will no longer receive signals from this bot.
To start receiving signals again, use /start
    """
    await update.message.reply_text(message, parse_mode='HTML')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    scanner = context.bot_data.get('scanner')
    if scanner:
        await scanner.user_manager.update_last_active(str(update.effective_user.id))
    
    help_message = f"""
❓ <b>Help & Advanced Strategy</b>

<b>Available Commands:</b>
/start - Start bot and receive signals
/status - Check current bot status
/stats - View bot statistics
/lastsignal - Get the last signal
/settings - View/change your settings
/stop - Stop receiving signals
/help - Show this help message

<b>Advanced Strategy (1:2 R:R):</b>
1️⃣ <b>Multi-Timeframe Filter</b> - M15 signals aligned with H1/H4 trend
2️⃣ <b>TTM Squeeze</b> - Identifies compression before breakouts
3️⃣ <b>Support/Resistance</b> - Trade breakouts of recent swing points
4️⃣ <b>RSI Divergence</b> - Catches trend reversals early
5️⃣ <b>Volatility Filter</b> - Skip coins with ATR < {MIN_ATR_PERCENT}% or > {MAX_ATR_PERCENT}%

<b>Signal Strength Scoring (1-5):</b>
1 - Weak | 2 - Moderate | 3 - Strong | 4 - Very Strong | 5 - Exceptional

<b>Risk Warning:</b>
⚠️ Trading carries high risk. Never risk more than you can afford to lose.
    """
    await update.message.reply_text(help_message, parse_mode='HTML')

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in telegram bot"""
    logger.error(f"Telegram error: {context.error}")
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "An error occurred. Please try again later."
            )
    except:
        pass

async def run_scanner_background(scanner: CryptoScanner):
    """Run scanner in background"""
    try:
        await scanner.scanning_task()
    except asyncio.CancelledError:
        logger.info("Scanner task cancelled")
    except Exception as e:
        logger.error(f"Scanner task error: {e}")

# =========================
# MAIN FUNCTION
# =========================

async def main_async():
    """Main async function"""
    scanner = None
    try:
        scanner = CryptoScanner()
        
        application = Application.builder().token(TELEGRAM_TOKEN).build()
        application.bot_data['scanner'] = scanner
        
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("status", status_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(CommandHandler("lastsignal", lastsignal_command))
        application.add_handler(CommandHandler("settings", settings_command))
        application.add_handler(CommandHandler("minstrength", minstrength_command))
        application.add_handler(CommandHandler("toggle", toggle_command))
        application.add_handler(CommandHandler("stop", stop_command))
        application.add_handler(CommandHandler("help", help_command))
        
        application.add_error_handler(error_handler)
        
        startup_message = """
🚀 <b>Advanced Bot Started!</b>

The crypto signal bot is now running with all optimization techniques:
✅ Multi-timeframe analysis
✅ TTM Squeeze detection
✅ S/R level filtering
✅ RSI divergence detection
✅ Volatility filtering

Use /settings to customize your preferences.
You'll receive high-probability signals when detected.
        """
        await scanner.broadcast_to_users(startup_message)
        
        scan_task = asyncio.create_task(run_scanner_background(scanner))
        
        logger.info("Starting Telegram bot...")
        await application.initialize()
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
        
        while scanner.scanning:
            await asyncio.sleep(1)
        
        logger.info("Shutting down...")
        scan_task.cancel()
        try:
            await scan_task
        except asyncio.CancelledError:
            pass
        
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scanner:
            scanner.stop_scanning()
            await scanner._save_signal_history()

def main():
    """Main entry point"""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()