import pandas as pd
import ta
import time
import logging
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
import sys
import asyncio

# =========================
# CONFIG
# =========================
BINANCE_API_KEY = "YOUR_BINANCE_KEY"
BINANCE_SECRET = "YOUR_BINANCE_SECRET"

TELEGRAM_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"

# Timeframes
TIMEFRAME = "15m"
HIGHER_TIMEFRAME = "1h"  # For multi-timeframe analysis
HIGHEST_TIMEFRAME = "4h"  # Additional confirmation

RISK_PERCENT = 1.0       # 1% risk per trade
RR_RATIO = 2.0           # Risk Reward 1:2
CAPITAL = 1000.0         # 1000 USDT
TOP_VOLUME = 50          # Top 50 volume pairs
SCAN_INTERVAL = 300       # Scan every 5 minutes
MIN_VOLUME_USDT = 100000 # Minimum 24h volume in USDT
MAX_WORKERS = 10         # Max threads for parallel processing

# Volatility filter
MIN_ATR_PERCENT = 0.5    # Minimum ATR percentage (0.5% move minimum)
MAX_ATR_PERCENT = 5.0    # Maximum ATR percentage (avoid too volatile)

# File to store active users
ACTIVE_USERS_FILE = "active_users.json"

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class UserManager:
    """Manage active Telegram users"""
    # ... (keep existing UserManager class from previous version)
    def __init__(self, storage_file: str):
        self.storage_file = storage_file
        self.active_users: Dict[str, dict] = {}
        self.load_users()
    
    def load_users(self):
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as f:
                    self.active_users = json.load(f)
                logger.info(f"Loaded {len(self.active_users)} active users")
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            self.active_users = {}
    
    def save_users(self):
        try:
            with open(self.storage_file, 'w') as f:
                json.dump(self.active_users, f, indent=2)
            logger.info(f"Saved {len(self.active_users)} active users")
        except Exception as e:
            logger.error(f"Error saving users: {e}")
    
    def add_user(self, user_id: str, username: str = None, first_name: str = None):
        self.active_users[user_id] = {
            'username': username,
            'first_name': first_name,
            'joined_at': datetime.now().isoformat(),
            'last_active': datetime.now().isoformat(),
            'active': True,
            'preferences': {
                'receive_signals': True,
                'receive_updates': True
            }
        }
        self.save_users()
        logger.info(f"User {user_id} added/updated")
    
    def remove_user(self, user_id: str):
        if user_id in self.active_users:
            self.active_users[user_id]['active'] = False
            self.active_users[user_id]['left_at'] = datetime.now().isoformat()
            self.save_users()
            logger.info(f"User {user_id} deactivated")
    
    def get_active_users(self) -> List[str]:
        return [
            uid for uid, data in self.active_users.items() 
            if data.get('active', False) and data.get('preferences', {}).get('receive_signals', True)
        ]
    
    def update_last_active(self, user_id: str):
        if user_id in self.active_users:
            self.active_users[user_id]['last_active'] = datetime.now().isoformat()
            self.save_users()
    
    def get_user_count(self) -> int:
        return len(self.get_active_users())


class TechnicalIndicators:
    """Enhanced technical indicators with advanced techniques"""
    
    @staticmethod
    def add_basic_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """Add basic indicators (EMAs, ATR, RSI)"""
        # EMAs
        df['ema50'] = ta.trend.ema_indicator(df['close'], window=50)
        df['ema200'] = ta.trend.ema_indicator(df['close'], window=200)
        
        # ATR
        df['atr'] = ta.volatility.average_true_range(
            df['high'], df['low'], df['close'], window=14
        )
        
        # RSI
        df['rsi'] = ta.momentum.rsi(df['close'], window=14)
        
        # Volume SMA
        df['volume_sma'] = df['volume'].rolling(window=20).mean()
        
        return df
    
    @staticmethod
    def add_bollinger_bands(df: pd.DataFrame, window: int = 20, std: int = 2) -> pd.DataFrame:
        """Add Bollinger Bands"""
        bb = ta.volatility.BollingerBands(
            df['close'], window=window, window_dev=std
        )
        df['bb_high'] = bb.bollinger_hband()
        df['bb_low'] = bb.bollinger_lband()
        df['bb_mid'] = bb.bollinger_mavg()
        df['bb_width'] = (df['bb_high'] - df['bb_low']) / df['bb_mid']
        df['bb_percent'] = (df['close'] - df['bb_low']) / (df['bb_high'] - df['bb_low'])
        
        return df
    
    @staticmethod
    def add_keltner_channels(df: pd.DataFrame, window: int = 20, atr_mult: float = 1.5) -> pd.DataFrame:
        """Add Keltner Channels"""
        df['kc_mid'] = ta.trend.ema_indicator(df['close'], window=window)
        df['kc_atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=10)
        df['kc_upper'] = df['kc_mid'] + (df['kc_atr'] * atr_mult)
        df['kc_lower'] = df['kc_mid'] - (df['kc_atr'] * atr_mult)
        
        return df
    
    @staticmethod
    def add_ttm_squeeze(df: pd.DataFrame) -> pd.DataFrame:
        """
        TTM Squeeze indicator - identifies compression/expansion
        Returns True when market is squeezing (consolidating)
        """
        # Calculate Bollinger Bands and Keltner Channels
        df = TechnicalIndicators.add_bollinger_bands(df)
        df = TechnicalIndicators.add_keltner_channels(df)
        
        # Squeeze condition: Bollinger Bands inside Keltner Channels
        df['squeeze_on'] = (df['bb_low'] > df['kc_lower']) & (df['bb_high'] < df['kc_upper'])
        
        # Squeeze counter (how long has squeeze been on)
        squeeze_counter = []
        count = 0
        for squeeze in df['squeeze_on']:
            if squeeze:
                count += 1
            else:
                count = 0
            squeeze_counter.append(count)
        
        df['squeeze_count'] = squeeze_counter
        
        # Momentum oscillator (for squeeze release detection)
        df['squeeze_momentum'] = ta.momentum.awesome_oscillator(df['high'], df['low'])
        
        return df
    
    @staticmethod
    def find_swing_highs_lows(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
        """
        Find swing highs and lows using Donchian Channels / Fractals
        """
        # Donchian Channels for swing points
        df['swing_high'] = df['high'].rolling(window=window).max()
        df['swing_low'] = df['low'].rolling(window=window).min()
        
        # Identify if current price broke recent swing high/low
        df['broke_swing_high'] = df['close'] > df['swing_high'].shift(1)
        df['broke_swing_low'] = df['close'] < df['swing_low'].shift(1)
        
        # Find nearest swing high/low (for S/R levels)
        df['nearest_swing_high'] = df['swing_high'].rolling(window=20).max()
        df['nearest_swing_low'] = df['swing_low'].rolling(window=20).min()
        
        # Distance to nearest swing levels
        df['dist_to_swing_high'] = (df['nearest_swing_high'] - df['close']) / df['close'] * 100
        df['dist_to_swing_low'] = (df['close'] - df['nearest_swing_low']) / df['close'] * 100
        
        return df
    
    @staticmethod
    def check_rsi_divergence(df: pd.DataFrame, window: int = 14) -> Dict:
        """
        Check for RSI divergence
        Returns dict with divergence signals
        """
        # Get last 20 candles for divergence check
        recent = df.tail(20)
        
        # Find local highs/lows in price and RSI
        price_highs = []
        price_lows = []
        rsi_highs = []
        rsi_lows = []
        
        for i in range(2, len(recent) - 2):
            # Price high
            if (recent['high'].iloc[i] > recent['high'].iloc[i-1] and 
                recent['high'].iloc[i] > recent['high'].iloc[i-2] and
                recent['high'].iloc[i] > recent['high'].iloc[i+1] and 
                recent['high'].iloc[i] > recent['high'].iloc[i+2]):
                price_highs.append((i, recent['high'].iloc[i]))
                rsi_highs.append((i, recent['rsi'].iloc[i]))
            
            # Price low
            if (recent['low'].iloc[i] < recent['low'].iloc[i-1] and 
                recent['low'].iloc[i] < recent['low'].iloc[i-2] and
                recent['low'].iloc[i] < recent['low'].iloc[i+1] and 
                recent['low'].iloc[i] < recent['low'].iloc[i+2]):
                price_lows.append((i, recent['low'].iloc[i]))
                rsi_lows.append((i, recent['rsi'].iloc[i]))
        
        divergence = {
            'bearish': False,  # Price higher high, RSI lower high
            'bullish': False,  # Price lower low, RSI higher low
            'strength': 0
        }
        
        # Check bearish divergence (for short signals)
        if len(price_highs) >= 2 and len(rsi_highs) >= 2:
            last_price_high = price_highs[-1][1]
            prev_price_high = price_highs[-2][1]
            last_rsi_high = rsi_highs[-1][1]
            prev_rsi_high = rsi_highs[-2][1]
            
            if last_price_high > prev_price_high and last_rsi_high < prev_rsi_high:
                divergence['bearish'] = True
                divergence['strength'] += 1
        
        # Check bullish divergence (for long signals)
        if len(price_lows) >= 2 and len(rsi_lows) >= 2:
            last_price_low = price_lows[-1][1]
            prev_price_low = price_lows[-2][1]
            last_rsi_low = rsi_lows[-1][1]
            prev_rsi_low = rsi_lows[-2][1]
            
            if last_price_low < prev_price_low and last_rsi_low > prev_rsi_low:
                divergence['bullish'] = True
                divergence['strength'] += 1
        
        return df
    
    @staticmethod
    def calculate_atr_percent(df: pd.DataFrame) -> float:
        """Calculate ATR as percentage of price"""
        last = df.iloc[-1]
        return (last['atr'] / last['close']) * 100


class CryptoScanner:
    """Main scanner class with advanced techniques"""
    
    def __init__(self):
        """Initialize connections and validate configuration"""
        self._validate_config()
        
        # Initialize Binance client
        self.client = Client(BINANCE_API_KEY, BINANCE_SECRET)
        
        # Initialize Telegram bot
        self.bot = Bot(token=TELEGRAM_TOKEN)
        
        # Initialize user manager
        self.user_manager = UserManager(ACTIVE_USERS_FILE)
        
        # Track sent signals to avoid duplicates
        self.sent_signals = {}
        self.SIGNAL_COOLDOWN = 3600  # Don't repeat same symbol for 1 hour
        
        # Track last broadcast time for different message types
        self.last_status_broadcast = datetime.min
        
        # Control flag for scanning loop
        self.scanning = True
        
        # Performance tracking
        self.signals_generated = 0
        self.last_signal_time = None
        
        logger.info("CryptoScanner initialized successfully")
    
    def _validate_config(self):
        """Validate configuration parameters"""
        if BINANCE_API_KEY == "YOUR_BINANCE_KEY" or BINANCE_SECRET == "YOUR_BINANCE_SECRET":
            logger.error("Please set your Binance API credentials")
            sys.exit(1)
        
        if TELEGRAM_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
            logger.error("Please set your Telegram Bot token")
            sys.exit(1)
    
    async def broadcast_to_users(self, message: str, parse_mode: str = 'HTML'):
        """Broadcast message to all active users"""
        active_users = self.user_manager.get_active_users()
        success_count = 0
        fail_count = 0
        
        for user_id in active_users:
            try:
                await self.bot.send_message(
                    chat_id=int(user_id),
                    text=message,
                    parse_mode=parse_mode
                )
                success_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send to user {user_id}: {e}")
                fail_count += 1
                
                if "blocked" in str(e) or "deactivated" in str(e):
                    self.user_manager.remove_user(user_id)
        
        if success_count > 0:
            logger.info(f"Broadcast complete. Success: {success_count}, Failed: {fail_count}")
    
    def get_top_symbols(self) -> List[str]:
        """Get top volume USDT pairs from Binance Futures"""
        try:
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
            logger.info(f"Found {len(top_symbols)} top volume symbols")
            
            return top_symbols
            
        except Exception as e:
            logger.error(f"Error in get_top_symbols: {e}")
            return []
    
    def get_klines(self, symbol: str, timeframe: str = TIMEFRAME, limit: int = 200) -> Optional[pd.DataFrame]:
        """Get klines/candlestick data for a symbol"""
        try:
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
            df.dropna(inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting klines for {symbol} ({timeframe}): {e}")
            return None
    
    def check_multi_timeframe_trend(self, symbol: str) -> Tuple[bool, bool]:
        """
        Check trend on higher timeframes
        Returns (is_bullish_h1, is_bullish_h4)
        """
        try:
            # Get H1 data
            df_h1 = self.get_klines(symbol, HIGHER_TIMEFRAME, 100)
            if df_h1 is not None and len(df_h1) > 50:
                df_h1 = TechnicalIndicators.add_basic_indicators(df_h1)
                last_h1 = df_h1.iloc[-1]
                is_bullish_h1 = last_h1['close'] > last_h1['ema50'] > last_h1['ema200']
            else:
                is_bullish_h1 = False
            
            # Get H4 data
            df_h4 = self.get_klines(symbol, HIGHEST_TIMEFRAME, 100)
            if df_h4 is not None and len(df_h4) > 50:
                df_h4 = TechnicalIndicators.add_basic_indicators(df_h4)
                last_h4 = df_h4.iloc[-1]
                is_bullish_h4 = last_h4['close'] > last_h4['ema50'] > last_h4['ema200']
            else:
                is_bullish_h4 = False
            
            return is_bullish_h1, is_bullish_h4
            
        except Exception as e:
            logger.error(f"Error checking multi-timeframe trend for {symbol}: {e}")
            return False, False
    
    def calculate_signal_strength(self, signal_type: str, df: pd.DataFrame, 
                                   h1_trend: bool, h4_trend: bool) -> int:
        """
        Calculate signal strength based on multiple confirmations
        Returns score from 0-5
        """
        strength = 0
        last = df.iloc[-1]
        
        # 1. Multi-timeframe alignment (2 points)
        if signal_type == 'LONG':
            if h1_trend:
                strength += 1
            if h4_trend:
                strength += 1
        else:  # SHORT
            if not h1_trend:
                strength += 1
            if not h4_trend:
                strength += 1
        
        # 2. Squeeze release (1 point)
        if not last['squeeze_on'] and last['squeeze_count'] > 3:
            strength += 1
        
        # 3. Volume confirmation (1 point)
        if last['volume'] > last['volume_sma'] * 1.5:
            strength += 1
        
        # 4. RSI divergence (1 point)
        if signal_type == 'LONG' and last.get('rsi_divergence_bullish', False):
            strength += 1
        elif signal_type == 'SHORT' and last.get('rsi_divergence_bearish', False):
            strength += 1
        
        return strength
    
    def check_signal(self, symbol: str) -> Optional[Dict]:
        """Enhanced signal checking with all optimization techniques"""
        try:
            # Get data for multiple timeframes
            df = self.get_klines(symbol, TIMEFRAME, 200)
            if df is None or len(df) < 200:
                return None
            
            # Add all indicators
            df = TechnicalIndicators.add_basic_indicators(df)
            df = TechnicalIndicators.add_ttm_squeeze(df)
            df = TechnicalIndicators.find_swing_highs_lows(df)
            
            # Check multi-timeframe trend
            h1_trend, h4_trend = self.check_multi_timeframe_trend(symbol)
            
            # Check RSI divergence
            df = TechnicalIndicators.check_rsi_divergence(df)
            
            # Get last candle
            last = df.iloc[-1]
            prev = df.iloc[-2]
            
            # Validate data
            if pd.isna(last[['close', 'atr', 'ema50', 'ema200']]).any():
                return None
            
            entry = last['close']
            atr = last['atr']
            
            # Volatility filter (ATR %)
            atr_percent = (atr / entry) * 100
            if atr_percent < MIN_ATR_PERCENT or atr_percent > MAX_ATR_PERCENT:
                logger.debug(f"{symbol} skipped - ATR%: {atr_percent:.2f}%")
                return None
            
            risk_amount = CAPITAL * (RISK_PERCENT / 100)
            
            # LONG signal conditions with all techniques
            long_conditions = {
                'trend': last['ema50'] > last['ema200'],
                'price_above_ema': last['close'] > last['ema50'],
                'rsi_not_overbought': last['rsi'] < 70,
                'rsi_not_weak': last['rsi'] > 40,
                'volume_confirm': last['volume'] > last['volume_sma'],
                'swing_break': last['broke_swing_high'],  # Broke recent high
                'not_near_resistance': last['dist_to_swing_high'] > 1.0,  # >1% from resistance
                'squeeze_release': not last['squeeze_on'] and last['squeeze_count'] > 0,
                'higher_tf_trend': h1_trend or h4_trend
            }
            
            # SHORT signal conditions
            short_conditions = {
                'trend': last['ema50'] < last['ema200'],
                'price_below_ema': last['close'] < last['ema50'],
                'rsi_not_oversold': last['rsi'] > 30,
                'rsi_not_strong': last['rsi'] < 60,
                'volume_confirm': last['volume'] > last['volume_sma'],
                'swing_break': last['broke_swing_low'],  # Broke recent low
                'not_near_support': last['dist_to_swing_low'] > 1.0,  # >1% from support
                'squeeze_release': not last['squeeze_on'] and last['squeeze_count'] > 0,
                'higher_tf_trend': not (h1_trend or h4_trend)  # Lower timeframes bearish
            }
            
            # Count how many conditions are met
            long_score = sum(long_conditions.values())
            short_score = sum(short_conditions.values())
            
            # Require at least 6 out of 9 conditions for a valid signal
            min_conditions = 6
            
            if long_score >= min_conditions:
                # Calculate stops and targets
                sl = entry - (atr * 1.5)
                tp = entry + (atr * RR_RATIO * 1.5)
                position_size = risk_amount / (entry - sl)
                
                # Calculate signal strength
                strength = self.calculate_signal_strength('LONG', df, h1_trend, h4_trend)
                
                self.signals_generated += 1
                self.last_signal_time = datetime.now()
                
                return {
                    'type': 'LONG',
                    'symbol': symbol,
                    'entry': entry,
                    'sl': sl,
                    'tp': tp,
                    'size': position_size,
                    'rr': RR_RATIO,
                    'atr': atr,
                    'atr_percent': atr_percent,
                    'rsi': last['rsi'],
                    'strength': strength,
                    'conditions_met': long_score,
                    'total_conditions': len(long_conditions),
                    'timestamp': datetime.now()
                }
            
            elif short_score >= min_conditions:
                sl = entry + (atr * 1.5)
                tp = entry - (atr * RR_RATIO * 1.5)
                position_size = risk_amount / (sl - entry)
                
                strength = self.calculate_signal_strength('SHORT', df, h1_trend, h4_trend)
                
                self.signals_generated += 1
                self.last_signal_time = datetime.now()
                
                return {
                    'type': 'SHORT',
                    'symbol': symbol,
                    'entry': entry,
                    'sl': sl,
                    'tp': tp,
                    'size': position_size,
                    'rr': RR_RATIO,
                    'atr': atr,
                    'atr_percent': atr_percent,
                    'rsi': last['rsi'],
                    'strength': strength,
                    'conditions_met': short_score,
                    'total_conditions': len(short_conditions),
                    'timestamp': datetime.now()
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking signal for {symbol}: {e}")
            return None
    
    def format_signal_message(self, signal: Dict) -> str:
        """Format signal with enhanced information"""
        emoji = "🟢" if signal['type'] == 'LONG' else "🔴"
        
        # Calculate potential profit
        if signal['type'] == 'LONG':
            potential_profit = (signal['tp'] - signal['entry']) * signal['size']
        else:
            potential_profit = (signal['entry'] - signal['tp']) * signal['size']
        
        # Create strength indicator
        strength_emoji = "💪" * signal['strength']
        
        message = f"""
{emoji} <b>{signal['type']} SIGNAL - {signal['symbol']}</b>
━━━━━━━━━━━━━━━━━━━
💰 <b>Entry:</b> {signal['entry']:.4f}
🛑 <b>Stop Loss:</b> {signal['sl']:.4f}
✅ <b>Take Profit:</b> {signal['tp']:.4f}
📊 <b>Position Size:</b> {signal['size']:.4f}
📈 <b>Risk/Reward:</b> 1:{signal['rr']}
💵 <b>Potential Profit:</b> ${potential_profit:.2f}

<b>Technical Analysis:</b>
📉 <b>ATR:</b> {signal['atr']:.4f} ({signal['atr_percent']:.2f}%)
📊 <b>RSI:</b> {signal['rsi']:.1f}
🎯 <b>Signal Strength:</b> {signal['strength']}/5 {strength_emoji}
✅ <b>Conditions Met:</b> {signal['conditions_met']}/{signal['total_conditions']}

⏰ <b>Time:</b> {signal['timestamp'].strftime('%H:%M:%S')}
━━━━━━━━━━━━━━━━━━━
        """
        return message
    
    def can_send_signal(self, symbol: str) -> bool:
        """Check if we can send signal (avoid duplicates)"""
        if symbol in self.sent_signals:
            time_diff = time.time() - self.sent_signals[symbol]
            if time_diff < self.SIGNAL_COOLDOWN:
                return False
        return True
    
    async def process_and_broadcast_signals(self):
        """Main method to process and broadcast signals"""
        try:
            symbols = self.get_top_symbols()
            if not symbols:
                logger.warning("No symbols found")
                return
            
            logger.info(f"Scanning {len(symbols)} symbols...")
            
            # Process symbols in parallel
            signals = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_symbol = {
                    executor.submit(self.check_signal, symbol): symbol 
                    for symbol in symbols
                }
                
                for future in as_completed(future_to_symbol):
                    signal = future.result()
                    if signal:
                        signals.append(signal)
            
            # Sort signals by strength (highest first)
            signals.sort(key=lambda x: x['strength'], reverse=True)
            
            # Send signals to all active users
            for signal in signals:
                if self.can_send_signal(signal['symbol']):
                    message = self.format_signal_message(signal)
                    await self.broadcast_to_users(message)
                    
                    self.sent_signals[signal['symbol']] = time.time()
                    logger.info(f"Signal broadcast for {signal['symbol']} (Strength: {signal['strength']}/5)")
                    await asyncio.sleep(0.1)
            
            # Send periodic status update
            if datetime.now() - self.last_status_broadcast > timedelta(minutes=30):
                await self.broadcast_status_update()
                self.last_status_broadcast = datetime.now()
            
            if signals:
                logger.info(f"Scan complete. Found {len(signals)} signals")
            
        except Exception as e:
            logger.error(f"Error in process_and_broadcast_signals: {e}")
    
    async def broadcast_status_update(self):
        """Send enhanced status update"""
        active_users = self.user_manager.get_user_count()
        
        # Calculate some stats
        last_signal_str = "Never"
        if self.last_signal_time:
            last_signal_str = self.last_signal_time.strftime('%H:%M:%S')
        
        message = f"""
🤖 <b>Bot Status Update</b>
━━━━━━━━━━━━━━━━━━━
✅ Running normally
👥 Active users: {active_users}
📊 Timeframe: {TIMEFRAME} (with {HIGHER_TIMEFRAME}/{HIGHEST_TIMEFRAME} confirmation)
💰 Risk per trade: {RISK_PERCENT}%
📈 Top {TOP_VOLUME} volume pairs
⏱️ Scan interval: {SCAN_INTERVAL}s
📊 Signals today: {self.signals_generated}
⏰ Last signal: {last_signal_str}

<b>Techniques Active:</b>
• Multi-timeframe filter ✅
• TTM Squeeze ✅
• S/R levels ✅
• RSI Divergence ✅
• Volatility filter ✅
━━━━━━━━━━━━━━━━━━━
        """
        await self.broadcast_to_users(message)
    
    async def scanning_task(self):
        """Background task for scanning"""
        while self.scanning:
            try:
                await self.process_and_broadcast_signals()
            except Exception as e:
                logger.error(f"Error in scanning task: {e}")
            
            for _ in range(SCAN_INTERVAL):
                if not self.scanning:
                    break
                await asyncio.sleep(1)
    
    def stop_scanning(self):
        """Stop the scanning task"""
        self.scanning = False


# =========================
# TELEGRAM COMMAND HANDLERS (same as before)
# =========================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    user_id = str(user.id)
    
    scanner = context.bot_data.get('scanner')
    if scanner:
        scanner.user_manager.add_user(
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
        scanner.user_manager.update_last_active(user_id)
        
        active_users = scanner.user_manager.get_user_count()
        message = f"""
📊 <b>Bot Status</b>
━━━━━━━━━━━━━━━━━━━
🟢 Status: <b>Active</b>
👥 Active Users: {active_users}
📈 Scanning: Top {TOP_VOLUME} pairs
⏱️ Interval: {SCAN_INTERVAL}s
📊 Timeframes: {TIMEFRAME} + {HIGHER_TIMEFRAME}/{HIGHEST_TIMEFRAME}
💰 Risk: {RISK_PERCENT}% per trade
📈 Signals today: {scanner.signals_generated}
━━━━━━━━━━━━━━━━━━━
        """
        await update.message.reply_text(message, parse_mode='HTML')


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner:
        scanner.user_manager.update_last_active(user_id)
        
        total_signals_today = len([s for s in scanner.sent_signals.values() 
                                  if time.time() - s < 86400])
        
        message = f"""
📈 <b>Bot Statistics</b>
━━━━━━━━━━━━━━━━━━━
📊 Signals today: {total_signals_today}
👥 Active users: {scanner.user_manager.get_user_count()}
💹 Total users all time: {len(scanner.user_manager.active_users)}
⏰ Last signal: {'Never' if not scanner.last_signal_time else 
                 scanner.last_signal_time.strftime('%H:%M:%S')}

<b>Advanced Features:</b>
• Multi-timeframe: ✅ Active
• TTM Squeeze: ✅ Active
• S/R Levels: ✅ Active
• RSI Divergence: ✅ Active
• Volatility Filter: ✅ Active
━━━━━━━━━━━━━━━━━━━
        """
        await update.message.reply_text(message, parse_mode='HTML')


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner:
        scanner.user_manager.remove_user(user_id)
    
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
        scanner.user_manager.update_last_active(str(update.effective_user.id))
    
    help_message = f"""
❓ <b>Help & Advanced Strategy</b>

<b>Available Commands:</b>
/start - Start bot and receive signals
/status - Check current bot status
/stats - View bot statistics
/stop - Stop receiving signals
/help - Show this help message

<b>Advanced Strategy (1:2 R:R, targeting 55-65% win rate):</b>

1️⃣ <b>Multi-Timeframe Filter</b>
   • M15 signals aligned with H1/H4 trend
   • Reduces false signals

2️⃣ <b>TTM Squeeze</b>
   • Identifies compression before breakouts
   • Enters on squeeze release

3️⃣ <b>Support/Resistance</b>
   • Only trade breakouts of recent swing points
   • Avoid trading near major S/R levels

4️⃣ <b>RSI Divergence</b>
   • Catches trend reversals early
   • High probability entry signals

5️⃣ <b>Volatility Filter</b>
   • Skip coins with ATR < {MIN_ATR_PERCENT}% or > {MAX_ATR_PERCENT}%
   • Ensures enough movement for profit

<b>Signal Strength Scoring (1-5):</b>
• Higher score = more confirmations
• Focus on 4-5 strength signals

<b>Risk Warning:</b>
⚠️ Trading carries high risk. Never risk more than you can afford to lose.
    """
    await update.message.reply_text(help_message, parse_mode='HTML')


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in telegram bot"""
    logger.error(f"Telegram error: {context.error}")


async def run_scanner_background(scanner: CryptoScanner):
    """Run scanner in background"""
    await scanner.scanning_task()


async def main_async():
    """Main async function"""
    scanner = CryptoScanner()
    
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.bot_data['scanner'] = scanner
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("help", help_command))
    
    application.add_error_handler(error_handler)
    
    # Send startup message
    startup_message = """
🚀 <b>Advanced Bot Started!</b>

The crypto signal bot is now running with all optimization techniques:
✅ Multi-timeframe analysis
✅ TTM Squeeze detection
✅ S/R level filtering
✅ RSI divergence detection
✅ Volatility filtering

You'll receive high-probability signals when detected.
    """
    await scanner.broadcast_to_users(startup_message)
    
    # Start background scanning
    asyncio.create_task(run_scanner_background(scanner))
    
    logger.info("Starting Telegram bot...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        scanner.stop_scanning()
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


def main():
    """Main function"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()