import pandas as pd
import ta
import time
import logging
import json
import os
from datetime import datetime, timedelta
from binance.client import Client
from binance.exceptions import BinanceAPIException
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List
import sys
import asyncio
from telegram.ext import ApplicationBuilder

# =========================
# CONFIG
# =========================
BINANCE_API_KEY = "JZdDjf3jXexFotRKImZanW3MBWc940lDg05bjRVhPNVKGr9xpOHhNMuAcQ7d6gJp"
BINANCE_SECRET = "aps8ibon7fDxQmO1W1fRzxypj7UDGqCtwigx5JhqakEWJ9fWUSkL23uWEGFWMH68"

TELEGRAM_TOKEN = "8737132980:AAHXlronSEspoQ8shgrKZu3fihJaLtesGbY"

TIMEFRAME = "15m"
RISK_PERCENT = 1.0       # 1% risk per trade
RR_RATIO = 2.0           # Risk Reward 1:2
CAPITAL = 1000.0         # 1000 USDT
TOP_VOLUME = 50          # Top 50 volume pairs
SCAN_INTERVAL = 300       # Scan every 5 min
MIN_VOLUME_USDT = 100000 # Minimum 24h volume in USDT
MAX_WORKERS = 10         # Max threads for parallel processing

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
    
    def __init__(self, storage_file: str):
        self.storage_file = storage_file
        self.active_users: Dict[str, dict] = {}
        self.load_users()
    
    def load_users(self):
        """Load active users from file"""
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as f:
                    self.active_users = json.load(f)
                logger.info(f"Loaded {len(self.active_users)} active users")
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            self.active_users = {}
    
    def save_users(self):
        """Save active users to file"""
        try:
            with open(self.storage_file, 'w') as f:
                json.dump(self.active_users, f, indent=2)
            logger.info(f"Saved {len(self.active_users)} active users")
        except Exception as e:
            logger.error(f"Error saving users: {e}")
    
    def add_user(self, user_id: str, username: str = None, first_name: str = None):
        """Add or update user"""
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
        """Remove user (soft delete by marking inactive)"""
        if user_id in self.active_users:
            self.active_users[user_id]['active'] = False
            self.active_users[user_id]['left_at'] = datetime.now().isoformat()
            self.save_users()
            logger.info(f"User {user_id} deactivated")
    
    def get_active_users(self) -> List[str]:
        """Get list of active user IDs"""
        return [
            uid for uid, data in self.active_users.items() 
            if data.get('active', False) and data.get('preferences', {}).get('receive_signals', True)
        ]
    
    def update_last_active(self, user_id: str):
        """Update user's last active timestamp"""
        if user_id in self.active_users:
            self.active_users[user_id]['last_active'] = datetime.now().isoformat()
            self.save_users()
    
    def get_user_count(self) -> int:
        """Get number of active users"""
        return len(self.get_active_users())
    
    def cleanup_inactive_users(self, days: int = 30):
        """Remove users inactive for specified days"""
        cutoff = datetime.now() - timedelta(days=days)
        to_remove = []
        
        for user_id, data in self.active_users.items():
            last_active = datetime.fromisoformat(data.get('last_active', '2000-01-01'))
            if last_active < cutoff:
                to_remove.append(user_id)
        
        for user_id in to_remove:
            del self.active_users[user_id]
        
        if to_remove:
            self.save_users()
            logger.info(f"Cleaned up {len(to_remove)} inactive users")


class CryptoScanner:
    """Main scanner class for cryptocurrency trading signals"""
    
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
                await asyncio.sleep(0.05)  # Small delay to avoid rate limiting
            except Exception as e:
                logger.error(f"Failed to send to user {user_id}: {e}")
                fail_count += 1
                
                # If user blocked the bot or deleted chat, deactivate them
                if "blocked" in str(e) or "deactivated" in str(e):
                    self.user_manager.remove_user(user_id)
        
        if success_count > 0:
            logger.info(f"Broadcast complete. Success: {success_count}, Failed: {fail_count}")
    
    def get_top_symbols(self) -> List[str]:
        """Get top volume USDT pairs from Binance Futures"""
        try:
            tickers = self.client.futures_ticker()
            
            # Filter USDT pairs and filter by minimum volume
            usdt_pairs = [
                t for t in tickers 
                if t['symbol'].endswith("USDT") 
                and float(t['quoteVolume']) > MIN_VOLUME_USDT
            ]
            
            # Sort by 24h volume
            sorted_pairs = sorted(
                usdt_pairs, 
                key=lambda x: float(x['quoteVolume']), 
                reverse=True
            )
            
            top_symbols = [x['symbol'] for x in sorted_pairs[:TOP_VOLUME]]
            logger.info(f"Found {len(top_symbols)} top volume symbols")
            
            return top_symbols
            
        except BinanceAPIException as e:
            logger.error(f"Binance API error in get_top_symbols: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_top_symbols: {e}")
            return []
    
    def get_klines(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get klines/candlestick data for a symbol"""
        try:
            klines = self.client.futures_klines(
                symbol=symbol, 
                interval=TIMEFRAME, 
                limit=200
            )
            
            df = pd.DataFrame(klines, columns=[
                'time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'qav', 'num_trades',
                'taker_base_vol', 'taker_quote_vol', 'ignore'
            ])
            
            # Convert string columns to float
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            
            # Convert time to datetime
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            
            # Drop any rows with NaN values
            df.dropna(inplace=True)
            
            return df
            
        except BinanceAPIException as e:
            logger.error(f"Binance API error for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting klines for {symbol}: {e}")
            return None
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators"""
        try:
            # EMAs
            df['ema50'] = ta.trend.ema_indicator(df['close'], window=50)
            df['ema200'] = ta.trend.ema_indicator(df['close'], window=200)
            
            # ATR
            df['atr'] = ta.volatility.average_true_range(
                df['high'], df['low'], df['close'], window=14
            )
            
            # Additional indicators for confirmation
            df['rsi'] = ta.momentum.rsi(df['close'], window=14)
            df['volume_sma'] = df['volume'].rolling(window=20).mean()
            
            return df
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return df
    
    def check_signal(self, symbol: str) -> Optional[Dict]:
        """Check for trading signals on a symbol"""
        try:
            # Get and validate data
            df = self.get_klines(symbol)
            if df is None or len(df) < 200:
                return None
            
            # Calculate indicators
            df = self.calculate_indicators(df)
            
            # Get last row for signal
            last = df.iloc[-1]
            
            # Check for NaN values
            if pd.isna(last[['close', 'atr', 'ema50', 'ema200']]).any():
                return None
            
            entry = last['close']
            atr = last['atr']
            
            # Validate ATR (avoid division by zero)
            if atr <= 0 or atr > entry * 0.1:  # ATR shouldn't be more than 10% of price
                return None
            
            risk_amount = CAPITAL * (RISK_PERCENT / 100)
            
            # Check if volume is above average (confirmation)
            volume_confirm = last['volume'] > last['volume_sma']
            
            # LONG condition
            if (last['ema50'] > last['ema200'] and 
                last['close'] > last['ema50'] and
                last['rsi'] < 70 and  # Not overbought
                last['rsi'] > 40 and  # Not too weak
                volume_confirm):
                
                sl = entry - (atr * 1.5)  # Wider stop for more reliability
                tp = entry + (atr * RR_RATIO * 1.5)
                
                # Calculate position size based on risk
                position_size = risk_amount / (entry - sl)
                
                return {
                    'type': 'LONG',
                    'symbol': symbol,
                    'entry': entry,
                    'sl': sl,
                    'tp': tp,
                    'size': position_size,
                    'rr': RR_RATIO,
                    'atr': atr,
                    'rsi': last['rsi'],
                    'timestamp': datetime.now()
                }
            
            # SHORT condition
            elif (last['ema50'] < last['ema200'] and 
                  last['close'] < last['ema50'] and
                  last['rsi'] > 30 and  # Not oversold
                  last['rsi'] < 60 and  # Not too strong
                  volume_confirm):
                
                sl = entry + (atr * 1.5)
                tp = entry - (atr * RR_RATIO * 1.5)
                
                position_size = risk_amount / (sl - entry)
                
                return {
                    'type': 'SHORT',
                    'symbol': symbol,
                    'entry': entry,
                    'sl': sl,
                    'tp': tp,
                    'size': position_size,
                    'rr': RR_RATIO,
                    'atr': atr,
                    'rsi': last['rsi'],
                    'timestamp': datetime.now()
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking signal for {symbol}: {e}")
            return None
    
    def format_signal_message(self, signal: Dict) -> str:
        """Format signal for Telegram message"""
        emoji = "🟢" if signal['type'] == 'LONG' else "🔴"
        
        # Calculate potential profit
        if signal['type'] == 'LONG':
            potential_profit = (signal['tp'] - signal['entry']) * signal['size']
        else:
            potential_profit = (signal['entry'] - signal['tp']) * signal['size']
        
        message = f"""
{emoji} <b>{signal['type']} SIGNAL - {signal['symbol']}</b>
━━━━━━━━━━━━━━━━━━━
💰 <b>Entry:</b> {signal['entry']:.4f}
🛑 <b>Stop Loss:</b> {signal['sl']:.4f}
✅ <b>Take Profit:</b> {signal['tp']:.4f}
📊 <b>Position Size:</b> {signal['size']:.4f}
📈 <b>Risk/Reward:</b> 1:{signal['rr']}
💵 <b>Potential Profit:</b> ${potential_profit:.2f}
📉 <b>ATR:</b> {signal['atr']:.4f}
📊 <b>RSI:</b> {signal['rsi']:.1f}
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
            # Get top symbols
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
                    try:
                        signal = future.result()
                        if signal:
                            signals.append(signal)
                    except Exception as e:
                        logger.error(f"Error processing symbol: {e}")
            
            # Send signals to all active users
            for signal in signals:
                if self.can_send_signal(signal['symbol']):
                    message = self.format_signal_message(signal)
                    await self.broadcast_to_users(message)
                    
                    # Update sent signals
                    self.sent_signals[signal['symbol']] = time.time()
                    logger.info(f"Signal broadcast for {signal['symbol']}")
                    await asyncio.sleep(0.1)  # Small delay between signals
            
            # Send periodic status update (every 30 minutes)
            if datetime.now() - self.last_status_broadcast > timedelta(minutes=30):
                await self.broadcast_status_update()
                self.last_status_broadcast = datetime.now()
            
            if signals:
                logger.info(f"Scan complete. Found {len(signals)} signals")
            
        except Exception as e:
            logger.error(f"Error in process_and_broadcast_signals: {e}")
    
    async def broadcast_status_update(self):
        """Send periodic status update to all users"""
        active_users = self.user_manager.get_user_count()
        message = f"""
🤖 <b>Bot Status Update</b>
━━━━━━━━━━━━━━━━━━━
✅ Running normally
👥 Active users: {active_users}
📊 Timeframe: {TIMEFRAME}
💰 Risk per trade: {RISK_PERCENT}%
📈 Top {TOP_VOLUME} volume pairs
⏱️ Scan interval: {SCAN_INTERVAL}s
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
            
            # Wait for next scan
            for _ in range(SCAN_INTERVAL):
                if not self.scanning:
                    break
                await asyncio.sleep(1)
    
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
    
    # Get scanner instance
    scanner = context.bot_data.get('scanner')
    if scanner:
        # Add user to active users
        scanner.user_manager.add_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name
        )
    
    welcome_message = f"""
🎉 <b>Welcome to Crypto Signal Bot!</b>

Hello {user.first_name}! I'm your automated crypto trading signal scanner.

<b>Features:</b>
• Real-time signal scanning
• Top {TOP_VOLUME} volume pairs
• {TIMEFRAME} timeframe analysis
• Risk management included
• Multi-user support

<b>Commands:</b>
/start - Show this welcome message
/status - Check bot status
/stats - View scanning statistics
/stop - Stop receiving signals
/help - Show help message

You'll automatically receive trading signals when detected!
    """
    
    await update.message.reply_text(welcome_message, parse_mode='HTML')
    
    # Send quick status
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
📊 Timeframe: {TIMEFRAME}
💰 Risk: {RISK_PERCENT}% per trade
━━━━━━━━━━━━━━━━━━━
        """
        await update.message.reply_text(message, parse_mode='HTML')


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command"""
    scanner = context.bot_data.get('scanner')
    user_id = str(update.effective_user.id)
    
    if scanner:
        scanner.user_manager.update_last_active(user_id)
        
        # Get some basic stats
        total_signals_today = len([s for s in scanner.sent_signals.values() 
                                  if time.time() - s < 86400])
        
        message = f"""
📈 <b>Bot Statistics</b>
━━━━━━━━━━━━━━━━━━━
📊 Signals today: {total_signals_today}
👥 Active users: {scanner.user_manager.get_user_count()}
💹 Total users all time: {len(scanner.user_manager.active_users)}
⏰ Last signal: {'Never' if not scanner.sent_signals else 
                 datetime.fromtimestamp(max(scanner.sent_signals.values())).strftime('%H:%M:%S')}
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
❓ <b>Help & Commands</b>

<b>Available Commands:</b>
/start - Start bot and receive signals
/status - Check current bot status
/stats - View bot statistics
/stop - Stop receiving signals
/help - Show this help message

<b>How it works:</b>
1. Bot scans top {TOP_VOLUME} volume pairs every {SCAN_INTERVAL}s
2. Technical analysis on {TIMEFRAME} timeframe
3. Signals sent when conditions are met
4. Each signal includes entry, SL, TP, and position size

<b>Strategy:</b>
• EMA50/EMA200 trend filter
• RSI for momentum confirmation
• Volume confirmation
• ATR-based stops and targets
• 1:{RR_RATIO} risk/reward ratio

<b>Risk Warning:</b>
⚠️ Trading cryptocurrencies carries high risk. Never trade more than you can afford to lose. This bot is for informational purposes only.
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
    # Initialize scanner
    scanner = CryptoScanner()
    
    # Create Telegram application
    # application = Application.builder().token(TELEGRAM_TOKEN).build()
    application = (
    ApplicationBuilder()
    .token(TELEGRAM_TOKEN)
    .connect_timeout(30.0)  # Tăng thời gian chờ kết nối lên 30 giây
    .read_timeout(30.0)     # Tăng thời gian chờ đọc dữ liệu lên 30 giây
    .build()
)
    
    # Store scanner in bot_data for access in handlers
    application.bot_data['scanner'] = scanner
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("help", help_command))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Send startup message to all users
    startup_message = """
🚀 <b>Bot Started!</b>

The crypto signal bot is now running and scanning for opportunities.
You'll receive signals when detected.
    """
    await scanner.broadcast_to_users(startup_message)
    
    # Start background scanning task
    asyncio.create_task(run_scanner_background(scanner))
    
    # Start the bot
    logger.info("Starting Telegram bot...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    # Keep the bot running
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