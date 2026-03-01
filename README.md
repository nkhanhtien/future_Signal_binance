# 🚀 Binance Crypto Signal Telegram Bot

Bot tự động quét thị trường tiền mã hóa trên Binance Futures, thực hiện phân tích kỹ thuật (Technical Analysis) sử dụng thư viện `pandas` & `ta`, và gửi tín hiệu giao dịch (Long/Short) trực tiếp đến người dùng qua Telegram.

## ✨ Tính năng chính

- **Quét thời gian thực:** Liên tục quét top 50 cặp coin có khối lượng giao dịch (Volume) lớn nhất trên Binance.
- **Phân tích kỹ thuật tự động:** Sử dụng `pandas` để xử lý dữ liệu Klines và `ta` để tính toán các chỉ báo (EMA, RSI, ATR, Volume SMA).
- **Quản lý rủi ro (Risk Management):** Tự động tính toán vị thế (Position Size), Stop Loss (SL) và Take Profit (TP) dựa trên ATR và tỷ lệ R:R.
- **Bot Telegram tương tác:** Hỗ trợ đa người dùng với các lệnh quản lý trạng thái (`/start`, `/status`, `/stats`, `/stop`).
- **Xử lý đa luồng (Multi-threading):** Sử dụng `ThreadPoolExecutor` để quét song song, tối ưu hóa tốc độ xử lý.

## 🛠️ Yêu cầu hệ thống

- Python 3.8 trở lên.
- Tài khoản Binance (cần API Key & Secret).
- Bot Telegram (cần Bot Token từ BotFather).

## 📦 Cài đặt

**Bước 1:** Clone hoặc tải source code về máy.

**Bước 2:** Cài đặt các thư viện Python cần thiết bằng `pip`:


## 📦 Cài đặt & Khởi chạy (Setup & Run)

Khuyến nghị sử dụng môi trường ảo (Virtual Environment) để cài đặt các thư viện cho dự án này, giúp tránh xung đột với các package khác trên hệ thống.

**Bước 1: Di chuyển vào thư mục dự án**
```bash
cd future_Signal_binance
python3 -m venv venv
source venv/bin/activate
pip install pandas ta python-binance python-telegram-bot
python deepseed_future.py

Cách thoát môi trường ảo (Khi không dùng nữa):
Gõ lệnh sau vào terminal:
deactivate