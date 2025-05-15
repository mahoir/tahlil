import ccxt
import pandas as pd
import os
import json
import shutil
import time
from datetime import datetime

# --- تنظیمات ---
# لیست نمادهای مورد نظر شما (با فرمت استاندارد صرافی)
symbols_to_fetch = ["BTC/USDT", "ETH/USDT", "TON/USDT"]
# لیست تایم فریم های مورد نظر
desired_timeframes = ['1m', '5m', '15m', '1h', '4h', '1d']
# شناسه صرافی (مثلا 'binance', 'bybit', 'kraken')
exchange_id = 'binance'
# پوشه اصلی برای ذخیره داده ها
data_directory = 'crypto_data'
# تعداد سطوح عمق بازار برای دانلود (مثلا 100 سطح بالا و پایین)
order_book_limit = 100

# --- توابع کمکی ---

def create_and_clean_symbol_folder(base_dir, symbol_name):
    """
    Creates a folder for the symbol, deleting its contents if it already exists.
    """
    # Replace invalid characters in symbol name for folder name
    folder_name = symbol_name.replace('/', '_').replace(':', '_')
    folder_path = os.path.join(base_dir, folder_name)

    if os.path.exists(folder_path):
        print(f"Cleaning existing data for {symbol_name} in {folder_path}...")
        shutil.rmtree(folder_path) # Delete the folder and its contents

    os.makedirs(folder_path) # Create the fresh folder
    print(f"Created/Cleaned folder: {folder_path}")
    return folder_path

def fetch_all_ohlcv(exchange, symbol, timeframe, limit=1000):
    """
    Fetches potentially all available historical OHLCV data for a symbol and timeframe.
    Returns a sorted pandas DataFrame (newest first).
    """
    all_ohlcv = []
    since = None # Start from the earliest available data

    print(f"  Fetching {symbol} {timeframe} data...")

    # Fetching loop to get historical data in chunks
    while True:
        try:
            ohlcv_chunk = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)

            if not ohlcv_chunk:
                break

            all_ohlcv.extend(ohlcv_chunk)
            print(f"    Fetched {len(ohlcv_chunk)} candles. Total fetched: {len(all_ohlcv)}")

            # Update 'since' for the next request
            since = ohlcv_chunk[-1][0] + 1 # Timestamp is in milliseconds

            # Respect exchange rate limits
            time.sleep(exchange.rateLimit / 1000)

        except ccxt.RateLimitExceeded as e:
             print(f"    Rate limit exceeded. Waiting {exchange.rateLimit / 1000} seconds. Error: {e}")
             time.sleep(exchange.rateLimit / 1000 * 2)
        except Exception as e:
            print(f"    Error fetching {symbol} {timeframe} chunk: {e}")
            break

    if all_ohlcv:
         # Convert list of lists to pandas DataFrame
         df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
         # Convert timestamp from milliseconds to datetime objects
         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
         # Remove potential duplicate timestamps if any and sort ascending initially
         df = df.drop_duplicates(subset=['timestamp']).sort_values(by='timestamp').reset_index(drop=True)

         # Sort descending for saving (newest first)
         df_sorted = df.sort_values(by='timestamp', ascending=False).reset_index(drop=True)

         print(f"  Finished fetching {symbol} {timeframe}. Total unique candles: {len(df_sorted)}")
         return df_sorted
    else:
        print(f"  No OHLCV data fetched for {symbol} {timeframe}.")
        return pd.DataFrame()

def format_ohlcv_for_txt(df, symbol, timeframe):
    """
    Formats the OHLCV DataFrame into a clear string for TXT file, readable by AI.
    """
    if df.empty:
        return "No OHLCV data available."

    header = f"--- OHLCV Data for {symbol} - Timeframe: {timeframe} ---\n"
    header += "Columns: Timestamp | Open | High | Low | Close | Volume\n"
    header += "-------------------------------------------------------\n"

    data_lines = []
    # Iterate through DataFrame rows and format each line
    # Use isoformat for clear timestamp representation
    for index, row in df.iterrows():
        line = (
            f"Timestamp: {row['timestamp'].isoformat()} | "
            f"Open: {row['open']} | "
            f"High: {row['high']} | "
            f"Low: {row['low']} | "
            f"Close: {row['close']} | "
            f"Volume: {row['volume']}"
        )
        data_lines.append(line)

    # Join lines with newline character
    return header + "\n".join(data_lines) + "\n--- End of OHLCV Data ---"


def format_order_book_for_txt(orderbook, symbol):
    """
    Formats the order book dictionary into a clear string for TXT file, readable by AI.
    """
    if orderbook is None:
        return "Order Book data not available."

    # Capture current time or use a timestamp from the data if available and reliable
    # For simplicity, let's just state it's a snapshot
    header = f"--- Order Book Snapshot for {symbol} ---\n"
    header += "Note: This is a snapshot, not real-time or historical depth.\n"
    header += "------------------------------------------\n"

    bids_section = "--- Bids (Buy Orders - Price | Volume) ---\n"
    if 'bids' in orderbook and orderbook['bids']:
        for price, volume in orderbook['bids']:
            bids_section += f"Price: {price} | Volume: {volume}\n"
    else:
        bids_section += "No bid orders in snapshot.\n"
    bids_section += "------------------------------------\n"


    asks_section = "--- Asks (Sell Orders - Price | Volume) ---\n"
    if 'asks' in orderbook and orderbook['asks']:
         for price, volume in orderbook['asks']:
            asks_section += f"Price: {price} | Volume: {volume}\n"
    else:
        asks_section += "No ask orders in snapshot.\n"
    asks_section += "------------------------------------\n"

    return header + bids_section + asks_section + "--- End of Order Book Snapshot ---"


def fetch_order_book(exchange, symbol, limit=100):
    """
    Fetches the current order book for a symbol.
    """
    print(f"  Fetching Order Book for {symbol}...")
    try:
        # Fetch the order book snapshot
        orderbook = exchange.fetch_order_book(symbol, limit=limit)
        print(f"    Fetched {len(orderbook.get('bids', []))} bids and {len(orderbook.get('asks', []))} asks.")
        return orderbook
    except Exception as e:
        print(f"  Error fetching order book for {symbol}: {e}")
        return None


# --- پیاده سازی بخش های مربوط به داده های دیگر (نیاز به APIهای مجزا) ---
# (این توابع تغییر نکرده اند و صرفا Placeholder هستند)

def fetch_fear_greed_index():
    """Placeholder"""
    print("  Attempting to fetch Fear & Greed Index (Requires external API)...")
    return None

def fetch_market_cap(symbol_name):
    """Placeholder"""
    print(f"  Attempting to fetch Market Cap for {symbol_name} (Requires external API)...")
    return None

# --- اجرای اصلی ---

if __name__ == "__main__":
    # Create the base data directory if it doesn't exist
    os.makedirs(data_directory, exist_ok=True)

    try:
        # Initialize the exchange object
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True, # Respect exchange rate limits
            # Add API key and secret here if needed
            # 'apiKey': 'YOUR_API_KEY',
            # 'secret': 'YOUR_SECRET',
        })

        print(f"Initialized exchange: {exchange_id}")

        if not exchange.has['fetchOHLCV']:
            print(f"Error: Exchange {exchange_id} does not support fetching OHLCV.")
            exit()
        if not exchange.has['fetchOrderBook']:
             print(f"Warning: Exchange {exchange_id} does not support fetching Order Book.")


        # Loop through each symbol
        for symbol in symbols_to_fetch:
            print(f"\n--- Processing Symbol: {symbol} ---")

            # Create and clean the folder for the current symbol
            symbol_folder = create_and_clean_symbol_folder(data_directory, symbol)

            # --- 1. Fetch and Save OHLCV Data (to .txt) ---
            for timeframe in desired_timeframes:
                ohlcv_df_sorted = fetch_all_ohlcv(exchange, symbol, timeframe)
                if not ohlcv_df_sorted.empty:
                    # Format data for TXT
                    ohlcv_txt_content = format_ohlcv_for_txt(ohlcv_df_sorted, symbol, timeframe)
                    # Save to .txt file
                    ohlcv_filename = os.path.join(symbol_folder, f"{symbol.replace('/', '_')}_{timeframe}_ohlcv.txt")
                    with open(ohlcv_filename, 'w', encoding='utf-8') as f:
                        f.write(ohlcv_txt_content)
                    print(f"  Saved OHLCV data to {ohlcv_filename}")

            # --- 2. Fetch and Save Market Depth (Order Book) (to .txt) ---
            if exchange.has['fetchOrderBook']:
                order_book_data = fetch_order_book(exchange, symbol, limit=order_book_limit)
                if order_book_data:
                    # Format data for TXT
                    order_book_txt_content = format_order_book_for_txt(order_book_data, symbol)
                    # Save to .txt file
                    order_book_filename = os.path.join(symbol_folder, f"{symbol.replace('/', '_')}_orderbook_snapshot.txt")
                    with open(order_book_filename, 'w', encoding='utf-8') as f:
                         f.write(order_book_txt_content)
                    print(f"  Saved Order Book snapshot to {order_book_filename}")
            else:
                 print(f"  Skipping Order Book fetch: Exchange {exchange_id} does not support it.")


            # --- 3. Fetch and Save Other Data (to .txt if implemented) ---
            # (These functions are placeholders)

            # Example: Fetch and save Fear & Greed Index (requires external API)
            # fear_greed_data = fetch_fear_greed_index()
            # if fear_greed_data:
            #     # You would need a function to format this data for TXT as well
            #     fg_filename = os.path.join(symbol_folder, "fear_greed_index.txt")
            #     with open(fg_filename, 'w', encoding='utf-8') as f:
            #          f.write(f"--- Fear & Greed Index ---\n{json.dumps(fear_greed_data, indent=4)}") # Simple JSON dump for example
            #     print(f"  Saved Fear & Greed Index to {fg_filename}")

            # Example: Fetch and save Market Cap (requires external API like CoinGecko/CMC)
            # market_cap_data = fetch_market_cap(symbol) # Might need symbol mapping
            # if market_cap_data:
            #     # You would need a function to format this data for TXT as well
            #     mc_filename = os.path.join(symbol_folder, "market_cap.txt")
            #     with open(mc_filename, 'w', encoding='utf-8') as f:
            #          f.write(f"--- Market Cap Data ---\n{json.dumps(market_cap_data, indent=4)}") # Simple JSON dump for example
            #     print(f"  Saved Market Cap data to {mc_filename}")


            print(f"--- Finished processing {symbol} ---")
            # Add a small delay between symbols to be extra safe with rate limits
            time.sleep(5)


    except Exception as e:
        print(f"\nAn error occurred during the main process: {e}")

    print("\nData fetching process finished.")
