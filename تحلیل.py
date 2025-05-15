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
# شناسه صرافی (مثلا 'binance', 'bybit', 'kraken')..
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
    Note: This is a basic loop. Fetching full history might require more
    sophisticated handling of timestamps and potential API limits/errors.
    """
    all_ohlcv = []
    since = None # Start from the earliest available data

    print(f"  Fetching {symbol} {timeframe} data...")

    # Fetching loop to get historical data in chunks
    # Start from a recent time if full history is too long/problematic
    # Example: since = exchange.parse8601('2023-01-01T00:00:00Z') # Fetch from a specific date

    while True:
        try:
            # Fetch data chunk
            # Use since=None initially, then update it with the timestamp of the last candle + 1 millisecond
            # Note: Different exchanges might handle 'since' and limits slightly differently.
            # A robust implementation might need exchange-specific logic.
            ohlcv_chunk = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)

            if not ohlcv_chunk:
                # No more data
                break

            all_ohlcv.extend(ohlcv_chunk)
            print(f"    Fetched {len(ohlcv_chunk)} candles. Total fetched: {len(all_ohlcv)}")

            # Update 'since' for the next request
            # We need to fetch from the timestamp of the *last* candle received + 1 millisecond
            since = ohlcv_chunk[-1][0] + 1 # Timestamp is in milliseconds

            # Respect exchange rate limits - wait for the specified time
            time.sleep(exchange.rateLimit / 1000)

            # Optional: Add a check to stop after fetching a certain large amount if full history is too big
            # if len(all_ohlcv) > 100000: # Example limit
            #     print("  Reached maximum candle limit for fetching.")
            #     break

        except ccxt.RateLimitExceeded as e:
             print(f"    Rate limit exceeded. Waiting {exchange.rateLimit / 1000} seconds. Error: {e}")
             time.sleep(exchange.rateLimit / 1000 * 2) # Wait a bit longer if rate limited
        except Exception as e:
            print(f"    Error fetching {symbol} {timeframe} chunk: {e}")
            # Decide whether to break or continue/retry on other errors
            break # Stop fetching this timeframe on error

    if all_ohlcv:
         # Remove potential duplicate if the last candle of the previous chunk
         # is the same as the first candle of the current chunk (due to `since = timestamp + 1`)
         # This is a common issue with 'since' logic. A more robust check might be needed.
         # For simplicity here, we assume minimal duplicates or accept them for now.
         # A better approach involves checking the last timestamp of the combined list.

         # Convert list of lists to pandas DataFrame
         df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
         # Convert timestamp from milliseconds to datetime objects
         df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
         # Remove potential duplicate timestamps if any
         df = df.drop_duplicates(subset=['timestamp']).sort_values(by='timestamp').reset_index(drop=True)

         # --- تغییر جدید: مرتب سازی داده ها بر اساس تاریخ (جدیدترین در ابتدا) ---
         df_sorted = df.sort_values(by='timestamp', ascending=False).reset_index(drop=True)
         # ---------------------------------------------------------------------

         print(f"  Finished fetching {symbol} {timeframe}. Total unique candles: {len(df_sorted)}")
         return df_sorted # برگرداندن DataFrame مرتب شده
    else:
        print(f"  No OHLCV data fetched for {symbol} {timeframe}.")
        return pd.DataFrame() # Return empty DataFrame if no data

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

def fetch_fear_greed_index():
    """
    Placeholder function to fetch Fear & Greed Index.
    Requires finding and using a specific API (e.g., Alternative.me API).
    Returns: dict or None
    """
    print("  Attempting to fetch Fear & Greed Index (Requires external API)...")
    # Example (this is illustrative, you need to find/use a real API):
    # try:
    #     response = requests.get("YOUR_FEAR_GREED_API_ENDPOINT")
    #     data = response.json()
    #     print("    Fear & Greed Index data fetched.")
    #     return data
    # except Exception as e:
    #     print(f"  Error fetching Fear & Greed Index: {e}")
    return None # Not implemented in this example

def fetch_market_cap(symbol_name):
    """
    Placeholder function to fetch Market Cap and Circulating Supply.
    Requires finding and using a specific API (e.g., CoinMarketCap or CoinGecko API).
    Note: symbol_name format might differ for these APIs (e.g., "bitcoin", "ethereum").
    Returns: dict or None
    """
    print(f"  Attempting to fetch Market Cap for {symbol_name} (Requires external API)...")
    # Example (this is illustrative, you need to find/use a real API):
    # try:
    #     # Map exchange symbol to CoinGecko/CMC ID if needed
    #     coin_id = map_symbol_to_coin_id(symbol_name) # Your mapping function
    #     response = requests.get(f"YOUR_MARKET_DATA_API_ENDPOINT/{coin_id}")
    #     data = response.json()
    #     print(f"    Market Cap data fetched for {symbol_name}.")
    #     return data
    # except Exception as e:
    #     print(f"  Error fetching Market Cap for {symbol_name}: {e}")
    return None # Not implemented in this example

# --- اجرای اصلی ---

if __name__ == "__main__":
    # Create the base data directory if it doesn't exist
    os.makedirs(data_directory, exist_ok=True)

    try:
        # Initialize the exchange object
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True, # Respect exchange rate limits
            # Add API key and secret here if needed for higher limits or private data
            # 'apiKey': 'YOUR_API_KEY',
            # 'secret': 'YOUR_SECRET',
        })

        print(f"Initialized exchange: {exchange_id}")

        # Check if exchange supports required features
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

            # --- 1. Fetch and Save OHLCV Data ---
            for timeframe in desired_timeframes:
                # تابع fetch_all_ohlcv حالا DataFrame مرتب شده را برمی گرداند
                ohlcv_df_sorted = fetch_all_ohlcv(exchange, symbol, timeframe)
                if not ohlcv_df_sorted.empty:
                    # Save OHLCV data to CSV
                    ohlcv_filename = os.path.join(symbol_folder, f"{symbol.replace('/', '_')}_{timeframe}_ohlcv.csv")
                    # ذخیره DataFrame مرتب شده در فایل
                    ohlcv_df_sorted.to_csv(ohlcv_filename, index=False)
                    print(f"  Saved OHLCV data to {ohlcv_filename}")

            # --- 2. Fetch and Save Market Depth (Order Book) ---
            # Note: This gets a snapshot of the current order book.
            # Fetching historical order book is much more complex and data-intensive.
            if exchange.has['fetchOrderBook']:
                order_book_data = fetch_order_book(exchange, symbol, limit=order_book_limit)
                if order_book_data:
                    # Save Order Book data to JSON
                    order_book_filename = os.path.join(symbol_folder, f"{symbol.replace('/', '_')}_orderbook_snapshot.json")
                    with open(order_book_filename, 'w') as f:
                        json.dump(order_book_data, f, indent=4)
                    print(f"  Saved Order Book snapshot to {order_book_filename}")
            else:
                 print(f"  Skipping Order Book fetch: Exchange {exchange_id} does not support it.")


            # --- 3. Fetch and Save Other Data (Fear/Greed, Market Cap, etc.) ---
            # These functions are placeholders. You need to implement them
            # using APIs from other data sources.

            # Example: Fetch and save Fear & Greed Index (requires external API)
            # fear_greed_data = fetch_fear_greed_index()
            # if fear_greed_data:
            #     fg_filename = os.path.join(symbol_folder, "fear_greed_index.json")
            #     with open(fg_filename, 'w') as f:
            #         json.dump(fear_greed_data, f, indent=4)
            #     print(f"  Saved Fear & Greed Index to {fg_filename}")
            # else:
            #     print("  Fear & Greed Index not fetched.")


            # Example: Fetch and save Market Cap (requires external API like CoinGecko/CMC)
            # market_cap_data = fetch_market_cap(symbol) # Might need symbol mapping
            # if market_cap_data:
            #     mc_filename = os.path.join(symbol_folder, "market_cap.json")
            #     with open(mc_filename, 'w') as f:
            #         json.dump(market_cap_data, f, indent=4)
            #     print(f"  Saved Market Cap data to {mc_filename}")
            # else:
            #      print("  Market Cap data not fetched.")


            print(f"--- Finished processing {symbol} ---")
            # Add a small delay between symbols to be extra safe with rate limits
            time.sleep(5) # Wait 5 seconds between processing different symbols


    except Exception as e:
        print(f"\nAn error occurred during the main process: {e}")

    print("\nData fetching process finished.")
