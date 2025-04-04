import yfinance as yf
import pandas as pd
import time
import threading
from datetime import datetime, time as dt_time
import pytz
import os
from kafka import KafkaProducer, KafkaConsumer
import json
import subprocess
import csv
import asyncio
import logging
from typing import List, Dict, Any
from zoneinfo import ZoneInfo
import httpx
import nest_asyncio
from collections import deque
task_counter = 0
stock_index_topic = "stock_index_topic"

"1 Trading time judgment —————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————"
# 1.1 Define real-time data
# Ticker symbol for the STI Index
ticker_symbol_sti = "^STI"
# Ticker symbol for the S&P 500 index
ticker_symbol_sp = "^GSPC"

# 1.2 Define market hours
# Singapore Time (SGT)
MARKET_OPEN_1 = dt_time(9, 0)   # 9:00 AM SGT
MARKET_CLOSE_1 = dt_time(12, 0)  # 12:00 PM SGT
MARKET_OPEN_2 = dt_time(13, 0)   # 1:00 PM SGT
MARKET_CLOSE_2 = dt_time(17, 0)  # 5:00 PM SGT
# Eastern Time (ET)
MARKET_OPEN = dt_time(9, 30)       # 9:30 AM ET
MARKET_EARLYCLOSE = dt_time(13, 0) # 1:00 PM ET
MARKET_CLOSE = dt_time(16, 0)      # 4:00 PM ET

# 1.3 Define time zones
sgt_timezone = pytz.timezone("Asia/Singapore")   # Singapore Time
et_timezone = pytz.timezone("America/New_York")    # Eastern Time

# 1.4 Get trade calendar
# For STI
trade_calendar_sti = {}
with open('trade_calendar/trade_calendar_STI_2025.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        date_str = row['trade_date']
        status = int(row['trade_status'])
        trade_calendar_sti[date_str] = status
# For S&P500
trade_calendar_sp = {}
with open('trade_calendar/trade_calendar_SP500_2025.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        date_str = row['trade_date']
        status = int(row['trade_status'])
        trade_calendar_sp[date_str] = status
# For SH300 (if needed)
# Log Configuration
logger = logging.getLogger(__name__)
# Enable nested event loops
nest_asyncio.apply()

class TradeCalendar:
    __COLUMNS = ['trade_date', 'trade_status', 'day_week']
    __TIMEZONE = ZoneInfo("Asia/Shanghai")  # Beijing Time
    __CACHE_DIR = "cache/trade_calendar"
    __CACHE_FILENAME_TEMPLATE = "trade_calendar_{year}.csv"

    def __init__(self) -> None:
        super().__init__()
        os.makedirs(self.__CACHE_DIR, exist_ok=True)

    async def get_calendar(self, year=None) -> pd.DataFrame:
        """Get the stock trading calendar"""
        if not year:
            year = datetime.now(self.__TIMEZONE).year
        cache_path = self._get_csv_path(year)
        # Try to read data from the cache
        if os.path.exists(cache_path):
            return pd.read_csv(cache_path, header=0)
        # Fetch data from a remote interface
        calendar_df = await self._fetch_calendar_from_szse(year=year)
        if not calendar_df.empty:
            calendar_df.to_csv(cache_path, index=False)
            logger.info(f"Trade calendar data has been cached to: {cache_path}")
        return calendar_df

    def _get_csv_path(self, year) -> str:
        """Generate the path to the cache file"""
        return os.path.join(self.__CACHE_DIR, self.__CACHE_FILENAME_TEMPLATE.format(year=year))

    async def _fetch_month_data(self, client: httpx.AsyncClient, year: int, month: int) -> List[Dict[str, Any]]:
        """Get trading calendar data for a specific month from the Shenzhen Stock Exchange"""
        api_url = f"http://www.szse.cn/api/report/exchange/onepersistenthour/monthList?month={year}-{month}"
        try:
            res = await client.get(api_url)
            res.raise_for_status()
            res_json = res.json()
            return res_json.get('data', [])
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            return []

    async def _fetch_calendar_from_szse(self, year=None) -> pd.DataFrame:
        """Fetch trading calendar data for specific months from SZSE"""
        async with httpx.AsyncClient() as client:
            tasks = [self._fetch_month_data(client, year, month) for month in range(1, 13)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        data = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error getting monthly data: {result}")
                continue
            if result:
                data.extend(result)

        if not data:
            return pd.DataFrame(columns=self.__COLUMNS)

        rename = {'jyrq': 'trade_date', 'jybz': 'trade_status', 'zrxh': 'day_week'}
        return pd.DataFrame(data=data).rename(columns=rename)[self.__COLUMNS]

    async def is_trading_day_and_time(self) -> bool:
        """Check whether the current time is within the trading hours"""
        # Get today's date using Beijing time
        today = datetime.now(self.__TIMEZONE).strftime('%Y-%m-%d')
        calendar_df = await self.get_calendar()

        # Ensure trade_status is an integer and trade_date is a string
        calendar_df['trade_status'] = calendar_df['trade_status'].astype(int)
        calendar_df['trade_date'] = calendar_df['trade_date'].astype(str)

        trade_dates = calendar_df.query("trade_status == 1")['trade_date'].tolist()

        if today not in trade_dates:
            logger.info("Today is not a trading day...")
            return False

        # Get the current time using Beijing time
        current_time = datetime.now(self.__TIMEZONE).time()

        trading_hours = [
            (dt_time(9, 15), dt_time(9, 25)),   # the opening call auction
            (dt_time(9, 30), dt_time(11, 30)),   # the continuous auction
            (dt_time(13, 0), dt_time(14, 57)),   # the continuous auction
            (dt_time(14, 57), dt_time(15, 0))     # the closing call auction
        ]
        return any(start <= current_time <= end for start, end in trading_hours)

# Initialize the trading calendar
trade_calendar = TradeCalendar()
'''
condition = ['true', 'true', 'true', 'true', 'false', 'false','false']
condition_index = 0  # Used to record the index of the current condition
'''

# 1.2✅ Set the function to determine the trading period
async def is_trading_time():
    return trade_calendar.is_trading_day_and_time()
    """
    # Simulate condition judgment
    global condition_index
    if condition_index >= len(condition):  # If the condition list is exhausted, restart from the beginning
        condition_index = 0
    current_condition = condition[condition_index]  # Get the current condition
    condition_index += 1  # Move to the next condition
    return current_condition == 'true'  # Return boolean value
    """

# 1.5 Function to check if the market is open
# For STI
def is_market_open_sti():
    "Determine if the market is open based on today's trade_status and the time period."
    # 1) Get today's date string (e.g., "2025-03-24")
    today_sti = datetime.now(sgt_timezone).strftime("%Y-%m-%d")
    # 2) Get today's trading status from trade_calendar; default is 0 (closed)
    trade_status_sti = trade_calendar_sti.get(today_sti, 0)
    # 3) Get the current SGT time (time only, without date)
    now_sgt_time = datetime.now(sgt_timezone).time()

    # Make decisions based on trade_status
    if trade_status_sti == 0:
               return True

# For S&P500
def is_market_open_sp():
    "Determine if the market is open based on today's trade_status and the time period."
    # 1) Get today's date string (e.g., "2025-03-24")
    today_sp = datetime.now(et_timezone).strftime("%Y-%m-%d")
    # 2) Get today's trading status from trade_calendar; default is 0 (closed)
    trade_status_sp = trade_calendar_sp.get(today_sp, 0)
    # 3) Get the current ET time (time only, without date)
    now_et_time = datetime.now(et_timezone).time()

    # Make decisions based on trade_status
    if trade_status_sp == 0:
        return True

"2 Kafka producer setup ——————————————————————————————————————————————————————————————————————————————————————————————————————————————"
producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

"3 Producer function to fetch and send the latest price to Kafka ———————————————————————————————————————————————————————————————"
# For STI
def producer_sti():
    try:
        # Fetch the latest data
        sti_data = yf.Ticker(ticker_symbol_sti)
        latest_price_sti = sti_data.info

        # Extract the last price
        price_sti = latest_price_sti.get('regularMarketPrice')

        # Get current timestamp in SGT
        now_sgt = datetime.now(sgt_timezone)
        date_sgt = now_sgt.strftime("%Y-%m-%d")
        time_sgt = now_sgt.strftime("%H:%M:%S")

        # Create a dictionary for the new row
        new_row_sti = {
            "stock_code": "STI",
            "date": date_sgt,
            "time": time_sgt,
            "last_price": price_sti
        }

        # Send data to Kafka (on the 'stock_index_topic')
        producer.send('stock_index_topic', new_row_sti)
        producer.flush()  # Ensure all messages are sent

        # Print the data (optional)
        print(f"[Producer] Stock Code: {new_row_sti['stock_code']}, Date: {new_row_sti['date']}, Time: {new_row_sti['time']}, Price: {new_row_sti['last_price']}")
    except Exception as e:
        print(f"Error fetching data: {e}")

# For S&P500
def producer_sp():
    try:
        # Fetch the latest data
        sp_data = yf.Ticker(ticker_symbol_sp)
        latest_price_sp = sp_data.info

        # Extract the last price
        price_sp = latest_price_sp.get('regularMarketPrice')

        # Get current timestamp in ET
        now_et = datetime.now(sgt_timezone)
        date_et = now_et.strftime("%Y-%m-%d")
        time_et = now_et.strftime("%H:%M:%S")

        # Create a dictionary for the new row
        new_row_sp = {
            "stock_code": "SP500",
            "date": date_et,
            "time": time_et,
            "last_price": price_sp
        }

        # Send data to Kafka (on the 'stock_index_topic')
        producer.send('stock_index_topic', new_row_sp)
        producer.flush()  # Ensure all messages are sent

        # Print the data (optional)
        print(f"[Producer] Stock Code: {new_row_sp['stock_code']}, Date: {new_row_sp['date']}, Time: {new_row_sp['time']}, Price: {new_row_sp['last_price']}")
    except Exception as e:
        print(f"Error fetching data: {e}")

# For SH300
# Stock API
API_STOCK_INFO = "https://push2delay.eastmoney.com/api/qt/stock/details/get"
STOCK_CODE = "SH300"
INTERVAL = 3  # Get new data every 3 seconds
# Get today's date
today = datetime.today().strftime("%Y-%m-%d")
async def fetch_latest_stock_data():
    """Fetch the latest stock data"""
    params = {
        "secid": "1.000300",
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(API_STOCK_INFO, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
    return None
async def producer_task():
    """Kafka Producer"""
    # Continuously produce data
    while True:
        data = await fetch_latest_stock_data()
        if data and data.get("data") and data["data"].get("details"):
            latest_record = data["data"]["details"][-1]
            print("🔍 Raw SH300 record:", latest_record)

    # Handle string format
        if isinstance(latest_record, str):
            parts = latest_record.split(',')
            time_str = parts[0]
            price = parts[1]
        else:
    # Handle dict format fallback (in case API changes)
            time_str = latest_record.get('f51')
            price = latest_record.get('f52')

    # Create the record
            stock_info = {
            "stock_code": STOCK_CODE,
            "date": today,
            "time": time_str,
            "price": float(price),
            }

            producer.send(stock_index_topic, value=stock_info)
            print(f"Sent real-time data to Kafka: {stock_info}")

        #await asyncio.sleep(INTERVAL)  # Commented out because the main loop already handles timing

# In the main program, replace output_c with Kafka consumer_task or a similar function for visualization and historical output
def output_c():
    run_time = time.ctime(time.time())
    print(f"Current time is {run_time}, market is closed, only displaying historical data")

"4 Main loop to run during market hours ——————————————————————————————————————————————————————————————————————————————————————————————————"
# Define functions for both markets
def fetch_data_sti():
    while True:
        if is_market_open_sti():
            producer_sti()
            time.sleep(3)  # Wait for 3 seconds before the next fetch
        else:
            print("Singaporean market is closed. Stopping the script. Consumer please display the latest data.")
            break  # Exit the loop when the market closes

def fetch_data_sp500():
    while True:
        if is_market_open_sp():
            producer_sp()
            time.sleep(3)  # Wait for 3 seconds before the next fetch
        else:
            print("American market is closed. Stopping the script. Consumer please display the latest data.")
            break  # Exit the loop when the market closes

async def main():
    global task_counter
    last_result = None  # Used to record the previous result
    n = 0
    while True:
        # Run is_trading_time() judgment function every 3 seconds
        await asyncio.sleep(3)
        #condition = 'true'
        condition = await is_trading_time()

        if condition:
            # producer_task()
            task_counter += 1  # Assign a new task number
            task_id = task_counter
            print(f"Producer Task{task_id} Started, time: {time.ctime(time.time())}")
            asyncio.create_task(producer_task())  # Start Producer Task without blocking the main program
            last_result = True  # Update the last result
            """
            To run both Producer and Consumer concurrently:
            # Thread 1: Run Kafka Consumer (synchronously)
            consumer_thread = threading.Thread(target=consumer_task, daemon=True)
            consumer_thread.start()

            # Thread 2: Run Kafka Producer (asynchronous)
            asyncio.run(producer_task())
            """
        else:
            # Output_c()
            if last_result is not True:
                output_c()         # Alternatively, run Kafka consumer_task or similar visualization/output function
            last_result = False  # Update the previous result

# Run the main program
if __name__ == "__main__":
   asyncio.run(main())

# Create threads for both functions
thread_sti = threading.Thread(target=fetch_data_sti)
thread_sp500 = threading.Thread(target=fetch_data_sp500)

# Start both threads
thread_sti.start()
thread_sp500.start()

# Start SH300 async producer
asyncio.run(main())

# Optional: Wait for both threads to finish
thread_sti.join()
thread_sp500.join()