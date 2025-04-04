import matplotlib.pyplot as plt
import json
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime
import time

def calculate_var(prices, confidence=0.05):
    """
    Calculate VaR using the historical simulation method:
    1. Compute the logarithmic returns of the consecutive prices.
    2. Compute the percentile based on the given confidence level; VaR is the absolute value of that percentile.
    """
    prices = np.array(prices)
    if len(prices) < 2:
        return None
    log_returns = np.diff(np.log(prices))
    var = -np.percentile(log_returns, confidence * 100)
    return var

def main():
    consumer = KafkaConsumer(
        'stock_index_topic',
        bootstrap_servers='127.0.0.1:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='read-all-history-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("Consumer started. Reading all historical data...")

    price_history = []

    timeout_seconds = 10
    last_message_time = time.time()

    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        if not msg_pack:
            if time.time() - last_message_time > timeout_seconds:
                break
            continue

        for tp, messages in msg_pack.items():
            for message in messages:
                data = message.value
                stock_code = data.get("stock_code")
                date = data.get("date")
                time_str = data.get("time")
                price = data.get("price")
                print(f"[Consumer] Data Received >> Stock_code: {stock_code}, Date: {date}, Time: {time_str}, Price: {price}")
                if price is not None:
                    price_history.append(price)
                last_message_time = time.time()

    if len(price_history) >= 2:
        var = calculate_var(price_history, confidence=0.05)
        print(f"[Consumer] Overall VaR (95% confidence level): {var}")

        # Visualization
        plt.plot(price_history, label='Price History')
        plt.axhline(y=price_history[-1] - var, color='r', linestyle='--', label='VaR (5%) Threshold')
        plt.title("Stock Price History with VaR (95%)")
        plt.xlabel("Time (Index)")
        plt.ylabel("Price")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print("[Consumer] Insufficient data to calculate VaR")

if __name__ == '__main__':
    main()
