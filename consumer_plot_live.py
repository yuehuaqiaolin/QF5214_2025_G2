from alerts import check_alert_conditions
import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.dates as mdates
from matplotlib.dates import date2num
from collections import deque
from datetime import datetime

# Kafka setup
topic = "stock_index_topic"
consumer = KafkaConsumer(
    topic,
    bootstrap_servers='127.0.0.1:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Deques to store real-time data
maxlen = 100  # Ensure sufficient history for ES/VAR estimation
time_data = deque(maxlen=maxlen)
price_data = deque(maxlen=maxlen)
stock_code = "SH300"  # Can be changed to 'STI' or 'SP500'

# Setup Matplotlib plot
fig, ax = plt.subplots()
line, = ax.plot([], [], lw=2)
var_line = ax.axhline(color='red', linestyle='--', label='VaR Threshold')
es_line = ax.axhline(color='orange', linestyle='--', label='ES Threshold')

ax.set_title(f"Live Price for {stock_code}")
ax.set_xlabel("Time")
ax.set_ylabel("Price")
ax.grid(True)
ax.legend()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
fig.autofmt_xdate()

def update_plot(frame):
    try:
        msg = next(consumer)
        data = msg.value

        if data['stock_code'] == stock_code:
            # Convert string to datetime object
            timestamp = datetime.strptime(data['date'] + ' ' + data['time'], '%Y-%m-%d %H:%M:%S')
            price = data.get('price') or data.get('last_price')


            print(f"📈 SH300 Live — Time: {timestamp}, Price: {price}")
            time_data.append(timestamp)
            price_data.append(price)
            print(f"🔢 Time points: {len(time_data)}, Price points: {len(price_data)}")

            # ✅ Alert check with live price history
            alert_msg, var_threshold, es_threshold = check_alert_conditions(data, list(price_data), alpha=0.05, return_thresholds=True)
            if alert_msg:
                print(f"🚨 {alert_msg}")

            # Update VaR/ES lines
            if var_threshold is not None:
                var_line.set_ydata([var_threshold] * 2)
            if es_threshold is not None:
                es_line.set_ydata([es_threshold] * 2)

            # Plot update
            if len(price_data) > 1:
                ax.set_ylim(min(price_data) * 0.99, max(price_data) * 1.01)
                ax.set_xlim(date2num(time_data[0]), date2num(time_data[-1]))

            line.set_data(list(time_data), list(price_data))
            ax.relim()
            ax.autoscale_view()
            fig.canvas.draw()


    except Exception as e:
        print(f"Error: {e}")
    return line, var_line, es_line

# Attach animation
ani = animation.FuncAnimation(fig, update_plot, interval=1000, save_count=100)

plt.tight_layout()
plt.show()
