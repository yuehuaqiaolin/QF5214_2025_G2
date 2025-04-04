import numpy as np
from collections import deque
from email_alert import send_email

# Rolling window of returns
return_window = {
    "SH300": deque(maxlen=100),
    "SP500": deque(maxlen=100),
    "STI": deque(maxlen=100),
}

# Define confidence level (alpha)
ALPHA = 0.05

def check_alert_conditions(data, price_history=None, alpha=ALPHA, return_thresholds=False):
    stock = data['stock_code']
    price = data['price']

    # Skip if we don't track this stock
    if stock not in return_window:
        return (None, None, None) if return_thresholds else None

    # Update returns
    history = return_window[stock]
    if len(history) > 0:
        last_price = history[-1]
        ret = (price - last_price) / last_price
        history.append(price)
    else:
        ret = 0.0
        history.append(price)

    # Compute thresholds only if we have enough data
    if len(prices) < 2:
        return (None, None, None) if return_thresholds else None

    prices = price_history
    returns = np.diff(prices)
    sorted_returns = np.sort(returns)
    index = int(alpha * len(sorted_returns))
    var = sorted_returns[:index][-1]
    var_threshold = sorted_returns[index]
    es_threshold = sorted_returns[:index].mean()  # Expected Shortfall

    if index == 0:
        return (None, None, None) if return_thresholds else None

    alert_msg = None
    if ret < var_threshold:
        alert_msg = f"⚠️ VaR Breach! Return: {ret:.4f} < VaR: {var_threshold:.4f}"
    elif ret < es_threshold:
        alert_msg = f"🚨 ES Breach! Return: {ret:.4f} < ES: {es_threshold:.4f}"

    if alert_msg:
        send_email(f"{stock} Risk Alert", alert_msg)

    if return_thresholds:
        return alert_msg, var_threshold, es_threshold
    else:
        return alert_msg