# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 15:06:41 2025

@author: DUAN
"""
import json
from kafka import KafkaConsumer
from datetime import datetime

def safe_json_deserializer(v):
    try:
        decoded_value = v.decode('utf-8')
        return json.loads(decoded_value)
    except Exception as e:
        print(f"[ERROR] Unable to parse data: {v}, error info: {e}")
        return None  # Prevent crash

consumer = KafkaConsumer(
    'stock_index_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=f'read-all-history-group-{datetime.now().timestamp()}',
    value_deserializer=safe_json_deserializer
)

for message in consumer:
    print(f"[DEBUG] Received message: {message.value}")  # Ensure Consumer is receiving data
