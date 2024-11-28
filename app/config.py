import os
import json
from datetime import time

def load_json_config(filename):
    config_path = f"/app/config/{filename}"
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    return {}

def parse_time(time_str):
    hours, minutes = map(int, time_str.split(':'))
    return time(hours, minutes)

def process_schedule_config(config):
    if 'daily' in config:
        for schedule in config['daily']:
            schedule['from'] = parse_time(schedule['from'])
            schedule['to'] = parse_time(schedule['to'])
    return config

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

ALERT_CONFIGS = load_json_config('alert_config.json') or {
    "time_delay": 0,
    "closing_delay": 0
}

schedule_config = load_json_config('schedule_config.json') or {}
SCHEDULE_CONFIGS = {
    condition: process_schedule_config(config)
    for condition, config in schedule_config.items()
}

