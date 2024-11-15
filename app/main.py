from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import redis
import aiohttp
import os
from typing import Optional

app = FastAPI()
redis_client = redis.Redis(host='redis', port=6379, db=0)

class Alert(BaseModel):
    message: str
    time_delay: Optional[int] = 0
    count_delay: Optional[int] = 0
    closing_delay: Optional[int] = 0

@app.post("/alert")
async def create_alert(alert: Alert):
    current_time = datetime.now().timestamp()
    
    # If all delays are 0, send message immediately
    if alert.time_delay == 0 and alert.count_delay == 0 and alert.closing_delay == 0:
        await send_telegram_message(f"Alert: {alert.message}")
        return {"status": "sent"}

    # Store alert in Redis
    alert_id = f"alert:{current_time}"
    alert_data = {
        "message": alert.message,
        "time_delay": alert.time_delay,
        "count_delay": alert.count_delay,
        "closing_delay": alert.closing_delay,
        "timestamp": current_time,
        "status": "pending"
    }
    
    redis_client.hmset(alert_id, alert_data)
    return {"status": "scheduled", "alert_id": alert_id}

async def send_telegram_message(message: str):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    async with aiohttp.ClientSession() as session:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        params = {
            "chat_id": chat_id,
            "text": message
        }
        async with session.post(url, json=params) as response:
            if response.status != 200:
                raise HTTPException(status_code=500, detail="Failed to send Telegram message")
