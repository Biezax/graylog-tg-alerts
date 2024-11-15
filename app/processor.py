import redis
import asyncio
import os
from datetime import datetime
import aiohttp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_client = redis.Redis(host='redis', port=6379, db=0)

async def send_telegram_message(message: str):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    async with aiohttp.ClientSession() as session:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        params = {
            "chat_id": chat_id,
            "text": message
        }
        await session.post(url, json=params)

async def process_alerts():
    while True:
        try:
            current_time = datetime.now().timestamp()
            logger.info("Starting alerts processing cycle")
            
            # Get all pending alerts
            for key in redis_client.keys("alert:*"):
                alert_data = redis_client.hgetall(key)
                
                if not alert_data:
                    continue
                    
                timestamp = float(alert_data[b'timestamp'])
                time_delay = int(alert_data[b'time_delay'])
                count_delay = int(alert_data[b'count_delay'])
                closing_delay = int(alert_data[b'closing_delay'])
                message = alert_data[b'message'].decode('utf-8')
                status = alert_data[b'status'].decode('utf-8')

                if status == "pending":
                    if time_delay > 0 and (current_time - timestamp) >= time_delay:
                        await send_telegram_message(f"Alert start: {message}")
                        redis_client.hset(key, "status", "sent")
                        logger.info(f"Sent start message for alert {key}")
                    
                    if count_delay > 0:
                        similar_alerts = len(redis_client.keys(f"alert:{int(timestamp)-60}:*"))
                        if similar_alerts >= count_delay:
                            await send_telegram_message(f"Alert start (count threshold reached): {message}")
                            redis_client.hset(key, "status", "sent")
                            logger.info(f"Sent count-based message for alert {key}")

                elif status == "sent" and closing_delay > 0:
                    if (current_time - timestamp) >= closing_delay:
                        await send_telegram_message(f"Alert end: {message}")
                        redis_client.delete(key)
                        logger.info(f"Sent closing message and deleted alert {key}")

            logger.info("Finished processing cycle")
            await asyncio.sleep(60)  # Wait for 60 seconds before next cycle
            
        except Exception as e:
            logger.error(f"Error in process_alerts: {e}")
            await asyncio.sleep(60)  # Wait even if there's an error

if __name__ == "__main__":
    logger.info("Starting alert processor service")
    asyncio.run(process_alerts())
