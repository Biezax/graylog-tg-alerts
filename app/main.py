from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, time
import calendar
import aiohttp
from typing import Optional, List, Dict, Any
from database import init_db, get_db
import asyncio
import logging
from fastapi.background import BackgroundTasks
from config import ALERT_CONFIGS, SCHEDULE_CONFIGS, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import os
import json
from string import Template

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
alert_processor_task = None

class BacklogMessage(BaseModel):
    id: Optional[str]
    message: Optional[str]

class Event(BaseModel):
    id: Optional[str]
    event_definition_type: Optional[str]
    event_definition_id: Optional[str]
    origin_context: Optional[str]
    timestamp: Optional[str]
    timestamp_processing: Optional[str]
    timerange_start: Optional[str]
    timerange_end: Optional[str]
    streams: Optional[List[str]]
    source_streams: Optional[List[str]]
    message: str
    source: Optional[str]
    key_tuple: Optional[List[str]]
    key: Optional[str]
    priority: Optional[int]
    alert: Optional[bool]
    fields: Optional[Dict[str, Any]]
    group_by_fields: Optional[Dict[str, Any]]
    replay_info: Optional[Any]

class Alert(BaseModel):
    event_definition_id: Optional[str]
    event_definition_type: Optional[str]
    event_definition_title: Optional[str]
    event_definition_description: Optional[str]
    job_definition_id: Optional[str]
    job_trigger_id: Optional[str]
    event: Event
    backlog: Optional[List[BacklogMessage]] = []

def load_message_template():
    with open("/app/config/message_template.txt", "r") as f:
        return Template(f.read())

async def send_telegram_message(message: str):
    async with aiohttp.ClientSession() as session:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        params = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        try:
            logger.debug(f"Sending Telegram message. URL: {url}, Params: {params}")
            async with session.post(url, json=params) as response:
                response_text = await response.text()
                if response.status != 200:
                    logger.error(f"Failed to send Telegram message. Status: {response.status}, Response: {response_text}")
                    raise HTTPException(status_code=500, detail=f"Failed to send Telegram message: {response_text}")
                logger.info(f"Successfully sent Telegram message. Response: {response_text}")
        except Exception as e:
            logger.error(f"Error while sending Telegram message: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to send Telegram message: {str(e)}")

def get_weekday_number(weekday_name: str) -> int:
    weekdays = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }
    return weekdays.get(weekday_name.lower(), 0)

def is_specified_day(date: datetime, week: int, weekday: str) -> bool:
    weekday_num = get_weekday_number(weekday)
    
    month_days = [day for day in range(1, calendar.monthrange(date.year, date.month)[1] + 1)
                 if datetime(date.year, date.month, day).weekday() == weekday_num]
    
    if week < 0:
        target_day = month_days[week]
    else:
        try:
            target_day = month_days[week - 1]
        except IndexError:
            return False
            
    return date.day == target_day

def should_suppress_alert(alert_data):
    current_time = datetime.now()
    
    for condition, schedule in SCHEDULE_CONFIGS.items():
        try:
            if eval(condition, {"__builtins__": {}}, alert_data):
                if "daily" in schedule:
                    for time_range in schedule["daily"]:
                        if time_range["from"] <= current_time.time() <= time_range["to"]:
                            return True
                
                if "monthly" in schedule:
                    for monthly_rule in schedule["monthly"]:
                        if is_specified_day(current_time, 
                                         monthly_rule["week"], 
                                         monthly_rule["weekday"]):
                            return True
        except Exception as e:
            logger.error(f"Error evaluating schedule condition: {e}")
            continue
    
    return False

async def process_alerts():
    while True:
        try:
            current_time = datetime.now().timestamp()
            logger.info("Starting alerts processing cycle")
            
            conn = get_db()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM alerts WHERE event_ended = 0")
            alerts = cursor.fetchall()
            logger.info(f"Found {len(alerts)} alerts to process")
            
            for alert in alerts:
                event_id, event_title, start_date, end_date, last_timestamp, event_started, event_ended = alert
                time_delay = ALERT_CONFIGS["time_delay"]
                closing_delay = ALERT_CONFIGS["closing_delay"]
                
                if (current_time - last_timestamp) / 60 >= (time_delay + closing_delay):
                    logger.info(f"Alert {event_id} expired, marking as ended")
                    cursor.execute("""
                        UPDATE alerts 
                        SET event_ended = 1, end_date = ?
                        WHERE event_id = ?
                    """, (current_time, event_id))
                    conn.commit()
                    
                    message = f"✅ <b>Event has ended</b>\n\n"
                    message += f"No new alerts received for {time_delay + closing_delay} minutes.\n"
                    message += f"Event: {event_title}\n"
                    message += f"Duration: {datetime.fromtimestamp(start_date).strftime('%Y-%m-%d %H:%M:%S')} - {datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')}"
                    await send_telegram_message(message)
            
            conn.close()
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Error in process_alerts: {e}", exc_info=True)
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    init_db()
    global alert_processor_task
    alert_processor_task = asyncio.create_task(process_alerts())
    logger.info("Alert processor started")

@app.on_event("shutdown")
async def shutdown_event():
    if alert_processor_task:
        alert_processor_task.cancel()
        logger.info("Alert processor stopped")

def format_message(template: Template, data: dict) -> str:
    try:
        logger.info("=== FORMAT MESSAGE INPUT DATA ===")
        logger.info(json.dumps(data, indent=2, default=str))
        
        def sanitize(text):
            return str(text).replace('<', '&lt;').replace('>', '&gt;') if text else "N/A"

        template_vars = {}
        
        # Обработка корневых полей
        for key, value in data.items():
            if not isinstance(value, (dict, list)):
                template_vars[key] = sanitize(value)
        
        # Обработка полей события
        if 'event' in data and isinstance(data['event'], dict):
            event = data['event']
            for key, value in event.items():
                if isinstance(value, list):
                    template_vars[key] = ', '.join(str(x) for x in value) or "N/A"
                else:
                    template_vars[key] = sanitize(value)
            
            # Специальная обработка period
            if event.get('timerange_start') or event.get('timerange_end'):
                template_vars['period'] = f"• Period: {event.get('timerange_start', 'N/A')} - {event.get('timerange_end', 'N/A')}"
            else:
                template_vars['period'] = ""
                
            # Специальная обработка fields
            if event.get('fields'):
                fields = [f"• {k}: {sanitize(v)}" for k, v in event['fields'].items()]
                template_vars['fields'] = '\n'.join(fields)
            else:
                template_vars['fields'] = "N/A"
        
        # Обработка backlog
        if data.get('backlog'):
            details = ["📝 Backlog Details:"]
            details.extend(f"<code>{sanitize(msg['message'])}</code>" for msg in data['backlog'])
            template_vars['details'] = '\n'.join(details)
        else:
            template_vars['details'] = ""

        logger.info("Template variables prepared:")
        logger.info(json.dumps(template_vars, indent=2))
        
        # Удаляем лишние пустые строки
        result = template.safe_substitute(template_vars)
        result = '\n'.join(line for line in result.splitlines() if line.strip())
        
        logger.info("=== FINAL MESSAGE ===")
        logger.info(result)
        return result
        
    except Exception as e:
        logger.error(f"Error formatting message: {str(e)}, Data: {data}")
        raise

@app.post("/alert")
async def create_alert(alert: Alert):
    try:
        event_id = alert.event_definition_id
        if not event_id:
            error_message = "❌ <b>Error processing alert</b>\n\n"
            error_message += "event_definition_id is missing in the alert data\n"
            if alert.event_definition_title:
                error_message += f"Alert title: {alert.event_definition_title}"
            await send_telegram_message(error_message)
            return {"status": "error", "detail": "event_definition_id is required"}

        logger.info("=== INCOMING ALERT DATA ===")
        logger.info(alert)
        logger.info(json.dumps(alert.dict(), indent=2, default=str))
        
        if ALERT_CONFIGS["time_delay"] == 0:
            template = load_message_template()
            message = format_message(template, alert.dict())
            await send_telegram_message(message)
            return {"status": "sent"}

        current_time = datetime.now().timestamp()
        conn = get_db()
        cursor = conn.cursor()
        template = load_message_template()
        
        try:
            # Проверяем существование активного алерта (не завершенного)
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE event_id = ? AND event_ended = 0
            """, (event_id,))
            existing_alert = cursor.fetchone()
            
            if not existing_alert:
                # Первое появление алерта - отправляем сообщение и регистрируем
                message = format_message(template, alert.dict())
                await send_telegram_message(message)
                
                cursor.execute("""
                    INSERT INTO alerts (
                        event_id, event_title, start_date, end_date,
                        last_timestamp, event_started, event_ended
                    ) VALUES (?, ?, ?, ?, ?, 0, 0)
                """, (
                    event_id, 
                    alert.event_definition_title,
                    current_time,
                    current_time,
                    current_time
                ))
                conn.commit()
                return {"status": "first_sent_and_registered"}
            
            last_timestamp = existing_alert[4]
            time_diff = (current_time - last_timestamp) / 60
            event_started = existing_alert[5]
            
            if time_diff <= ALERT_CONFIGS["time_delay"] and not event_started:
                # Второй алерт в пределах time_delay - отправляем простое уведомление о начале
                message = "🚨 <b>Beginning of a recurring event detected!</b>\n\n"
                message += f"Event: {alert.event_definition_title}"
                await send_telegram_message(message)
                
                cursor.execute("""
                    UPDATE alerts 
                    SET event_started = 1, last_timestamp = ?
                    WHERE event_id = ?
                """, (current_time, event_id))
                conn.commit()
                return {"status": "event_started"}
            
            # Просто обновляем timestamp для последующих сообщений
            cursor.execute("""
                UPDATE alerts 
                SET last_timestamp = ?
                WHERE event_id = ?
            """, (current_time, event_id))
            conn.commit()
            return {"status": "updated"}
            
        finally:
            conn.close()
            
    except Exception as e:
        error_message = "❌ <b>Error processing alert</b>\n\n"
        error_message += f"Error: {str(e)}\n"
        if alert and alert.event_definition_title:
            error_message += f"Alert title: {alert.event_definition_title}"
        await send_telegram_message(error_message)
        logger.error(f"Error in create_alert: {str(e)}")
        return {"status": "error", "detail": str(e)}
