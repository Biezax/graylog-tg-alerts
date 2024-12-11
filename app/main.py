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
from config import ALERT_CONFIGS, SCHEDULE_CONFIGS, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, LOG_LEVEL
import os
import json
from string import Template

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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

async def send_telegram_message(message: str, reply_to_message_id: int = None):
    async with aiohttp.ClientSession() as session:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        params = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        if reply_to_message_id:
            params["reply_to_message_id"] = reply_to_message_id
            
        try:
            logger.debug(f"Request params: {params}")
            async with session.post(url, json=params) as response:
                response_data = await response.json()
                if response.status != 200:
                    logger.error(f"Failed to send Telegram message. Status: {response.status}, Response: {response_data}")
                    raise HTTPException(status_code=500, detail=f"Failed to send Telegram message: {response_data}")
                logger.info("Message sent successfully")
                logger.debug(f"Telegram API response: {response_data}")
                return response_data.get('result', {}).get('message_id')
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
    logger.info("=== CHECKING SUPPRESS RULES ===")
    logger.info(f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    for condition, schedule in SCHEDULE_CONFIGS.items():
        try:
            logger.info(f"Checking condition: {condition}")
            if eval(condition, {"__builtins__": {}}, alert_data):
                logger.info(f"Condition '{condition}' matched")
                
                if "daily" in schedule:
                    logger.info("Checking daily schedules")
                    for time_range in schedule["daily"]:
                        logger.info(f"Checking time range: {time_range['from']} - {time_range['to']}")
                        if time_range["from"] <= current_time.time() <= time_range["to"]:
                            logger.info(f"Alert suppressed: current time {current_time.time()} is within range")
                            return True
                        logger.info("Time range not matched")
                
                if "monthly" in schedule:
                    logger.info("Checking monthly schedules")
                    for monthly_rule in schedule["monthly"]:
                        logger.info(f"Checking monthly rule: week={monthly_rule['week']}, weekday={monthly_rule['weekday']}")
                        if is_specified_day(current_time, 
                                         monthly_rule["week"], 
                                         monthly_rule["weekday"]):
                            logger.info(f"Alert suppressed: matches monthly schedule")
                            return True
                        logger.info("Monthly rule not matched")
            else:
                logger.info(f"Condition '{condition}' not matched")
                
        except Exception as e:
            logger.error(f"Error evaluating schedule condition: {e}")
            continue
    
    logger.info("No suppress rules matched")
    return False

async def process_alerts():
    while True:
        try:
            current_time = datetime.now().timestamp()
            logger.debug("Starting alerts processing cycle")
            
            conn = get_db()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE event_ended = 0
            """)
            alerts = cursor.fetchall()
            logger.debug(f"Found {len(alerts)} active alerts to process")
            
            for alert in alerts:
                event_id, start_date, event_title, last_timestamp, event_started, event_ended, first_message_id = alert
                start_date = float(start_date) if start_date else current_time
                last_timestamp = float(last_timestamp) if last_timestamp else current_time
                
                time_delay = ALERT_CONFIGS["time_delay"]
                closing_delay = ALERT_CONFIGS["closing_delay"]
                
                if (current_time - last_timestamp) / 60 >= (time_delay + closing_delay):
                    if event_started:
                        logger.info(f"Started alert {event_id} expired, marking as ended")
                        cursor.execute("""
                            UPDATE alerts 
                            SET event_ended = 1
                            WHERE event_id = ? AND start_date = ?
                        """, (event_id, start_date))
                        
                        message = f"✅ <b>Event has ended</b>\n\n"
                        message += f"No new alerts received for {time_delay + closing_delay} minutes.\n"
                        message += f"Event: {event_title}\n"
                        message += f"Duration: {datetime.fromtimestamp(start_date).strftime('%Y-%m-%d %H:%M:%S')} - {datetime.fromtimestamp(last_timestamp).strftime('%Y-%m-%d %H:%M:%S')}"
                        logger.info(f"Sending end event message as reply to message {first_message_id}")
                        await send_telegram_message(message, reply_to_message_id=first_message_id)
                    else:
                        logger.info(f"Not started alert {event_id} expired, marking as started and ended")
                        cursor.execute("""
                            UPDATE alerts 
                            SET event_started = 1, event_ended = 1
                            WHERE event_id = ? AND start_date = ?
                        """, (event_id, start_date))
                    conn.commit()

            conn.close()
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Error in process_alerts: {e}", exc_info=True)
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    try:
        logger.info("Initializing application")
        init_db()
        logger.info("Database initialized")
        
        logger.info("Starting alert processor")
        global alert_processor_task
        alert_processor_task = asyncio.create_task(process_alerts())
        logger.info("Alert processor started successfully")
    except Exception as e:
        logger.error(f"Error during startup: {e}", exc_info=True)
        raise

@app.on_event("shutdown")
async def shutdown_event():
    if alert_processor_task:
        alert_processor_task.cancel()
        logger.info("Alert processor stopped")

def format_message(template: Template, data: dict) -> str:
    try:
        logger.info("Formatting message")
        logger.debug("=== MESSAGE INPUT DATA ===")
        logger.debug(json.dumps(data, indent=2, default=str))
        
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

        logger.debug("Template variables:")
        logger.debug(json.dumps(template_vars, indent=2))
        
        result = template.safe_substitute(template_vars)
        result = '\n'.join(line for line in result.splitlines() if line.strip())
        
        logger.debug("=== FINAL MESSAGE ===")
        logger.debug(result)
        return result
        
    except Exception as e:
        logger.error(f"Error formatting message: {str(e)}")
        raise

@app.post("/alert")
async def create_alert(alert: Alert):
    try:
        event_id = alert.event_definition_id
        if not event_id:
            logger.error("Missing event_definition_id")
            error_message = "❌ <b>Error processing alert</b>\n\n"
            error_message += "event_definition_id is missing in the alert data\n"
            if alert.event_definition_title:
                error_message += f"Alert title: {alert.event_definition_title}"
            await send_telegram_message(error_message)
            return {"status": "error", "detail": "event_definition_id is required"}

        logger.info(f"Processing alert: {event_id} - {alert.event_definition_title}")
        logger.debug("Alert data:")
        logger.debug(json.dumps(alert.dict(), indent=2, default=str))
        
        alert_data = {
            "alert": alert.dict(),
            "event": alert.event.dict() if alert.event else {}
        }
        logger.debug("Checking alert suppression rules")
        if should_suppress_alert(alert_data):
            logger.info(f"Alert {event_id} suppressed by rules")
            return {"status": "suppressed"}
        
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
                message_id = await send_telegram_message(message)
                logger.info(f"First alert message sent with ID: {message_id}")
                
                cursor.execute("""
                    INSERT INTO alerts (
                        event_id, event_title, start_date,
                        last_timestamp, event_started, event_ended,
                        first_message_id
                    ) VALUES (?, ?, ?, ?, 0, 0, ?)
                """, (
                    event_id, 
                    alert.event_definition_title,
                    current_time,
                    current_time,
                    message_id
                ))
                conn.commit()
                return {"status": "first_sent_and_registered"}
            
            last_timestamp = existing_alert[3]
            time_diff = (current_time - last_timestamp) / 60
            event_started = existing_alert[4]
            first_message_id = existing_alert[6]
            
            logger.debug(f"Checking alert: time_diff={time_diff}, event_started={event_started}, first_message_id={first_message_id}")
            
            if time_diff <= ALERT_CONFIGS["time_delay"] and not event_started:
                # Второй алерт в пределах time_delay - отправляем уведомление о начале как ответ на первое сообщение
                message = "🚨 <b>Beginning of a recurring event detected!</b>\n\n"
                message += f"Event: {alert.event_definition_title}"
                logger.info(f"Sending start event message as reply to message {first_message_id}")
                await send_telegram_message(message, reply_to_message_id=first_message_id)
                
                cursor.execute("""
                    UPDATE alerts 
                    SET event_started = 1, last_timestamp = ?
                    WHERE event_id = ? AND start_date = ?
                """, (current_time, event_id, existing_alert[1]))
                conn.commit()
                return {"status": "event_started"}
            
            elif time_diff > ALERT_CONFIGS["time_delay"]:
                # Закрываем старый алерт
                cursor.execute("""
                    UPDATE alerts 
                    SET event_ended = 1
                    WHERE event_id = ? AND start_date = ?
                """, (event_id, existing_alert[1]))
                
                # Создаем новый алерт
                message = format_message(template, alert.dict())
                message_id = await send_telegram_message(message)
                logger.info(f"Creating new alert with ID: {message_id}")
                
                cursor.execute("""
                    INSERT INTO alerts (
                        event_id, event_title, start_date,
                        last_timestamp, event_started, event_ended,
                        first_message_id
                    ) VALUES (?, ?, ?, ?, 0, 0, ?)
                """, (
                    event_id, 
                    alert.event_definition_title,
                    current_time,
                    current_time,
                    message_id
                ))
                conn.commit()
                return {"status": "first_sent_and_registered"}
            
            cursor.execute("""
                UPDATE alerts 
                SET last_timestamp = ?
                WHERE event_id = ? AND start_date = ?
            """, (current_time, event_id, existing_alert[1]))
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
