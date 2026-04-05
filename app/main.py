import asyncio
import calendar
import html
import json
import logging
import secrets
from contextlib import asynccontextmanager, suppress
from datetime import datetime, time
from string import Template
from typing import Any, Dict, List, Optional

import aiohttp
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from config import (
    ALERT_WEBHOOK_TOKEN,
    LOG_LEVEL,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    AlertConfig,
    ScheduleRule,
    get_config_path,
    load_alert_config,
    load_schedule_configs,
    validate_runtime_env,
)
from database import init_db


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class TelegramSendError(RuntimeError):
    pass


class BacklogMessage(BaseModel):
    id: Optional[str] = None
    message: Optional[str] = None


class Event(BaseModel):
    id: Optional[str] = None
    event_definition_type: Optional[str] = None
    event_definition_id: Optional[str] = None
    origin_context: Optional[str] = None
    timestamp: Optional[str] = None
    timestamp_processing: Optional[str] = None
    timerange_start: Optional[str] = None
    timerange_end: Optional[str] = None
    streams: Optional[List[str]] = None
    source_streams: Optional[List[str]] = None
    message: str
    source: Optional[str] = None
    key_tuple: Optional[List[str]] = None
    key: Optional[str] = None
    priority: Optional[int] = None
    alert: Optional[bool] = None
    fields: Optional[Dict[str, Any]] = None
    group_by_fields: Optional[Dict[str, Any]] = None
    replay_info: Optional[Any] = None


class Alert(BaseModel):
    event_definition_id: Optional[str] = None
    event_definition_type: Optional[str] = None
    event_definition_title: Optional[str] = None
    event_definition_description: Optional[str] = None
    job_definition_id: Optional[str] = None
    job_trigger_id: Optional[str] = None
    event: Event
    backlog: List[BacklogMessage] = Field(default_factory=list)


def load_message_template() -> Template:
    template_path = get_config_path("message_template.txt")
    with template_path.open("r", encoding="utf-8") as file_obj:
        return Template(file_obj.read())


def sanitize(value: Any) -> str:
    if value is None or value == "":
        return "N/A"
    return html.escape(str(value))


def get_weekday_number(weekday_name: str) -> int:
    weekdays = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    return weekdays[weekday_name.lower()]


def is_specified_day(date: datetime, week: int, weekday: str) -> bool:
    weekday_num = get_weekday_number(weekday)
    month_days = [
        day
        for day in range(1, calendar.monthrange(date.year, date.month)[1] + 1)
        if datetime(date.year, date.month, day).weekday() == weekday_num
    ]

    if week < 0:
        target_day = month_days[week]
    else:
        try:
            target_day = month_days[week - 1]
        except IndexError:
            return False

    return date.day == target_day


def time_to_minutes(value: time) -> int:
    return value.hour * 60 + value.minute


def is_time_in_window(current_time: time, start_time: time, end_time: time) -> bool:
    current_minutes = time_to_minutes(current_time)
    start_minutes = time_to_minutes(start_time)
    end_minutes = time_to_minutes(end_time)

    if start_minutes <= end_minutes:
        return start_minutes <= current_minutes <= end_minutes
    return current_minutes >= start_minutes or current_minutes <= end_minutes


def match_schedule_rule(rule: ScheduleRule, alert_data: Dict[str, Any]) -> bool:
    event_data = alert_data.get("event") or {}
    alert_root = alert_data.get("alert") or {}
    match = rule.match

    if match.streams_any:
        streams = set(event_data.get("streams") or [])
        if not streams.intersection(match.streams_any):
            return False

    if match.source_streams_any:
        source_streams = set(event_data.get("source_streams") or [])
        if not source_streams.intersection(match.source_streams_any):
            return False

    priority = event_data.get("priority")
    if match.priority_lt is not None and (priority is None or priority >= match.priority_lt):
        return False
    if match.priority_lte is not None and (priority is None or priority > match.priority_lte):
        return False
    if match.priority_gt is not None and (priority is None or priority <= match.priority_gt):
        return False
    if match.priority_gte is not None and (priority is None or priority < match.priority_gte):
        return False

    if match.event_definition_id_in:
        event_definition_id = alert_root.get("event_definition_id")
        if event_definition_id not in match.event_definition_id_in:
            return False

    return True


def should_suppress_alert(
    alert_data: Dict[str, Any],
    schedule_rules: List[ScheduleRule],
    current_datetime: Optional[datetime] = None,
) -> bool:
    now = current_datetime or datetime.now()

    for index, rule in enumerate(schedule_rules, start=1):
        if not match_schedule_rule(rule, alert_data):
            logger.debug("Suppress rule %s did not match alert payload", index)
            continue

        daily_match = any(is_time_in_window(now.time(), item.from_time, item.to_time) for item in rule.daily)
        monthly_match = any(is_specified_day(now, item.week, item.weekday) for item in rule.monthly)

        if daily_match or monthly_match:
            logger.info("Alert suppressed by rule %s", index)
            return True

        logger.debug("Suppress rule %s matched payload but not time constraints", index)

    return False


def format_message(template: Template, data: Dict[str, Any]) -> str:
    logger.debug("Formatting message from payload: %s", json.dumps(data, indent=2, default=str))

    template_vars: Dict[str, str] = {}

    for key, value in data.items():
        if not isinstance(value, (dict, list)):
            template_vars[key] = sanitize(value)

    event_data = data.get("event")
    if isinstance(event_data, dict):
        for key, value in event_data.items():
            if isinstance(value, list):
                template_vars[key] = ", ".join(sanitize(item) for item in value) or "N/A"
            else:
                template_vars[key] = sanitize(value)

        if event_data.get("timerange_start") or event_data.get("timerange_end"):
            template_vars["period"] = (
                "• Period: "
                f"{sanitize(event_data.get('timerange_start'))} - {sanitize(event_data.get('timerange_end'))}"
            )
        else:
            template_vars["period"] = ""

        if event_data.get("fields"):
            fields = [f"• {sanitize(key)}: {sanitize(value)}" for key, value in event_data["fields"].items()]
            template_vars["fields"] = "\n".join(fields)
        else:
            template_vars["fields"] = "N/A"

    backlog = data.get("backlog") or []
    if backlog:
        details = ["📝 Backlog Details:"]
        for item in backlog:
            message = item.get("message") if isinstance(item, dict) else None
            details.append(f"<code>{sanitize(message)}</code>")
        template_vars["details"] = "\n".join(details)
    else:
        template_vars["details"] = ""

    result = template.safe_substitute(template_vars)
    return "\n".join(line for line in result.splitlines() if line.strip())


def verify_alert_token(header_value: Optional[str], expected_token: str) -> None:
    if not header_value or not secrets.compare_digest(header_value, expected_token):
        raise HTTPException(status_code=401, detail="Unauthorized")


def get_expiry_minutes(alert_config: AlertConfig, event_started: bool) -> int:
    if event_started:
        return alert_config.time_delay + alert_config.closing_delay
    return alert_config.time_delay


def is_alert_expired(alert_row, alert_config: AlertConfig, current_timestamp: float) -> bool:
    expiry_minutes = get_expiry_minutes(alert_config, bool(alert_row["event_started"]))
    if expiry_minutes == 0:
        return True

    last_timestamp_raw = alert_row["last_timestamp"]
    last_timestamp = current_timestamp if last_timestamp_raw is None else float(last_timestamp_raw)
    return (current_timestamp - last_timestamp) / 60 >= expiry_minutes


def build_event_ended_message(alert_row, alert_config: AlertConfig) -> str:
    duration = (
        f"{datetime.fromtimestamp(float(alert_row['start_date'])).strftime('%Y-%m-%d %H:%M:%S')} - "
        f"{datetime.fromtimestamp(float(alert_row['last_timestamp'])).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    silence_minutes = get_expiry_minutes(alert_config, True)
    return (
        "✅ <b>Event has ended</b>\n\n"
        f"No new alerts received for {silence_minutes} minutes.\n"
        f"Event: {sanitize(alert_row['event_title'])}\n"
        f"Duration: {duration}"
    )


def get_closing_notification(alert_row, alert_config: AlertConfig) -> Optional[Dict[str, Any]]:
    if not bool(alert_row["event_started"]):
        return None

    return {
        "message": build_event_ended_message(alert_row, alert_config),
        "reply_to_message_id": alert_row["first_message_id"],
        "event_id": alert_row["event_id"],
    }


async def send_telegram_message(app: FastAPI, message: str, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id

    logger.debug("Telegram payload: %s", payload)

    try:
        async with app.state.http_session.post(app.state.telegram_api_url, json=payload) as response:
            response_text = await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        raise TelegramSendError(str(exc)) from exc

    try:
        response_data = json.loads(response_text) if response_text else {}
    except json.JSONDecodeError:
        response_data = {"raw": response_text}

    if response.status != 200 or not response_data.get("ok", response.status == 200):
        raise TelegramSendError(f"status={response.status}, response={response_data}")

    logger.info("Message sent successfully")
    return response_data.get("result", {}).get("message_id")


async def notify_processing_error(app: FastAPI, alert_title: Optional[str], error_message: str) -> None:
    message = "❌ <b>Error processing alert</b>\n\n"
    message += f"Error: {sanitize(error_message)}\n"
    if alert_title:
        message += f"Alert title: {sanitize(alert_title)}"

    try:
        await send_telegram_message(app, message)
    except Exception as exc:
        logger.error("Failed to send error notification: %s", exc)


async def fetch_active_alert(db, event_id: str):
    async with db.execute(
        """
        SELECT event_id, start_date, event_title, last_timestamp, event_started, event_ended, first_message_id
        FROM alerts
        WHERE event_id = ? AND event_ended = 0
        ORDER BY start_date DESC
        LIMIT 1
        """,
        (event_id,),
    ) as cursor:
        return await cursor.fetchone()


async def fetch_active_alerts(db):
    async with db.execute(
        """
        SELECT event_id, start_date, event_title, last_timestamp, event_started, event_ended, first_message_id
        FROM alerts
        WHERE event_ended = 0
        ORDER BY start_date ASC
        """
    ) as cursor:
        return await cursor.fetchall()


async def insert_alert(db, event_id: str, event_title: Optional[str], current_timestamp: float, message_id: Optional[int]) -> None:
    await db.execute(
        """
        INSERT INTO alerts (
            event_id, event_title, start_date,
            last_timestamp, event_started, event_ended,
            first_message_id
        ) VALUES (?, ?, ?, ?, 0, 0, ?)
        """,
        (
            event_id,
            event_title,
            current_timestamp,
            current_timestamp,
            message_id,
        ),
    )
    await db.commit()


async def mark_alert_started(db, event_id: str, start_date: float, current_timestamp: float) -> None:
    await db.execute(
        """
        UPDATE alerts
        SET event_started = 1, last_timestamp = ?
        WHERE event_id = ? AND start_date = ?
        """,
        (current_timestamp, event_id, start_date),
    )
    await db.commit()


async def update_alert_timestamp(db, event_id: str, start_date: float, current_timestamp: float) -> None:
    await db.execute(
        """
        UPDATE alerts
        SET last_timestamp = ?
        WHERE event_id = ? AND start_date = ?
        """,
        (current_timestamp, event_id, start_date),
    )
    await db.commit()


async def mark_alert_ended(db, event_id: str, start_date: float) -> None:
    await db.execute(
        """
        UPDATE alerts
        SET event_ended = 1
        WHERE event_id = ? AND start_date = ?
        """,
        (event_id, start_date),
    )
    await db.commit()


async def close_alert_record(app: FastAPI, alert_row) -> None:
    await mark_alert_ended(app.state.db, alert_row["event_id"], alert_row["start_date"])


async def send_closing_notification(app: FastAPI, notification: Dict[str, Any]) -> None:
    try:
        await send_telegram_message(
            app,
            notification["message"],
            reply_to_message_id=notification["reply_to_message_id"],
        )
    except TelegramSendError as exc:
        logger.error("Failed to send closing alert for %s: %s", notification["event_id"], exc)


async def process_alerts(app: FastAPI) -> None:
    while True:
        try:
            current_timestamp = datetime.now().timestamp()
            notifications = []
            async with app.state.db_lock:
                active_alerts = await fetch_active_alerts(app.state.db)
                for alert_row in active_alerts:
                    if is_alert_expired(alert_row, app.state.alert_config, current_timestamp):
                        notification = get_closing_notification(alert_row, app.state.alert_config)
                        await close_alert_record(app, alert_row)
                        if notification:
                            notifications.append(notification)
            for notification in notifications:
                await send_closing_notification(app, notification)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Error in process_alerts: %s", exc, exc_info=True)

        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    http_session = None
    alert_processor_task = None
    db = None

    try:
        logger.info("Initializing application")
        validate_runtime_env()

        app.state.alert_config = load_alert_config()
        app.state.schedule_configs = load_schedule_configs()
        app.state.template = load_message_template()
        app.state.db_lock = asyncio.Lock()
        app.state.telegram_api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        app.state.alert_webhook_token = ALERT_WEBHOOK_TOKEN

        http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        db = await init_db()

        app.state.http_session = http_session
        app.state.db = db

        alert_processor_task = asyncio.create_task(process_alerts(app))
        app.state.alert_processor_task = alert_processor_task

        logger.info("Application initialized successfully")
        yield
    finally:
        if alert_processor_task:
            alert_processor_task.cancel()
            with suppress(asyncio.CancelledError):
                await alert_processor_task

        if http_session:
            await http_session.close()

        if db:
            await db.close()


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
async def healthcheck(request: Request):
    if not getattr(request.app.state, "template", None):
        raise HTTPException(status_code=503, detail="Template is not loaded")

    task = getattr(request.app.state, "alert_processor_task", None)
    if task is None or task.done():
        raise HTTPException(status_code=503, detail="Alert processor is not running")

    try:
        async with request.app.state.db.execute("SELECT 1") as cursor:
            await cursor.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database is unavailable: {exc}") from exc

    return {"status": "ok"}


@app.post("/alert")
async def create_alert(
    alert: Alert,
    request: Request,
    x_alert_token: Optional[str] = Header(default=None),
):
    verify_alert_token(x_alert_token, request.app.state.alert_webhook_token)

    try:
        event_id = alert.event_definition_id or alert.event.event_definition_id
        if not event_id:
            detail = "event_definition_id is required"
            await notify_processing_error(request.app, alert.event_definition_title, detail)
            raise HTTPException(status_code=400, detail=detail)

        alert_payload = alert.model_dump()
        alert_data = {
            "alert": alert_payload,
            "event": alert.event.model_dump(),
        }

        if should_suppress_alert(alert_data, request.app.state.schedule_configs):
            logger.info("Alert %s suppressed by rules", event_id)
            return {"status": "suppressed"}

        if request.app.state.alert_config.time_delay == 0:
            message = format_message(request.app.state.template, alert_payload)
            await send_telegram_message(request.app, message)
            return {"status": "sent"}

        while True:
            closing_notification = None
            current_timestamp = datetime.now().timestamp()
            async with request.app.state.db_lock:
                existing_alert = await fetch_active_alert(request.app.state.db, event_id)
                if existing_alert and is_alert_expired(existing_alert, request.app.state.alert_config, current_timestamp):
                    closing_notification = get_closing_notification(existing_alert, request.app.state.alert_config)
                    await close_alert_record(request.app, existing_alert)
                    existing_alert = None

                if not closing_notification and not existing_alert:
                    message = format_message(request.app.state.template, alert_payload)
                    message_id = await send_telegram_message(request.app, message)
                    await insert_alert(
                        request.app.state.db,
                        event_id,
                        alert.event_definition_title,
                        current_timestamp,
                        message_id,
                    )
                    return {"status": "first_sent_and_registered"}
                if not closing_notification and bool(existing_alert["event_started"]):
                    await update_alert_timestamp(
                        request.app.state.db,
                        event_id,
                        existing_alert["start_date"],
                        current_timestamp,
                    )
                    return {"status": "updated"}
                if not closing_notification and existing_alert:
                    message = "🚨 <b>Beginning of a recurring event detected!</b>\n\n"
                    message += f"Event: {sanitize(alert.event_definition_title)}"
                    await send_telegram_message(
                        request.app,
                        message,
                        reply_to_message_id=existing_alert["first_message_id"],
                    )
                    await mark_alert_started(
                        request.app.state.db,
                        event_id,
                        existing_alert["start_date"],
                        current_timestamp,
                    )
                    return {"status": "event_started"}

            if closing_notification:
                await send_closing_notification(request.app, closing_notification)
                continue
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error in create_alert: %s", exc, exc_info=True)
        await notify_processing_error(request.app, alert.event_definition_title, str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc
