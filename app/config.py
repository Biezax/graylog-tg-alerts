import json
import os
from datetime import time
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ALERT_WEBHOOK_TOKEN = os.getenv("ALERT_WEBHOOK_TOKEN")

DEFAULT_ALERT_CONFIG = {
    "time_delay": 0,
    "closing_delay": 0,
}

WEEKDAYS = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
}


class DailyWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_time: time = Field(alias="from")
    to_time: time = Field(alias="to")


class MonthlyRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    week: int
    weekday: str

    @field_validator("week")
    @classmethod
    def validate_week(cls, value: int) -> int:
        if value == 0:
            raise ValueError("week must not be 0")
        return value

    @field_validator("weekday")
    @classmethod
    def validate_weekday(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in WEEKDAYS:
            raise ValueError("weekday must be a valid weekday name")
        return normalized


class MatchRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    streams_any: List[str] = Field(default_factory=list)
    source_streams_any: List[str] = Field(default_factory=list)
    priority_lt: Optional[int] = None
    priority_lte: Optional[int] = None
    priority_gt: Optional[int] = None
    priority_gte: Optional[int] = None
    event_definition_id_in: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_non_empty(self) -> "MatchRule":
        if not any(
            [
                self.streams_any,
                self.source_streams_any,
                self.event_definition_id_in,
                self.priority_lt is not None,
                self.priority_lte is not None,
                self.priority_gt is not None,
                self.priority_gte is not None,
            ]
        ):
            raise ValueError("match must contain at least one condition")
        return self


class ScheduleRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    match: MatchRule
    daily: List[DailyWindow] = Field(default_factory=list)
    monthly: List[MonthlyRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_schedules(self) -> "ScheduleRule":
        if not self.daily and not self.monthly:
            raise ValueError("rule must contain at least one daily or monthly condition")
        return self


class AlertConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    time_delay: int = 0
    closing_delay: int = 0

    @field_validator("time_delay", "closing_delay")
    @classmethod
    def validate_delay(cls, value: int) -> int:
        if value < 0:
            raise ValueError("delay must be >= 0")
        return value


def get_config_dir() -> Path:
    configured_dir = os.getenv("CONFIG_DIR")
    if configured_dir:
        return Path(configured_dir)

    candidates = [
        Path("/app/config"),
        Path(__file__).resolve().parent / "config",
        Path(__file__).resolve().parent.parent / "config",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def get_config_path(filename: str) -> Path:
    return get_config_dir() / filename


def load_json_config(filename: str):
    config_path = get_config_path(filename)
    if not config_path.exists():
        return None

    with config_path.open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def load_alert_config() -> AlertConfig:
    raw_config = load_json_config("alert_config.json") or DEFAULT_ALERT_CONFIG
    try:
        return AlertConfig.model_validate(raw_config)
    except ValidationError as exc:
        raise ValueError(f"Invalid alert_config.json: {exc}") from exc


def load_schedule_configs() -> List[ScheduleRule]:
    raw_config = load_json_config("schedule_config.json") or []
    if isinstance(raw_config, dict):
        raise ValueError("Legacy schedule_config.json format is not supported")
    if not isinstance(raw_config, list):
        raise ValueError("schedule_config.json must contain a list of rules")

    try:
        return [ScheduleRule.model_validate(rule) for rule in raw_config]
    except ValidationError as exc:
        raise ValueError(f"Invalid schedule_config.json: {exc}") from exc


def validate_runtime_env() -> None:
    missing_variables = []
    if not TELEGRAM_BOT_TOKEN:
        missing_variables.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing_variables.append("TELEGRAM_CHAT_ID")
    if not ALERT_WEBHOOK_TOKEN:
        missing_variables.append("ALERT_WEBHOOK_TOKEN")

    if missing_variables:
        missing = ", ".join(missing_variables)
        raise ValueError(f"Missing required environment variables: {missing}")
