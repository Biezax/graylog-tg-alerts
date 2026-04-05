import sys
import unittest
from datetime import datetime, time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

import main  # noqa: E402
from config import AlertConfig, ScheduleRule  # noqa: E402


class SchedulingTests(unittest.TestCase):
    def test_time_window_crosses_midnight(self):
        self.assertTrue(main.is_time_in_window(time(2, 0), time(18, 0), time(9, 0)))
        self.assertTrue(main.is_time_in_window(time(18, 30), time(18, 0), time(9, 0)))
        self.assertFalse(main.is_time_in_window(time(12, 0), time(18, 0), time(9, 0)))

    def test_suppress_rule_uses_safe_match_config(self):
        rule = ScheduleRule.model_validate(
            {
                "match": {
                    "streams_any": ["test"],
                    "priority_lt": 3,
                },
                "daily": [
                    {
                        "from": "18:00",
                        "to": "09:00",
                    }
                ],
            }
        )
        alert_data = {
            "alert": {"event_definition_id": "rule-1"},
            "event": {"streams": ["test"], "priority": 2},
        }

        self.assertTrue(main.should_suppress_alert(alert_data, [rule], datetime(2026, 4, 6, 2, 0)))
        self.assertFalse(main.should_suppress_alert(alert_data, [rule], datetime(2026, 4, 6, 12, 0)))

    def test_expiry_depends_on_alert_state(self):
        alert_config = AlertConfig(time_delay=10, closing_delay=5)
        pending_row = {"event_started": 0, "last_timestamp": 0}
        recurring_row = {"event_started": 1, "last_timestamp": 0}

        self.assertTrue(main.is_alert_expired(pending_row, alert_config, 11 * 60))
        self.assertFalse(main.is_alert_expired(recurring_row, alert_config, 11 * 60))
        self.assertTrue(main.is_alert_expired(recurring_row, alert_config, 15 * 60))


if __name__ == "__main__":
    unittest.main()
