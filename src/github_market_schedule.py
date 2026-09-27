"""Select the active GitHub cron occurrence using the NYSE session calendar."""

from datetime import UTC, datetime, timedelta
import os
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

NEW_YORK = ZoneInfo("America/New_York")


def select_update(event: str, cron: str, now: datetime) -> dict[str, str]:
    local = now.astimezone(NEW_YORK)
    result = {"should_run": "false", "label": "inactive-schedule",
              "with_fundamentals": "false", "ny_time": local.isoformat()}
    if event == "workflow_dispatch":
        return {**result, "should_run": "true", "label": "manual"}
    if event != "schedule":
        return result

    day = local.date()
    monday = day - timedelta(days=day.weekday())
    sessions = mcal.get_calendar("NYSE").schedule(start_date=monday, end_date=day)
    if sessions.empty or sessions.index[-1].date() != day:
        return {**result, "label": "market-holiday"}

    session = sessions.iloc[-1]
    for label, column, delay in (("market-open", "market_open", 5),
                                 ("market-close", "market_close", 10)):
        target = session[column].to_pydatetime() + timedelta(minutes=delay)
        active_cron = f"{target.minute} {target.hour} * * 1-5"
        # Match the triggering expression, not the current hour: Actions can queue.
        if cron == active_cron and now >= target:
            return {**result, "should_run": "true", "label": label,
                    "with_fundamentals": str(
                        label == "market-close" and sessions.index[0].date() == day
                    ).lower()}
    return result


if __name__ == "__main__":
    outputs = select_update(os.getenv("EVENT_NAME", ""),
                            os.getenv("EVENT_SCHEDULE", ""), datetime.now(UTC))
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        for key, value in outputs.items():
            print(f"{key}={value}")
            output.write(f"{key}={value}\n")
