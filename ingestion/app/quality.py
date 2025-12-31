from __future__ import annotations

from datetime import datetime, timezone, timedelta

from platform_common.config import settings, QualityResult


ALLOWED_REGIONS = {"us-east", "us-west", "eu-west", "ap-south"}


def evaluate_quality(event: dict, event_type: str) -> QualityResult:
    issues: list[str] = []

    # Region whitelist (example rule)
    region = event.get("region")
    if not isinstance(region, str) or not region:
        issues.append("region_null_or_invalid")
    elif region not in ALLOWED_REGIONS:
        issues.append("region_unknown")

    # Late-arriving logic
    now = datetime.now(timezone.utc)
    event_time = event.get("event_time")
    if isinstance(event_time, str):
        # Pydantic will normally parse, but if raw dict call, skip parsing here
        try:
            event_time = datetime.fromisoformat(event_time)
        except Exception:
            issues.append("event_time_unparseable")
            event_time = None

    is_late = False
    if isinstance(event_time, datetime):
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        if now - event_time > timedelta(days=settings.LATE_ARRIVAL_DAYS):
            is_late = True

        # Basic sanity: too far in future (> 1 day)
        if event_time - now > timedelta(days=1):
            issues.append("event_time_future_exceeds_1d")
    else:
        # missing or bad event_time already captured
        pass

    return QualityResult(is_valid=len([i for i in issues if i != "region_unknown"]) == 0, issues=issues, is_late=is_late)
