import json
import logging
from datetime import date, timedelta

from mcp.server.fastmcp import FastMCP
from apscheduler.schedulers.background import BackgroundScheduler

from db import (
    init_db,
    query_daily_summary,
    query_activities_by_date_range,
    query_sleep,
    query_hrv,
    query_body_battery_range,
    query_training_readiness,
    query_daily_summaries_range,
    query_sleep_range,
    query_hrv_range,
)
from garmin_client import sync_garmin_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

import os as _os
_port = int(_os.environ.get("PORT", "8080"))
mcp = FastMCP("Garmin Health MCP Server", host="0.0.0.0", port=_port)


def _extract_sleep_score(daily_sleep: dict) -> int | None:
    """Extract numeric sleep score from various Garmin response formats."""
    scores = daily_sleep.get("sleepScores")
    if isinstance(scores, dict):
        overall = scores.get("overall")
        if isinstance(overall, dict):
            return overall.get("value")
        return overall
    return daily_sleep.get("overallSleepScore")


def _extract_sleep_quality(daily_sleep: dict) -> str | None:
    """Extract sleep quality label."""
    scores = daily_sleep.get("sleepScores")
    if isinstance(scores, dict):
        overall = scores.get("overall")
        if isinstance(overall, dict):
            return overall.get("qualifierKey")
    return daily_sleep.get("sleepQualityType")


@mcp.tool()
def get_today_summary() -> str:
    """Get today's health summary including heart rate, steps, Body Battery, stress, and sleep."""
    today = date.today().isoformat()
    summary = query_daily_summary(today)
    if not summary:
        return json.dumps({"error": "No data available for today. Data may not have synced yet."})

    result = {
        "date": today,
        "steps": summary.get("totalSteps"),
        "heart_rate": {
            "resting": summary.get("restingHeartRate"),
            "min": summary.get("minHeartRate"),
            "max": summary.get("maxHeartRate"),
        },
        "stress": {
            "average": summary.get("averageStressLevel"),
            "max": summary.get("maxStressLevel"),
        },
        "body_battery": {
            "highest": summary.get("bodyBatteryHighestValue"),
            "lowest": summary.get("bodyBatteryLowestValue"),
            "most_recent": summary.get("bodyBatteryMostRecentValue"),
        },
        "sleep": {
            "duration_seconds": summary.get("sleepingSeconds"),
        },
        "calories": {
            "total": summary.get("totalKilocalories"),
            "active": summary.get("activeKilocalories"),
        },
        "floors_climbed": summary.get("floorsAscended"),
        "intensity_minutes": summary.get("intensityMinutes"),
    }
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def get_training_history(days: int = 7) -> str:
    """Get training/activity history for the last N days.

    Args:
        days: Number of days to look back (default 7)
    """
    if days < 1:
        days = 1
    if days > 90:
        days = 90

    end = date.today().isoformat()
    start = (date.today() - timedelta(days=days)).isoformat()
    activities = query_activities_by_date_range(start, end)

    if not activities:
        return json.dumps({"message": f"No activities found in the last {days} days."})

    results = []
    for act in activities:
        results.append({
            "name": act.get("activityName"),
            "type": act.get("activityType", {}).get("typeKey") if isinstance(act.get("activityType"), dict) else act.get("activityType"),
            "date": act.get("startTimeLocal", "")[:10],
            "start_time": act.get("startTimeLocal"),
            "duration_minutes": round(act.get("duration", 0) / 60, 1),
            "distance_km": round(act.get("distance", 0) / 1000, 2) if act.get("distance") else None,
            "calories": act.get("calories"),
            "avg_heart_rate": act.get("averageHR"),
            "max_heart_rate": act.get("maxHR"),
            "training_effect_aerobic": act.get("aerobicTrainingEffect"),
            "training_effect_anaerobic": act.get("anaerobicTrainingEffect"),
            "vo2max": act.get("vO2MaxValue"),
        })

    return json.dumps({"days": days, "count": len(results), "activities": results}, ensure_ascii=False)


@mcp.tool()
def get_recovery_status() -> str:
    """Get recovery status including HRV, Training Readiness, and Body Battery trend."""
    today = date.today()
    today_str = today.isoformat()
    week_ago = (today - timedelta(days=7)).isoformat()

    # HRV (data is nested under hrvSummary)
    hrv_raw = query_hrv(today_str)
    hrv_summary = None
    if hrv_raw:
        hrv = hrv_raw.get("hrvSummary", hrv_raw)
        hrv_summary = {
            "weekly_average": hrv.get("weeklyAvg"),
            "last_night": hrv.get("lastNightAvg"),
            "last_night_5_min_high": hrv.get("lastNight5MinHigh"),
            "baseline": {
                "low": hrv.get("baselineLowUpper"),
                "balanced_low": hrv.get("baselineBalancedLow"),
                "balanced_upper": hrv.get("baselineBalancedUpper"),
            },
            "status": hrv.get("status"),
        }

    # Training Readiness (stored as single dict from list)
    tr = query_training_readiness(today_str)
    tr_summary = None
    if tr:
        tr_summary = {
            "score": tr.get("score"),
            "level": tr.get("level"),
            "sleep_score": tr.get("sleepScore"),
            "recovery_time_minutes": tr.get("recoveryTime"),
            "feedback": tr.get("feedbackShort"),
        }

    # Body Battery trend (7 days)
    bb_data = query_body_battery_range(week_ago, today_str)
    bb_trend = []
    for bb in bb_data:
        bb_date = bb.get("date", "")
        raw = bb
        charged = raw.get("charged")
        drained = raw.get("drained")
        bb_trend.append({
            "date": bb_date,
            "charged": charged,
            "drained": drained,
        })

    result = {
        "date": today_str,
        "hrv": hrv_summary,
        "training_readiness": tr_summary,
        "body_battery_trend_7d": bb_trend if bb_trend else None,
    }
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def get_sleep_data(target_date: str = "") -> str:
    """Get sleep data for a specific date.

    Args:
        target_date: Date in YYYY-MM-DD format (defaults to last night / today)
    """
    if not target_date:
        target_date = date.today().isoformat()

    sleep = query_sleep(target_date)
    if not sleep:
        return json.dumps({"error": f"No sleep data found for {target_date}."})

    daily_sleep = sleep.get("dailySleepDTO", sleep)

    result = {
        "date": target_date,
        "sleep_start": daily_sleep.get("sleepStartTimestampLocal") or daily_sleep.get("sleepStart"),
        "sleep_end": daily_sleep.get("sleepEndTimestampLocal") or daily_sleep.get("sleepEnd"),
        "duration_seconds": daily_sleep.get("sleepTimeSeconds"),
        "quality": _extract_sleep_quality(daily_sleep),
        "sleep_score": _extract_sleep_score(daily_sleep),
        "deep_sleep_seconds": daily_sleep.get("deepSleepSeconds"),
        "light_sleep_seconds": daily_sleep.get("lightSleepSeconds"),
        "rem_sleep_seconds": daily_sleep.get("remSleepSeconds"),
        "awake_seconds": daily_sleep.get("awakeSleepSeconds"),
        "avg_spo2": daily_sleep.get("averageSpO2Value"),
        "avg_respiration": daily_sleep.get("averageRespirationValue"),
        "avg_stress": daily_sleep.get("averageStress") or daily_sleep.get("overallStress"),
    }
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def get_health_trends(days: int = 14) -> str:
    """Analyze recent health indicator trends over the specified number of days.

    Args:
        days: Number of days to analyze (default 14, max 90)
    """
    if days < 1:
        days = 1
    if days > 90:
        days = 90

    today = date.today()
    start = (today - timedelta(days=days)).isoformat()
    end = today.isoformat()

    summaries = query_daily_summaries_range(start, end)
    sleep_data = query_sleep_range(start, end)
    hrv_data = query_hrv_range(start, end)

    # Steps trend
    steps_trend = []
    rhr_trend = []
    stress_trend = []
    for s in summaries:
        dt = s.get("date", "")
        steps_trend.append({"date": dt, "steps": s.get("totalSteps")})
        if s.get("restingHeartRate"):
            rhr_trend.append({"date": dt, "resting_hr": s.get("restingHeartRate")})
        if s.get("averageStressLevel"):
            stress_trend.append({"date": dt, "avg_stress": s.get("averageStressLevel")})

    # Sleep trend
    sleep_trend = []
    for sl in sleep_data:
        dt = sl.get("date", "")
        daily = sl.get("dailySleepDTO", sl)
        sleep_trend.append({
            "date": dt,
            "duration_hours": round(daily.get("sleepTimeSeconds", 0) / 3600, 1) if daily.get("sleepTimeSeconds") else None,
            "score": _extract_sleep_score(daily),
        })

    # HRV trend
    hrv_trend = []
    for h in hrv_data:
        dt = h.get("date", "")
        hsummary = h.get("hrvSummary", h)
        hrv_trend.append({
            "date": dt,
            "weekly_avg": hsummary.get("weeklyAvg"),
            "last_night_avg": hsummary.get("lastNightAvg"),
            "status": hsummary.get("status"),
        })

    # Compute averages
    def avg(values):
        valid = [v for v in values if v is not None]
        return round(sum(valid) / len(valid), 1) if valid else None

    all_steps = [s.get("totalSteps") for s in summaries]
    all_rhr = [s.get("restingHeartRate") for s in summaries if s.get("restingHeartRate")]
    all_stress = [s.get("averageStressLevel") for s in summaries if s.get("averageStressLevel")]

    result = {
        "period": f"{start} to {end}",
        "days_with_data": len(summaries),
        "averages": {
            "daily_steps": avg(all_steps),
            "resting_heart_rate": avg(all_rhr),
            "stress_level": avg(all_stress),
        },
        "steps_trend": steps_trend,
        "resting_hr_trend": rhr_trend,
        "stress_trend": stress_trend,
        "sleep_trend": sleep_trend,
        "hrv_trend": hrv_trend,
    }
    return json.dumps(result, ensure_ascii=False)


def start_scheduler():
    from datetime import datetime, timedelta as td
    scheduler = BackgroundScheduler()
    scheduler.add_job(sync_garmin_data, "interval", minutes=30, id="garmin_sync")
    # Delay initial sync by 10s so the server can pass health checks first
    scheduler.add_job(
        sync_garmin_data, "date",
        run_date=datetime.now() + td(seconds=10),
        id="garmin_sync_initial",
    )
    scheduler.start()
    logger.info("Scheduler started: initial sync in 10s, then every 30 minutes")


if __name__ == "__main__":
    import os
    import sys
    import uvicorn
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route, Mount

    # Warn if env vars missing but don't crash — server can still start
    if not os.environ.get("GARMIN_EMAIL") or not os.environ.get("GARMIN_PASSWORD"):
        logger.warning("GARMIN_EMAIL and/or GARMIN_PASSWORD not set — Garmin sync will be disabled")
    else:
        logger.info("Garmin credentials found")

    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized")

    # Only start scheduler if credentials are available
    if os.environ.get("GARMIN_EMAIL") and os.environ.get("GARMIN_PASSWORD"):
        logger.info("Starting scheduler...")
        start_scheduler()
    else:
        logger.warning("Scheduler not started — no Garmin credentials")

    # Build the MCP SSE app and wrap it with a health check
    mcp_app = mcp.sse_app()

    async def health(request):
        return JSONResponse({"status": "ok"})

    app = Starlette(
        routes=[
            Route("/health", health),
            Route("/", health),
            Mount("/", app=mcp_app),
        ],
    )

    logger.info(f"Starting MCP server on 0.0.0.0:{_port}...")
    uvicorn.run(app, host="0.0.0.0", port=_port)
