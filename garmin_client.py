import os
import logging
from datetime import date, timedelta
from garminconnect import Garmin
from db import (
    upsert_daily_summary,
    upsert_activity,
    upsert_sleep,
    upsert_hrv,
    upsert_body_battery,
    upsert_training_readiness,
    log_sync,
)

logger = logging.getLogger(__name__)

_client: Garmin | None = None


def get_client() -> Garmin:
    global _client
    if _client is None:
        email = os.environ.get("GARMIN_EMAIL")
        password = os.environ.get("GARMIN_PASSWORD")
        if not email or not password:
            raise RuntimeError("GARMIN_EMAIL and GARMIN_PASSWORD environment variables are required")
        _client = Garmin(email, password)
        _client.login()
        logger.info("Garmin Connect login successful")
    return _client


def reconnect():
    global _client
    _client = None
    return get_client()


def sync_garmin_data():
    """Sync recent Garmin data into SQLite. Called every 30 minutes."""
    logger.info("Starting Garmin data sync...")
    try:
        client = get_client()
    except Exception:
        logger.warning("Session expired, reconnecting...")
        try:
            client = reconnect()
        except Exception as e:
            log_sync("error", str(e))
            logger.error(f"Failed to reconnect: {e}")
            return

    today = date.today()
    sync_days = 7

    try:
        # Daily summaries
        for i in range(sync_days):
            dt = today - timedelta(days=i)
            dt_str = dt.isoformat()
            try:
                stats = client.get_stats(dt_str)
                if stats:
                    upsert_daily_summary(dt_str, stats)
            except Exception as e:
                logger.warning(f"Failed to get daily summary for {dt_str}: {e}")

        # Activities
        try:
            activities = client.get_activities(0, sync_days * 5)
            for act in activities:
                activity_id = act.get("activityId")
                act_date = act.get("startTimeLocal", today.isoformat())[:10]
                if activity_id:
                    upsert_activity(activity_id, act_date, act)
        except Exception as e:
            logger.warning(f"Failed to get activities: {e}")

        # Sleep
        for i in range(sync_days):
            dt = today - timedelta(days=i)
            dt_str = dt.isoformat()
            try:
                sleep_data = client.get_sleep_data(dt_str)
                if sleep_data:
                    upsert_sleep(dt_str, sleep_data)
            except Exception as e:
                logger.warning(f"Failed to get sleep data for {dt_str}: {e}")

        # HRV
        for i in range(sync_days):
            dt = today - timedelta(days=i)
            dt_str = dt.isoformat()
            try:
                hrv_data = client.get_hrv_data(dt_str)
                if hrv_data:
                    upsert_hrv(dt_str, hrv_data)
            except Exception as e:
                logger.warning(f"Failed to get HRV for {dt_str}: {e}")

        # Body Battery (API returns a list)
        for i in range(sync_days):
            dt = today - timedelta(days=i)
            dt_str = dt.isoformat()
            try:
                bb_data = client.get_body_battery(dt_str)
                if bb_data:
                    # API returns list; store first item matching the date
                    if isinstance(bb_data, list):
                        for item in bb_data:
                            if isinstance(item, dict):
                                upsert_body_battery(item.get("date", dt_str), item)
                    else:
                        upsert_body_battery(dt_str, bb_data)
            except Exception as e:
                logger.warning(f"Failed to get body battery for {dt_str}: {e}")

        # Training Readiness (API returns a list)
        for i in range(sync_days):
            dt = today - timedelta(days=i)
            dt_str = dt.isoformat()
            try:
                tr_data = client.get_training_readiness(dt_str)
                if tr_data:
                    if isinstance(tr_data, list) and tr_data:
                        for item in tr_data:
                            if isinstance(item, dict):
                                tr_date = item.get("calendarDate", dt_str)
                                upsert_training_readiness(tr_date, item)
                    elif isinstance(tr_data, dict):
                        upsert_training_readiness(dt_str, tr_data)
            except Exception as e:
                logger.warning(f"Failed to get training readiness for {dt_str}: {e}")

        log_sync("success")
        logger.info("Garmin data sync completed successfully")

    except Exception as e:
        log_sync("error", str(e))
        logger.error(f"Sync failed: {e}")
