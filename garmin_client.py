import os
import time
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
_last_login_attempt: float = 0  # epoch timestamp of last login attempt
_LOGIN_COOLDOWN = 120  # seconds — avoid hammering Garmin OAuth

TOKEN_DIR = os.environ.get("TOKEN_DIR", "/tmp/garmin_tokens")


def get_client() -> Garmin:
    global _client
    if _client is None:
        _client = _login()
    return _client


def _login() -> Garmin:
    """Login to Garmin Connect, using cached tokens when possible."""
    global _last_login_attempt

    email = os.environ.get("GARMIN_EMAIL")
    password = os.environ.get("GARMIN_PASSWORD")
    if not email or not password:
        raise RuntimeError("GARMIN_EMAIL and GARMIN_PASSWORD environment variables are required")

    client = Garmin(email, password)

    # Try loading saved tokens first (avoids hitting OAuth endpoint)
    try:
        client.garth.load(TOKEN_DIR)
        client.display_name = client.garth.profile["displayName"]
        client.full_name = client.garth.profile["fullName"]
        logger.info("Garmin Connect: restored session from saved tokens")
        return client
    except Exception:
        logger.info("No saved tokens or tokens expired, performing full login...")

    # Rate-limit full logins to avoid 429
    now = time.time()
    elapsed = now - _last_login_attempt
    if elapsed < _LOGIN_COOLDOWN:
        wait = _LOGIN_COOLDOWN - elapsed
        raise RuntimeError(
            f"Login cooldown active — last login attempt was {elapsed:.0f}s ago, "
            f"need to wait {wait:.0f}s more. This prevents Garmin 429 rate limits."
        )

    _last_login_attempt = time.time()
    client.login()
    logger.info("Garmin Connect: full login successful")

    # Persist tokens for future use
    try:
        os.makedirs(TOKEN_DIR, exist_ok=True)
        client.garth.dump(TOKEN_DIR)
        logger.info(f"Garmin tokens saved to {TOKEN_DIR}")
    except Exception as e:
        logger.warning(f"Failed to save tokens: {e}")

    return client


def reconnect() -> Garmin:
    """Force a fresh login. Respects cooldown to avoid 429."""
    global _client
    _client = None
    return _login()


def _is_rate_limit_error(e: Exception) -> bool:
    """Check if an exception is a Garmin 429 rate limit error."""
    return "429" in str(e) or "Too Many Requests" in str(e)


def _safe_api_call(client: Garmin, func_name: str, *args, **kwargs):
    """
    Call a Garmin API method with automatic session recovery.
    Returns (result, client) — client may change after reconnect.
    Raises on 429 to stop the entire sync.
    """
    method = getattr(client, func_name)
    try:
        return method(*args, **kwargs), client
    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # bubble up — caller should stop sync
        # Likely session expired — try reconnect once
        logger.warning(f"API call {func_name} failed: {e}, attempting reconnect...")
        try:
            client = reconnect()
        except Exception as re:
            logger.error(f"Reconnect failed: {re}")
            raise
        # Retry the call with fresh client
        method = getattr(client, func_name)
        return method(*args, **kwargs), client


def sync_garmin_data():
    """Sync recent Garmin data into SQLite. Called every 30 minutes."""
    logger.info("Starting Garmin data sync...")

    try:
        client = get_client()
    except Exception:
        logger.warning("Initial get_client failed, attempting reconnect...")
        try:
            client = reconnect()
        except Exception as e:
            log_sync("error", str(e))
            logger.error(f"Failed to connect to Garmin: {e}")
            return

    today = date.today()
    sync_days = 7
    success_count = 0
    fail_count = 0
    rate_limited = False

    # Daily summaries
    for i in range(sync_days):
        if rate_limited:
            break
        dt_str = (today - timedelta(days=i)).isoformat()
        try:
            stats, client = _safe_api_call(client, "get_stats", dt_str)
            if stats:
                upsert_daily_summary(dt_str, stats)
                success_count += 1
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on daily summary — stopping sync to avoid further 429s")
                rate_limited = True
            else:
                logger.warning(f"Failed to get daily summary for {dt_str}: {e}")
                fail_count += 1

    # Activities
    if not rate_limited:
        try:
            activities, client = _safe_api_call(client, "get_activities", 0, sync_days * 5)
            for act in activities:
                activity_id = act.get("activityId")
                act_date = act.get("startTimeLocal", today.isoformat())[:10]
                if activity_id:
                    upsert_activity(activity_id, act_date, act)
                    success_count += 1
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on activities — stopping sync")
                rate_limited = True
            else:
                logger.warning(f"Failed to get activities: {e}")
                fail_count += 1

    # Sleep
    for i in range(sync_days):
        if rate_limited:
            break
        dt_str = (today - timedelta(days=i)).isoformat()
        try:
            sleep_data, client = _safe_api_call(client, "get_sleep_data", dt_str)
            if sleep_data:
                upsert_sleep(dt_str, sleep_data)
                success_count += 1
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on sleep — stopping sync")
                rate_limited = True
            else:
                logger.warning(f"Failed to get sleep data for {dt_str}: {e}")
                fail_count += 1

    # HRV
    for i in range(sync_days):
        if rate_limited:
            break
        dt_str = (today - timedelta(days=i)).isoformat()
        try:
            hrv_data, client = _safe_api_call(client, "get_hrv_data", dt_str)
            if hrv_data:
                upsert_hrv(dt_str, hrv_data)
                success_count += 1
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on HRV — stopping sync")
                rate_limited = True
            else:
                logger.warning(f"Failed to get HRV for {dt_str}: {e}")
                fail_count += 1

    # Body Battery
    for i in range(sync_days):
        if rate_limited:
            break
        dt_str = (today - timedelta(days=i)).isoformat()
        try:
            bb_data, client = _safe_api_call(client, "get_body_battery", dt_str)
            if bb_data:
                if isinstance(bb_data, list):
                    for item in bb_data:
                        if isinstance(item, dict):
                            upsert_body_battery(item.get("date", dt_str), item)
                else:
                    upsert_body_battery(dt_str, bb_data)
                success_count += 1
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on body battery — stopping sync")
                rate_limited = True
            else:
                logger.warning(f"Failed to get body battery for {dt_str}: {e}")
                fail_count += 1

    # Training Readiness
    for i in range(sync_days):
        if rate_limited:
            break
        dt_str = (today - timedelta(days=i)).isoformat()
        try:
            tr_data, client = _safe_api_call(client, "get_training_readiness", dt_str)
            if tr_data:
                if isinstance(tr_data, list) and tr_data:
                    for item in tr_data:
                        if isinstance(item, dict):
                            tr_date = item.get("calendarDate", dt_str)
                            upsert_training_readiness(tr_date, item)
                elif isinstance(tr_data, dict):
                    upsert_training_readiness(dt_str, tr_data)
                success_count += 1
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on training readiness — stopping sync")
                rate_limited = True
            else:
                logger.warning(f"Failed to get training readiness for {dt_str}: {e}")
                fail_count += 1

    # Save tokens after successful API usage (refreshes token expiry)
    if success_count > 0:
        try:
            os.makedirs(TOKEN_DIR, exist_ok=True)
            client.garth.dump(TOKEN_DIR)
        except Exception:
            pass

    # Accurate sync status logging
    if rate_limited:
        log_sync("rate_limited", f"success={success_count}, failed={fail_count}, stopped due to 429")
        logger.warning(f"Sync stopped due to rate limit. success={success_count}, failed={fail_count}")
    elif fail_count > 0:
        log_sync("partial", f"success={success_count}, failed={fail_count}")
        logger.warning(f"Sync completed with errors. success={success_count}, failed={fail_count}")
    else:
        log_sync("success", f"success={success_count}")
        logger.info(f"Garmin data sync completed successfully. {success_count} items synced")
