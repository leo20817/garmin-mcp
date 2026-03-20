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
_LOGIN_COOLDOWN = 300  # 5 minutes — Garmin rate limits are strict

TOKEN_DIR = os.environ.get("TOKEN_DIR", "/tmp/garmin_tokens")

# Delay between each API call to avoid triggering rate limits
API_CALL_DELAY = 2  # seconds


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

    # Try loading saved tokens first (avoids hitting OAuth endpoint entirely)
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
            f"Login cooldown active — last attempt {elapsed:.0f}s ago, "
            f"wait {wait:.0f}s more to avoid Garmin 429."
        )

    _last_login_attempt = time.time()
    client.login()
    logger.info("Garmin Connect: full login successful")

    _save_tokens(client)
    return client


def _save_tokens(client: Garmin):
    """Persist garth tokens to disk."""
    try:
        os.makedirs(TOKEN_DIR, exist_ok=True)
        client.garth.dump(TOKEN_DIR)
        logger.info(f"Garmin tokens saved to {TOKEN_DIR}")
    except Exception as e:
        logger.warning(f"Failed to save tokens: {e}")


def reconnect() -> Garmin:
    """Force a fresh login. Respects cooldown to avoid 429."""
    global _client
    _client = None
    return _login()


def _is_rate_limit_error(e: Exception) -> bool:
    """Check if an exception is a Garmin 429 rate limit error."""
    msg = str(e)
    return "429" in msg or "Too Many Requests" in msg


def _safe_api_call(client: Garmin, func_name: str, *args, **kwargs):
    """
    Call a Garmin API method with automatic session recovery.
    Returns (result, client) — client may change after reconnect.
    Raises on 429 to stop the entire sync.
    """
    method = getattr(client, func_name)
    try:
        result = method(*args, **kwargs)
        return result, client
    except Exception as e:
        if _is_rate_limit_error(e):
            raise  # bubble up — caller should stop sync entirely
        # Likely session expired — try reconnect once
        logger.warning(f"API call {func_name} failed: {e}, attempting reconnect...")
        try:
            client = reconnect()
        except Exception as re:
            logger.error(f"Reconnect failed: {re}")
            raise
        # Save new tokens immediately after reconnect
        _save_tokens(client)
        # Retry the call with fresh client
        method = getattr(client, func_name)
        return method(*args, **kwargs), client


def sync_garmin_data():
    """Sync recent Garmin data into SQLite. Called every 30 minutes."""
    logger.info("Starting Garmin data sync...")

    try:
        client = get_client()
    except Exception as e:
        logger.warning(f"get_client failed: {e}")
        # If cooldown is active, skip this sync entirely
        if "cooldown" in str(e).lower():
            log_sync("skipped", f"Login cooldown active, will retry next cycle")
            logger.info("Skipping sync — login cooldown active")
            return
        try:
            client = reconnect()
        except Exception as e2:
            log_sync("error", str(e2))
            logger.error(f"Failed to connect to Garmin: {e2}")
            return

    today = date.today()
    # Only sync last 2 days for frequent syncs — reduces API calls from ~42 to ~12
    sync_days = 2
    success_count = 0
    fail_count = 0
    rate_limited = False

    def _throttle():
        """Sleep between API calls to avoid rate limits."""
        time.sleep(API_CALL_DELAY)

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
            _throttle()
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on daily summary — stopping sync")
                rate_limited = True
            else:
                logger.warning(f"Failed to get daily summary for {dt_str}: {e}")
                fail_count += 1

    # Activities (single call, not per-day)
    if not rate_limited:
        try:
            activities, client = _safe_api_call(client, "get_activities", 0, 10)
            for act in activities:
                activity_id = act.get("activityId")
                act_date = act.get("startTimeLocal", today.isoformat())[:10]
                if activity_id:
                    upsert_activity(activity_id, act_date, act)
                    success_count += 1
            _throttle()
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
            _throttle()
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
            _throttle()
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
            _throttle()
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
            _throttle()
        except Exception as e:
            if _is_rate_limit_error(e):
                logger.error(f"Rate limited on training readiness — stopping sync")
                rate_limited = True
            else:
                logger.warning(f"Failed to get training readiness for {dt_str}: {e}")
                fail_count += 1

    # Save tokens after successful sync
    if success_count > 0:
        _save_tokens(client)

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
