import json
import requests
import os
import time
import csv
import calendar
import logging
import subprocess
from datetime import datetime, tzinfo, timedelta
from collections import defaultdict
from typing import Optional

import pandas as pd
import pytz
from pytz import timezone as tz
from pandas.tseries.frequencies import to_offset

import paho.mqtt.publish as publish

from django.conf import settings
from django.core import management
from django.core.cache import cache
from django.core.serializers import serialize as django_serialize
from django.db import transaction
from django.db import Error as DatabaseError
from django.db.models import Avg, Max, Count, F, ExpressionWrapper, DateTimeField
from django.db.models.functions import TruncHour, TruncMinute
from django.utils import timezone as dj_timezone

from dash_back.models import (
    Price, FlexabilitySim, Flexi, Hydro, PostForecast, Post, PostForecastMonth,
    Online, PostConsistency, Price
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Helpers using explicit Sofia time where we need "local" behavior
# --------------------------------------------------------------------
SOFIA_TZ = tz('Europe/Sofia')


def timeSet():
    now_setTime = datetime.now(SOFIA_TZ)
    consum_obj = {
        'setY': now_setTime.year,
        'setM': now_setTime.month,
        'setD': now_setTime.day,
        'setH': now_setTime.hour,
        'setmm': now_setTime.minute,
        'setS': now_setTime.second
    }
    topic = "setRTC"
    publish.single(topic, str(consum_obj), hostname="159.89.103.242", port=1883)


def manage_comm():
    # Get the current date in the 'Europe/Sofia' timezone
    now_local = datetime.now(SOFIA_TZ)
    tomorrow = now_local + timedelta(days=1)
    currDate = tomorrow.strftime("%Y-%m-%d")

    exist = Price.objects.filter(timestamp__gte=currDate)
    if exist.first():
        return

    crawl_command = "python manage.py crawl"
    process = subprocess.Popen(crawl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    print("Output:", output.decode())
    print("Error:", error.decode())
    # management.call_command('crawl')

# --------------------------------------------------------------------
# Range + interval helpers
# --------------------------------------------------------------------
def _range_bounds(date_range: str):
    """
    Return (start_utc, end_now_utc). Start is aligned to Sofia-local
    midnight/month/year, then converted to UTC. End is 'now' (UTC).
    """
    utc_now = dj_timezone.now()
    # "local" explicitly Sofia, independent of settings.TIME_ZONE
    local_now = dj_timezone.localtime(utc_now, SOFIA_TZ)

    start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    if date_range == "month":
        start_local = start_local.replace(day=1)
    elif date_range == "year":
        start_local = start_local.replace(month=1, day=1)
    elif date_range != "today":
        raise ValueError(f"Unsupported date_range: {date_range}")

    start_utc = start_local.astimezone(dj_timezone.utc)
    return start_utc, utc_now


def cache_version_for_today(interval: str) -> str:
    """
    Rotate today's cache at each interval boundary (UTC) so we don't serve stale data.
    """
    now_utc = dj_timezone.now()
    bucket = pd.Timestamp(now_utc).floor(interval).strftime("%Y%m%dT%H%M")
    return bucket


def _normalized_interval(date_range: str, requested: Optional[str]) -> str:
    if date_range == "month":
        return "1H"
    if date_range == "year":
        return "1D"
    # today
    return requested or "15min"

# --------------------------------------------------------------------
# Main resampling entry
# --------------------------------------------------------------------
def resample_range_task(date_range: str, device_id: Optional[str] = None, interval: str = "15min"):
    """
    Resamples Post data for today/month/year and caches the result.
    - month: 1H
    - year: 1D
    - today: requested (default 15min)
    Emits timestamps in Europe/Sofia (ISO8601 with +03:00 in summer).
    For 'today' we exclude the in-progress bucket so the last point isn't null.
    """
    # Enforce interval policy and build cache key suffix for "today"
    interval = _normalized_interval(date_range, interval)
    suffix = cache_version_for_today(interval) if date_range == "today" else ""

    start_utc, _end_now_utc = _range_bounds(date_range)  # we may override end for "today"

    # Compute last COMPLETED boundary in UTC and use it consistently for "today"
    if date_range == "today":
        now_ts = pd.Timestamp(dj_timezone.now())
        if now_ts.tz is None:
            now_ts = now_ts.tz_localize("UTC")
        else:
            now_ts = now_ts.tz_convert("UTC")

        end_completed_utc = now_ts.floor(interval)               # boundary at start of current bucket
        axis_end_utc = end_completed_utc - to_offset(interval)   # last COMPLETED bucket label
        end_utc = end_completed_utc                              # queryset upper bound: [start, end_completed)
    else:
        end_utc = _end_now_utc
        axis_end_utc = None  # we'll use max_time from data

    # Filter DB with the same upper bound we'll reflect in the axis
    qs = Post.objects.filter(created_date__gte=start_utc, created_date__lt=end_utc)
    if device_id:
        qs = qs.filter(devId=device_id)

    qs = qs.values("devId", "created_date", "value")
    df = pd.DataFrame(list(qs))
    cache_key = f"resampled_{date_range}:{device_id or 'all'}:{interval}:{suffix}"

    if df.empty:
        cache.set(cache_key, {}, timeout=60 * 5)
        return {}

    # Ensure UTC, tz-aware
    df["created"] = pd.to_datetime(df["created_date"], utc=True)
    df.drop(columns="created_date", inplace=True)

    # Build axis ONLY up to last completed bucket (for today)
    min_time = df["created"].min().floor(interval)
    if axis_end_utc is None:
        # month/year → include up to latest data
        max_time = df["created"].max().ceil(interval)
        axis_end_utc = max_time

    # Note: axis_end_utc is UTC tz-aware
    time_axis = pd.date_range(start=min_time, end=axis_end_utc, freq=interval, tz="UTC")

    result = defaultdict(list)
    for dev_id in df["devId"].unique():
        dev_df = (
            df[df["devId"] == dev_id]
            .set_index("created")[["value"]]
            .resample(interval, label="left", closed="left")
            .mean()
            .reindex(time_axis)
        )

        # Emit timestamps in local (Bulgaria) time
        for ts, row in dev_df.iterrows():
            ts_out = ts.astimezone(SOFIA_TZ)
            v = row["value"]
            result[dev_id].append([ts_out.isoformat(), None if pd.isna(v) else round(float(v), 2)])

    ttl = 60 * 15 if date_range == "today" else 60 * 30
    cache.set(cache_key, dict(result), timeout=ttl)
    return dict(result)
