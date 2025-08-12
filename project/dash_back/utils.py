import json
import requests
from django.conf import settings
from dash_back.models import Price, FlexabilitySim, Flexi, Hydro, PostForecast, Post, PostForecastMonth, Online, PostConsistency, Price
from datetime import datetime,tzinfo,timedelta
from django.core.serializers import serialize as django_serialize
from django.db import Error as DatabaseError
from datetime import date
from django.db import transaction
from django.db.models.functions import TruncHour, TruncMinute
from django.db.models import Avg, F, ExpressionWrapper, DateTimeField
import pandas as pd
import pytz
from pytz import timezone as tz
from django.utils import timezone as dj_timezone
from django.utils.timezone import now, localtime  # optional: you can keep these if you like
from django.conf import settings
import os
import paho.mqtt.publish as publish
import time
from django.core import management
import csv
from django.db.models import Avg, Max, Count
import calendar
from django.core.cache import cache
from django.utils.timezone import now, localtime
import logging
logger = logging.getLogger(__name__)
import subprocess
from collections import defaultdict
from typing import Optional
from pandas.tseries.frequencies import to_offset


     

def timeSet():
    now_setTime = datetime.now(tz('Europe/Sofia'))
    consum_obj = {
                    'setY': now_setTime.year,
                    'setM': now_setTime.month,
                    'setD':now_setTime.day,
                    'setH':now_setTime.hour,
                    'setmm':now_setTime.minute,
                    'setS':now_setTime.second                    
                }
    topic = "setRTC"
    publish.single(topic,str(consum_obj),hostname="159.89.103.242",port=1883)



def manage_comm():
    # Get the current date in the 'Europe/Sofia' timezone
    now = datetime.now(tz('Europe/Sofia'))
  
    tomorrow = now + timedelta(days=1)

    # Extract the date in the format 'YYYY-MM-DD'
    currDate = tomorrow.strftime("%Y-%m-%d")  
    
    exist = Price.objects.filter(timestamp__gte=currDate)
    
  
    if exist.first():
        pass
    else:
        crawl_command = "python manage.py crawl"
        # Run the crawl command using subprocess
        process = subprocess.Popen(crawl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()

        # Log the output and error, or handle them as needed
        print("Output:", output.decode())
        print("Error:", error.decode())
        # print("CALLING THE CRAWLER:")        
        # management.call_command('crawl')    
    

SOFIA_TZ = tz('Europe/Sofia')

def _range_bounds(date_range: str):
    utc_now = dj_timezone.now()
    # make "local" explicitly Sofia, independent of settings.TIME_ZONE
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



def resample_range_task(date_range: str, device_id: Optional[str] = None, interval: str = "15min"):
    interval = _normalized_interval(date_range, interval)
    suffix = cache_version_for_today(interval) if date_range == "today" else ""

    start_utc, _end_now_utc = _range_bounds(date_range)  # we’lloverride end for today below

    # ✅ Compute last COMPLETED boundary in UTC and use it consistently
    if date_range == "today":
        # floor(now) in UTC to resample interval (e.g., 15min)
        end_completed_utc = pd.Timestamp(dj_timezone.now(), tz=pytz.UTC).floor(interval)
        # we want to include ONLY completed buckets, so end label is one interval before
        axis_end_utc = end_completed_utc - to_offset(interval)
        end_utc = end_completed_utc  # queryset upper bound is [start, end_completed)
    else:
        end_utc = _end_now_utc
        axis_end_utc = None  # we’ll use max_time from data

    # 🔒 Filter DB with the same upper bound we’ll reflect in the axis
    qs = Post.objects.filter(created_date__gte=start_utc, created_date__lt=end_utc)
    if device_id:
        qs = qs.filter(devId=device_id)

    qs = qs.values("devId", "created_date", "value")
    df = pd.DataFrame(list(qs))
    cache_key = f"resampled_{date_range}:{device_id or 'all'}:{interval}:{suffix}"

    if df.empty:
        cache.set(cache_key, {}, timeout=60 * 5)
        return {}

    # ensure UTC, tz-aware
    df["created"] = pd.to_datetime(df["created_date"], utc=True)
    df.drop(columns="created_date", inplace=True)

    # Build axis ONLY up to last completed bucket
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
            .resample(interval, label="left", closed="left")   # 👈 explicit
            .mean()
            .reindex(time_axis)
        )

        # OPTIONAL: emit timestamps in local (Bulgaria) time
        for ts, row in dev_df.iterrows():
            ts_out = ts.astimezone(SOFIA_TZ)  # comment this line to keep UTC
            v = row["value"]
            result[dev_id].append([ts_out.isoformat(), None if pd.isna(v) else round(float(v), 2)])

    ttl = 60 * 15 if date_range == "today" else 60 * 30
    cache.set(cache_key, dict(result), timeout=ttl)
    return dict(result)