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

def _normalize_resample_format(resample: str) -> str:
    """Convert user-friendly resample format to pandas format."""
    mapping = {
        "15min": "15min",
        "1h": "1H", 
        "1day": "1D"
    }
    return mapping.get(resample, resample)

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

def _get_cache_ttl(date_range: str, interval: str) -> int:
    """Get appropriate cache TTL based on date range and interval."""
    if date_range == "today":
        # Shorter TTL for today's data as it changes frequently
        if interval in ["15min", "15T"]:
            return 60 * 10  # 10 minutes
        elif interval in ["1H", "1h"]:
            return 60 * 30  # 30 minutes
        else:  # 1D
            return 60 * 60  # 1 hour
    else:
        # Longer TTL for historical data (month/year)
        return 60 * 60 * 2  # 2 hours
    


def resample_range_task(date_range: str, device_id: Optional[str] = None, interval: str = "15min"):
    norm_interval = _normalize_resample_format(interval)
    suffix = cache_version_for_today(norm_interval) if date_range == "today" else ""
    start_utc, end_utc = _range_bounds(date_range)

    qs = Post.objects.filter(created_date__gte=start_utc, created_date__lt=end_utc)
    if device_id:
        qs = qs.filter(devId=device_id)

    df = pd.DataFrame(list(qs.values("devId", "created_date", "value")))
    base_key = f"resampled_{date_range}:{{}}:{norm_interval}:{suffix}"

    if df.empty:
        # still write empty caches so views are fast
        cache.set(base_key.format(device_id or "all"), {}, timeout=_get_cache_ttl(date_range, norm_interval))
        return {}

    df["created"] = pd.to_datetime(df["created_date"], utc=True)
    df.drop(columns="created_date", inplace=True)

    now_utc = pd.Timestamp.now(tz="UTC")
    min_time = df["created"].min().floor(norm_interval)
    last_complete = min(df["created"].max().floor(norm_interval), now_utc.floor(norm_interval))
    time_axis = pd.date_range(start=min_time, end=last_complete, freq=norm_interval, tz="UTC")

    result = defaultdict(list)
    for dev_id in df["devId"].unique():
        dev_df = (
            df[df["devId"] == dev_id]
            .set_index("created")[["value"]]
            .resample(norm_interval)        # use normalized interval here too
            .mean()
            .reindex(time_axis)
        )
        for ts, row in dev_df.iterrows():
            ts_out = ts.astimezone(SOFIA_TZ)
            v = row["value"]
            result[dev_id].append([ts_out.isoformat(), None if pd.isna(v) else round(float(v), 2)])

    ttl = _get_cache_ttl(date_range, norm_interval)

    # 1) cache the aggregate (all devices)
    cache.set(base_key.format("all"), dict(result), timeout=ttl)

    # 2) cache each device individually
    for dev_id, series in result.items():
        cache.set(base_key.format(dev_id), {dev_id: series}, timeout=ttl)

    # If a single device was requested, return just that slice; else return all
    return {device_id: result[device_id]} if device_id else dict(result)