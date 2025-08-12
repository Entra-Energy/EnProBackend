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
    

def _range_bounds(date_range: str):
    utc_now = dj_timezone.now()
    local_now = dj_timezone.localtime(utc_now)

    start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    if date_range == "month":
        start_local = start_local.replace(day=1)
    elif date_range == "year":
        start_local = start_local.replace(month=1, day=1)
    elif date_range != "today":
        raise ValueError(f"Unsupported date_range: {date_range}")

    start_utc = start_local.astimezone(dj_timezone.utc)  # explicit UTC
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

# @shared_task if you use Celery
def resample_range_task(date_range: str, device_id: Optional[str] = None, interval: str = "15min"):
    """
    Resamples Post data for today/month/year and caches the result.
    - month: 1H
    - year: 1D
    """
    # enforce interval policy
    interval = _normalized_interval(date_range, interval)
    suffix = cache_version_for_today(interval) if date_range == "today" else ""

    start_utc, end_utc = _range_bounds(date_range)

    qs = Post.objects.filter(created_date__gte=start_utc, created_date__lt=end_utc)
    if device_id:
        qs = qs.filter(devId=device_id)

    qs = qs.values("devId", "created_date", "value")
    df = pd.DataFrame(list(qs))
    cache_key = f"resampled_{date_range}:{device_id or 'all'}:{interval}:{suffix}"

    if df.empty:
        cache.set(cache_key, {}, timeout=60 * 5)
        return {}

    df["created"] = pd.to_datetime(df["created_date"], utc=True)
    df.drop(columns="created_date", inplace=True)

    # Build time axis in UTC aligned to the normalized interval
    min_time = df["created"].min().floor(interval)
    max_time = df["created"].max().ceil(interval)
    time_axis = pd.date_range(start=min_time, end=max_time, freq=interval, tz="UTC")

    result = defaultdict(list)
    # Resample per device
    for dev_id in df["devId"].unique():
        dev_df = (
            df[df["devId"] == dev_id]
            .set_index("created")[["value"]]
            .resample(interval)
            .mean()
            .reindex(time_axis)
        )
        for ts, row in dev_df.iterrows():
            v = row["value"]
            result[dev_id].append([ts.isoformat(), None if pd.isna(v) else round(float(v), 2)])

    ttl = 60 * 15 if date_range == "today" else 60 * 30
    cache.set(cache_key, dict(result), timeout=ttl)
    return dict(result)