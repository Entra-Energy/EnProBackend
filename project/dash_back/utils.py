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
from pytz import timezone
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

     

def timeSet():
    now_setTime = datetime.now(timezone('Europe/Sofia'))
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
    now = datetime.now(timezone('Europe/Sofia'))
  
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
    """
    Return (start_utc, end_utc) for the requested range.
    start is aligned to local midnight/month/year using Django's timezone.
    end is now() in UTC.
    """
    utc_now = now()
    local_now = localtime(utc_now)  # Convert to current timezone in settings

    # Start of today in local time
    start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)

    if date_range == "month":
        start_local = start_local.replace(day=1)
    elif date_range == "year":
        start_local = start_local.replace(month=1, day=1)
    elif date_range != "today":
        raise ValueError(f"Unsupported date_range: {date_range}")

    # Convert start back to UTC
    start_utc = start_local.astimezone(tz=None)
    return start_utc, utc_now


                            
def resample_range_task(date_range: str, device_id: str | None = None, interval: str = "15min"):
    """
    Resamples Post data for today/month/year and caches the result.
    """
    start_utc, end_utc = _range_bounds(date_range)

    qs = Post.objects.filter(created_date__gte=start_utc, created_date__lt=end_utc)
    if device_id:
        qs = qs.filter(devId=device_id)

    qs = qs.values("devId", "created_date", "value")
    df = pd.DataFrame(list(qs))
    if df.empty:
        cache.set(f"resampled_{date_range}:{device_id or 'all'}:{interval}", {}, timeout=60 * 5)
        return {}

    df["created"] = pd.to_datetime(df["created_date"], utc=True)
    df.drop(columns="created_date", inplace=True)

    min_time = df["created"].min().floor(interval)
    max_time = df["created"].max().ceil(interval)
    time_axis = pd.date_range(start=min_time, end=max_time, freq=interval, tz="UTC")

    result = defaultdict(list)
    for dev_id in df["devId"].unique():
        dev_df = (
            df[df["devId"] == dev_id]
            .set_index("created")[["value"]]
            .resample(interval)
            .mean()
            .reindex(time_axis)
        )
        for ts, row in dev_df.iterrows():
            result[dev_id].append([ts.isoformat(), None if pd.isna(row["value"]) else round(row["value"], 2)])

    ttl = 60 * 15 if date_range == "today" else 60 * 30
    cache.set(f"resampled_{date_range}:{device_id or 'all'}:{interval}", dict(result), timeout=ttl)
    return dict(result)


