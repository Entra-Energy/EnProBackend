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
from django.utils.timezone import now
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
    
    

                            
def resample_today_task(device_id=None, interval='15min'):
    """
    Resamples today's Post data to the given interval and caches the result.
    """
    # Define the timezone-aware start and end for today (e.g. Europe/Sofia)
    today = now().astimezone().date()
    tomorrow = today + timedelta(days=1)
    start_time = f"{today}T00:00:00Z"
    end_time = f"{tomorrow}T00:00:00Z"

    # Filter Post objects
    qs = Post.objects.filter(created_date__gte=start_time, created_date__lt=end_time)
    if device_id:
        qs = qs.filter(devId=device_id)

    qs = qs.values('devId', 'created_date', 'value')

    # Convert queryset to DataFrame
    df = pd.DataFrame(list(qs))
    if df.empty:
        return []

    df['created'] = pd.to_datetime(df['created_date'])
    df.drop(columns='created_date', inplace=True)

    # Generate a unified time axis
    min_time = df['created'].min().floor(interval)
    max_time = df['created'].max().ceil(interval)
    time_axis = pd.date_range(start=min_time, end=max_time, freq=interval)

    # Resample and pad each device
    result = defaultdict(list)
    for dev_id in df['devId'].unique():
        dev_df = df[df['devId'] == dev_id].set_index('created')
        dev_df = dev_df[['value']].resample(interval).mean()
        dev_df = dev_df.reindex(time_axis)

        for ts, row in dev_df.iterrows():
            result[dev_id].append([
            ts.isoformat(),
            None if pd.isna(row['value']) else round(row['value'], 2)
        ])

    # Store result in cache
    cache_key = f"resampled_today:{device_id or 'all'}:{interval}"
    cache.set(cache_key, result, timeout=60 * 15)  # Cache for 15 minutes

    return dict(result)



