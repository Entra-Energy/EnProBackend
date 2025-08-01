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
import logging
logger = logging.getLogger(__name__)
import subprocess


    


sm_coeff = [{"sm-0001":120},{"sm-0002":320},{"sm-0003":400},{"sm-0004":200},{"sm-0006":200},{"sm-0008":200},{"sm-0009":80},
{"sm-0010":60},{"sm-0011":60},{"sm-0015":60},{"sm-0016":250},{"sm-0017":200},{"sm-0018":400},{"sm-0019":500},{"sm-0020":500},{"sm-0025":200}]         

def get_curr_time():
    now = datetime.now(timezone('Europe/Sofia'))
    now = str(now)
    currDate = now.split(" ")[0]+"T"
    cur_hour = now.split(" ")[1].split(":")[0]
    cur_hour_min = now.split(" ")[1].split(":")[1]
    query_date = currDate+cur_hour+":"+cur_hour_min+":00Z"
    return query_date


def date_to_timestamp(date :str) -> int:   
    stamp = datetime.fromisoformat(date).timestamp()
    return int(stamp)

def convert(str):
    todays_date = date.today()
    year = todays_date.year
    month = todays_date.month
    day = todays_date.day

    str = str.split(':')
    hour = int(str[0])
    #est = pytz.timezone('Europe/Sofia')
    time_hour = datetime(year, month, day, hour, 0, 0, tzinfo=pytz.utc)
    return time_hour

def post_forecast():
    pass



# def price_to_db():
#     price_path = os.path.join(settings.BASE_DIR, 'ibex.json')
#     #print(price_path)
#     with open(price_path, 'r') as f:
#         my_json_obj = json.load(f)
#     for data in my_json_obj:
#         time = convert(data["time"])
#         price = float(data["price"])
#         #print(price)
#         Price.objects.get_or_create(timestamp=time, value = price)

def scheduled_flexi():        

    # test = FlexabilitySim.objects.all().last()
    # yourdate = test.scheduled
    curr = get_curr_time()
    
    sched_obj = FlexabilitySim.objects.filter(scheduled=curr)
    if(sched_obj):
        for obj in sched_obj:
            dev = obj.provided_dev
            pow = obj.sched_pow
            timer = int(obj.sched_durration)*60
            due_date = curr[:-1]
            stamp = date_to_timestamp(due_date)+timer            
            topic = str(dev+"/correction")
            for d in sm_coeff:
                coeff = d.get(dev, None)
                if coeff:
                    pow = pow/coeff
                    pow = round(pow, 2)
            single_data = {
                "power":pow,
                "due_sim_stamp":stamp
            }
            publish.single(topic, str(single_data), hostname="159.89.103.242", port=1883)
    else:
        print("There is no objects")
    actual_provide = Flexi.objects.filter(response_time=curr)
    if (actual_provide):
        for act_obj in actual_provide:
            dev_id = act_obj.flexiDev
            power = act_obj.res_pow
            duration = int(act_obj.res_durr)*60
            due_date_actual = curr[:-1]
            stamp_actual = date_to_timestamp(due_date_actual)+duration
            actual_topic = str(dev_id+"/actualProvide")
            for d in sm_coeff:
                coeff = d.get(dev_id, None)
                if coeff:
                    power = power/coeff
                    power = round(power, 2)
            actual_data = {
                "power":power,
                "due_stamp":stamp_actual
            }
            publish.single(actual_topic, str(actual_data), hostname="159.89.103.242", port=1883)
            

def exec_all():

    today = get_curr_time()
    future_reqs = Flexi.objects.filter(response_time__gte=today)
    if future_reqs:
        for req in future_reqs:
            dev_req = req.flexiDev
            scheduled_req = req.response_time
            pow_req = req.res_pow
            durr_req = req.res_durr
            FlexabilitySim.objects.get_or_create(provided_dev=dev_req,scheduled=scheduled_req,sched_pow=pow_req,sched_durration=durr_req)

def get_hydro():
    
    
    url = "https://api.thingspeak.com/channels/867128/feeds.json?api_key=9KHXETAXFJX8DWF9&results=2"

    r = requests.get(url)
    page_content = r.text
    # It turns out Flickr escapes single quotes (')
    # and apparently this isn't allowed and makes the JSON invalid.
    # we use String.replace to get around this
    probably_json = page_content.replace("\\'", "'")
    # now we load the json
    feed = json.loads(probably_json)
    data = feed["feeds"][-1]
    #iso_date = data["created_at"]
    power = data["field1"]
    guide_vains = data["field2"]
    level = data["field3"]
    gen_tmp = data["field4"]
    gen_u = data["field5"]
    gen_curr = data["field6"]
    
    #date_part = iso_date.split("T")[0].split("-")

    # year = int(date_part[0])
    # month = int(date_part[1])
    # day = int(date_part[2])

    # hour_part = iso_date.split("T")[1].split(":")

    # hour = int(hour_part[0])
    # last_min = int(hour_part[1])
    # now = datetime.now()

    # year_part = int(str(now).split(" ")[0].split("-")[0])
    # month_part = int(str(now).split(" ")[0].split("-")[1])
    # day_part = int(str(now).split(" ")[0].split("-")[2])

    # hour_part = int(str(now).split(" ")[1].split(":")[0])
    # min_part = int(str(now).split(" ")[1].split(":")[1])
    # sec_part = str(now).split(" ")[1].split(":")[2]

    # sec_part = float(sec_part)
    # sec_part = int(sec_part)
    # timestamp_now = datetime(year_part, month_part, day_part, hour_part, min_part, sec_part, tzinfo=pytz.utc).timestamp()    
    timestamp_now = round(time.time() * 1000)
    stamp = str(timestamp_now)
    HY_PW = str(power)
    HY_GV = str(guide_vains)
    HY_WL = str(level)
    HY_GT = str(gen_tmp)
    HY_GC = str(gen_curr)
    HY_GVOL = str(gen_u)
    hydro = {
        #"timestamp":stamp,
        "HY_PW":HY_PW,
        "HY_GV":HY_GV,
        "HY_WL":HY_WL,
        "HY_GT":HY_GT,
        "HY_GC":HY_GC,
        "HY_GVOL":HY_GVOL,
        "HY_ALRM":0
    }    
    
    topic = 'hydro'
    publish.single(topic, str(hydro), hostname="159.89.103.242", port=1883)
    if power:
        count = Hydro.objects.all().count()
        # if count > 200:
        #     Hydro.objects.all().delete()
        #Hydro.objects.create(hydro_pow=power,guide_vains=guide_vains,level=level,gen_temp=gen_tmp,gen_curr=gen_curr,gen_vol=gen_u)
        
    #return data
    

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
    
    
def price_csv():
    #Price.objects.all().delete()
    test = os.path.join(settings.BASE_DIR, 'day-ahead.csv')    
    today = date.today()
    with open(test, 'r') as file:
        csvreader = csv.reader(file)
        for row in csvreader:
            date_p = row[0].split(" - ")[0]
            date_part = date_p.split(" ")[0]
            hour_part = "T"+date_p.split(" ")[1]+":00Z"
            date_convert = date_part.split(".")
            
            if len(date_convert) == 3:
                compare_date = date_convert[2]+"-"+date_convert[1]+"-"+date_convert[0] 
                past = datetime.strptime(compare_date, "%Y-%m-%d")
            
                if past.date() < today:    
                    date_fin = compare_date+hour_part       
                    
                    price = row[1]
                    if price:
                        bgn_price = float(row[1])*1.96
                        bgn_price = round(bgn_price, 2)                                            
                        exist = Price.objects.filter(timestamp=date_fin)       
                        if exist.first():
                            pass
                        else:
                            Price.objects.create(timestamp=date_fin, value=bgn_price)    
                            
                            
def fetch_simavi():    

    devs = ["sm-0008","sm-0030"] 
    for d in devs:
        url = 'http://ec2-35-180-235-215.eu-west-3.compute.amazonaws.com:8080/flexigrid/api/emax/data/bulgaria?deviceName='+d+'&fromDate=2023-06-21 12:00:00&toDate=2023-06-21 13:00:00'
        response=requests.get(url)
        content = response.text
        json_data = content.replace("\\'", "'")
        data_feed = json.loads(json_data)    
        sm = data_feed.get("smartmeters")  
        for data in sm:
            stamp = data.get("timestamp3m", None)
            date_part = stamp.split("T")[0]
            time_part = stamp.split("T")[1]
            time_helper = time_part.split("Z")[0] 
            str_date = date_part +" "+time_helper    
            datetime_object = datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S')+timedelta(hours=3)
            power = data.get("power", None)        
            exist = Post.objects.filter(created_date=stamp,devId = d,value=power)
            #if start_date < datetime_object < end_date:
        
            if exist.count() > 0:
                pass
            else:
                Post.objects.create(created_date=datetime_object,devId = d,value=power)

                   
                
def clear_forecast_data(range, dev):
        if range == 'today':
            exist = PostForecast.today.filter(devId = dev+'F').count()           
            if exist > 0:
                PostForecast.today.filter(devId = dev+'F').delete()
                
                
def make_auto_forecast():
    devs = ["sm-0002","sm-0004","sm-0006","sm-0009","sm-0010","sm-0011","sm-0012","sm-0013","sm-0014","sm-0016","sm-0017","sm-0018","sm-0019","sm-0020","sm-0022","sm-0024", "sm-0025", "sm-0030"] 
    batch_size = 5  # Define the batch size
    for i in range(0, len(devs), batch_size):
        batch = devs[i:i + batch_size]  # Extract a batch of devices
        for d in batch:
            # Perform necessary checks and operations
            exist = PostForecast.today.filter(devId=d+'F').count()
            if exist > 0:
                pass
            else:
                topic = "tensor/today"
                publish.single(topic, str(d), hostname="159.89.103.242", port=1883)
        time.sleep(1)  # Add a delay before processing the next batch

            
            
def slow_month_all():
    try:
        today = datetime.today()
        datem = str(datetime(today.year, today.month, 1))
        datem = datem.split(" ")[0]
        result = Post.month.filter(created__gte=datem)
        cache.set('cached_data_all_month', result, timeout=60*20)  # Cache for 20 minutes        
    except Exception as e:
        # Log any exceptions that occur during the slow query or caching process
        logger.error(f"An error occurred during caching: {str(e)}")

def slow_year_all():
    try:
        year_query = Post.year.all()
        cache.set('cached_data_all_year', year_query, timeout=60*20)  # Cache for 20 minutes        
    except Exception as e:
        # Log any exceptions that occur during the slow query or caching process
        logger.error(f"An error occurred during caching: {str(e)}")

        
def slow_today_all():
    try:       
               
        today_query = Post.today.all().order_by('created_date').values('devId', 'created_date', 'value')
        if today_query.exists():           
            # Serialize and cache smaller chunks of data (e.g., 100 records per cache entry)
            cache.set(f'cached_data_all_today', today_query, timeout=60*15)
            
            logger.info("Data cached successfully")
        else:
            logger.warning("Today's data is empty, not caching.")
        
    except DatabaseError as db_error:
        logger.error(f"Database error occurred: {db_error}")
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")



def db_consistency():
    all = create_devs()
    for d in all:    
        # add timedelta because the server time settings    
        current_date = datetime.now() + timedelta(hours=2)
        curr_date_str = current_date.strftime('%Y-%m-%dT%H:%M')    
           
        existing = PostConsistency.objects.filter(devId=d, created_date=curr_date_str)        
        if not existing:
            last_data = PostConsistency.objects.filter(devId=d).last()
            if last_data:
                pow = last_data.value                
                PostConsistency.objects.create(devId=d, created_date=curr_date_str, value=pow)
                

def fill_prices():
    prices = Price.objects.all().order_by('timestamp')
    last_price = prices.last()
    
    if last_price is None:
        raise ValueError("No prices available to fill from.")

    last_timestamp = last_price.timestamp

    for i in range(24):  # Generate 24 new prices for the next 24 hours
        last_timestamp += timedelta(hours=1)
        new_price = Price(timestamp=last_timestamp, value=last_price.value)  # Use .value
        new_price.save()
        
       

    
def create_devs():
    all_devs = list()
    for i in range(30):
        if i < 10:
            all_devs.append(f"sm-000{i}")
        else:
            all_devs.append(f"sm-00{i}")
    return all_devs



def today_resample_15min():
    devs = create_devs()
    for dev in devs:
        dataset = Post.today.filter(devId=dev).order_by('created_date')
        data = list(dataset.values())
        df = pd.DataFrame(list(data))

        if 'created_date' not in df.columns:            
            continue  # Skip this device if no 'created_date' found

        df['created_date'] = pd.to_datetime(df['created_date'], utc=True)  # Ensuring it's in UTC
        df.set_index('created_date', inplace=True)
        # Resample to 15 minutes, summing the values in each interval
        resampled_df = df.resample('15T').mean(numeric_only=True) 
        resampled_df = resampled_df.fillna(method='ffill')
        resampled_df['devId'] = dev        
        
        resampled_data = resampled_df.reset_index().to_dict(orient='records')       

        cache_key = f'cache_15min_today_{dev}'        
        cache.set(cache_key, resampled_data, timeout=60*5)

def today_resample_30min():
    devs = create_devs()
    for dev in devs:
        dataset = Post.today.filter(devId=dev).order_by('created_date')
        data = list(dataset.values())
        df = pd.DataFrame(list(data))

        if 'created_date' not in df.columns:            
            continue  # Skip this device if no 'created_date' found

        df['created_date'] = pd.to_datetime(df['created_date'], utc=True)  # Ensuring it's in UTC
        df.set_index('created_date', inplace=True)
        # Resample to 15 minutes, summing the values in each interval
        resampled_df = df.resample('30T').mean(numeric_only=True) 
        resampled_df = resampled_df.fillna(method='ffill')
        resampled_df['devId'] = dev        
        
        resampled_data = resampled_df.reset_index().to_dict(orient='records')       

        cache_key = f'cache_30min_today_{dev}'        
        cache.set(cache_key, resampled_data, timeout=60*5)

def today_resample_45min():
    devs = create_devs()
    for dev in devs:
        dataset = Post.today.filter(devId=dev).order_by('created_date')
        data = list(dataset.values())
        df = pd.DataFrame(list(data))

        if 'created_date' not in df.columns:            
            continue  # Skip this device if no 'created_date' found

        df['created_date'] = pd.to_datetime(df['created_date'], utc=True)  # Ensuring it's in UTC
        df.set_index('created_date', inplace=True)
        # Resample to 15 minutes, summing the values in each interval
        resampled_df = df.resample('45T').mean(numeric_only=True) 
        resampled_df = resampled_df.fillna(method='ffill')
        resampled_df['devId'] = dev        
        
        resampled_data = resampled_df.reset_index().to_dict(orient='records')       

        cache_key = f'cache_45min_today_{dev}'        
        cache.set(cache_key, resampled_data, timeout=60*5)    
    

def today_resample_60min():
    devs = create_devs()
    for dev in devs:
        dataset = Post.today.filter(devId=dev).order_by('created_date')
        data = list(dataset.values())
        df = pd.DataFrame(list(data))

        if 'created_date' not in df.columns:            
            continue  # Skip this device if no 'created_date' found

        df['created_date'] = pd.to_datetime(df['created_date'], utc=True)  # Ensuring it's in UTC
        df.set_index('created_date', inplace=True)
        # Resample to 60 minutes, summing the values in each interval
        resampled_df = df.resample('60T').mean(numeric_only=True) 
        resampled_df = resampled_df.fillna(method='ffill')
        resampled_df['devId'] = dev           
        
        resampled_data = resampled_df.reset_index().to_dict(orient='records')          
        cache_key = f'cache_60min_today_{dev}'
        cache.set(cache_key, resampled_data, timeout=60*5)    




