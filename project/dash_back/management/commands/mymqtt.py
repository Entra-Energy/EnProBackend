from django.core.management.base import BaseCommand
import random
import json
from dash_back.models import Post, Online, Flexi, Aris, PostForecast, PostConsistency 
from paho.mqtt import client as mqtt_client 
from datetime import datetime, timezone,timedelta
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from django.utils import timezone as tzone
from django.utils.timezone import activate
import pytz
from pytz import timezone, UTC
from django.db.models import Sum

#from dash_back.tasks import task_mqtt_error 
import time
import re

#from pytz import timezone

class Command(BaseCommand):

    help = 'MQTT Processing'

    def handle(self, *args, **kwargs):
        
        self.error_objects = []
        self.counter = 0
        self.t1 = 0
        self.t2 = 0
        self.gps = ''
        self.newGps = ''
        
        def validateJSON(jsonData):
            try:
                json.loads(jsonData)
            except ValueError as err:
                return False
            return True   
        
        def parse_mqtt_payload(message: str):
            # Strip outer message and isolate the payload content
            payload_raw = message.split('"payload":')[1].strip().lstrip('{').rstrip('}}')
            items = payload_raw.split(',')

            result = {}
            for item in items:
                if ':' in item:
                    key, value = item.split(':')
                    result[key.strip()] = float(value)                          
            return result

        

                
        def on_connect(client, userdata, flags, rc):
            #print("Connected with result code "+str(rc))
            # Subscribing in on_connect() means that if we lose the connection and
            # reconnect then subscriptions will be renewed.
            client.subscribe("data/#")
            client.subscribe("ping/#")
            #client.subscribe("error/check/#")
            client.subscribe("flexiResponse/#")
            client.subscribe("corrResponse/#")
            client.subscribe("windData")
            client.subscribe("init/#")
            #client.subscribe("err/#")
            client.subscribe("forecast/#")   
               
            

        def on_message(client, userdata, msg):
            #print(msg.topic+" "+str(msg.payload))
            topic = msg.topic
            #print(topic)
            
            myList = topic.split('/')               
            sm_coeff = [{"sm-0001":120},{"sm-0002":320},{"sm-0003":400},{"sm-0004":200},{"sm-0006":200},{"sm-0008":200},{"sm-0009":80},
                        {"sm-0010":60},{"sm-0011":60},{"sm-0015":60},{"sm-0016":250},{"sm-0017":200},{"sm-0018":400},{"sm-0019":500},{"sm-0020":500},{"sm-0025":200}]

            if myList[0] == 'data':
                dev_id = myList[1]                 
                
                is_valid = validateJSON(msg.payload)
                if is_valid:                      
                    data_out=json.loads(msg.payload.decode())     
                    #print(f"DATA OUT: {data_out}")
                    payload = data_out.get('payload',{})
                    if payload:
                                   
                        timestamp = payload.get('timestamp', None)
                        if timestamp:
                            timestamp = int(timestamp)    
                                           
                            #print(f"Timestamp: {timestamp}")
                            #timestamp = datetime.utcfromtimestamp(timestamp).isoformat() for local
                           
                            timestamp = datetime.fromtimestamp(timestamp).isoformat()                           
                           
                            value = float(data_out['payload'].get('power', 0))  # Use get with a default value                         
                            
                            # readyness = data_out['payload'].get('gridReady', None)
                            # if readyness:
                            #     ready_grid = readyness
                            # else:
                            #     ready_grid = 0
                        
                            # costHour = data_out['payload'].get('costH', None)
                            # if costHour:
                            #     costH = costHour
                            # else:
                            #     costH = 0
                            # costDay = data_out['payload'].get('costD', None)
                            # if costDay:
                            #     costD = costDay
                            # else:
                            #     costD = 0
                            # costMonth = data_out['payload'].get('costM', None)
                            # if costMonth:
                            #     costM = costMonth
                            # else:
                            #     costM = 0                    
                        
                            # budgetHour = data_out['payload'].get('budgetH', None)
                            # if budgetHour:
                            #     budgetH = budgetHour
                            # else:
                            #     budgetH = 0              
                            # budgetDay = data_out['payload'].get('budgetD', None)                    
                            # if budgetDay:
                            #     budgetD = budgetDay
                            # else:
                            #     budgetD = 0   
                            # budgetMonth= data_out['payload'].get('budgetM', None)                    
                            # if budgetMonth:
                            #     budgetM = budgetMonth
                            # else:
                            #     budgetM = 0     
                                
                            # providing_amount = data_out['payload'].get('providingAmount', None)
                            # if providing_amount:
                            #     prov_amount =  providing_amount
                            # else:
                            #     prov_amount = 0
                            # actual_correction = data_out['payload'].get('actualCorr', None)
                            # if actual_correction:
                            #     actual_corr = actual_correction
                            # else:
                            #     actual_corr = 0
                            # actual_providing = data_out['payload'].get('actualProvide', None)
                            # if actual_providing:
                            #     actual_prov = actual_providing
                            # else:
                            #     actual_prov = 0 
                            producer_corrected_value = None
                            for d in sm_coeff:
                                coeff = d.get(dev_id, None)
                                if coeff is not None:  
                                    value *= coeff  
                                    value = round(value, 2) 
                                    if dev_id == 'sm-0001':
                                        if value > 30:
                                            correction = value - 30
                                            producer_corrected_value = value - 2*correction
                                            producer_corrected_value = round(producer_corrected_value, 2) 
                                    elif dev_id == 'sm-0016':
                                        if value > 65:
                                            correction = value - 65
                                            producer_corrected_value = value - 2*correction
                                            producer_corrected_value = round(producer_corrected_value, 2) 

                            if producer_corrected_value is not None:
                                value = producer_corrected_value
                                      
                            
                            exist = Post.objects.filter(created_date=timestamp, devId=dev_id)      
                            exist_consist = PostConsistency.objects.filter(created_date=timestamp, devId=dev_id)    
                           
                            if exist_consist.exists():
                                exist_consist.update(value=value)
                            else:
                                PostConsistency.objects.create(devId=dev_id, value=value, created_date=timestamp)

                            if exist.exists():
                                # If the object exists, update its value
                                exist.update(value=value)          
                            else:                                
                                Post.objects.create(devId=dev_id, value=value, created_date=timestamp)
                            #fill the consistent DB
                            # exist_consist = PostConsistency.objects.filter(created_date=timestamp, devId=dev_id)                          
                            # if exist_consist.exists():
                            #     exist_consist.update(value=value)
                            # else:
                            #     PostConsistency.objects.create(devId=dev_id, value=value, created_date=timestamp)


                            



                                #Post.objects.get_or_create(devId=dev_id,value=value,created_date=timestamp,grid=ready_grid, costH = costH, costD = costD, costM = costM, budgetH = budgetH,budgetD = budgetD, budgetM = budgetM, providingAmount= prov_amount,actualCorr=actual_corr,actualProviding=actual_prov )
            # if myList[0] == 'err':
            #     dev_id = myList[1]                
            #     data_out = json.loads(msg.payload.decode())
            #     timestamp = int(data_out['payload']['timestamp'])

            #     now = datetime.now(timezone('Europe/Sofia'))

            #     timestamp_iso = datetime.fromtimestamp(timestamp).isoformat()
            #     value = float(data_out['payload'].get('power', 0))  # Use get with a default value
            #     for d in sm_coeff:
            #         coeff = d.get(dev_id, None)
            #         if coeff is not None:  # Check for None explicitly
            #             value *= coeff  # Simplify calculation
            #             value = round(value, 2)

            #     exist = Post.objects.filter(created_date=timestamp_iso, devId=dev_id)
            #     if exist:
            #         pass
            #     else:
            #         Post.objects.get_or_create(devId=dev_id, value=value, created_date=timestamp_iso)
                    
            # if myList[0] == 'err':
            #     dev_id = myList[1]                
            #     data_out = json.loads(msg.payload.decode())
            #     timestamp = int(data_out['payload']['timestamp'])
            #     timestamp_iso = datetime.fromtimestamp(timestamp).isoformat()
            #     value = float(data_out['payload']['power'])                
            #     error_obj = {"devId":dev_id,"value":value,"created_date":timestamp_iso,"timestamp":timestamp}  
            #     has_entry = any(x for x in self.error_objects if x["timestamp"] == timestamp and x["devId"] == dev_id) 
            #     #print(has_entry)                          
            #     if has_entry:
            #         pass
            #     else:                                             
            #         self.error_objects.append(error_obj)
            #         #print(len(self.error_objects))
            #         if len(self.error_objects) >= 200:
            #             task_mqtt_error(self.error_objects)
            #             self.error_objects = []
                     
                
                # dev_id = myList[1]                
                # is_valid = validateJSON(msg.payload)
                # if is_valid:                      
                #     data_out=json.loads(msg.payload.decode())                
                #     timestamp = int(data_out['payload']['timestamp'])
                #     return_stamp = timestamp
                #     timestamp = datetime.fromtimestamp(timestamp).isoformat()
                #     value = float(data_out['payload']['power'])            
                #     #exist = Post.objects.filter(devId=dev_id, created_date=timestamp)     
                #     #if exist:                    
                #     topic = dev_id + "/timestamp"                       
                #     jObj = {
                #     "time": return_stamp,
                #     "pow": 0,
                #     }       
              
                #     publish.single(topic, str(jObj), hostname="159.89.103.242", port=1883)
                #     #else:
                #     Post.objects.get_or_create(devId=dev_id,value=value,created_date=timestamp)
                                
            if myList[0] == 'ping':     
                       
                dev_id = myList[1]
                is_valid = validateJSON(msg.payload)
                if is_valid:      
                
                    data_out=json.loads(msg.payload.decode())
                    
                    timestamp = (data_out['payload'].get('timestamp', None))
                    if timestamp:
                        #print(timestamp)
                        timestamp = datetime.fromtimestamp(timestamp).isoformat()
                    else:
                        timestamp = 0
                    value = float(data_out['payload']['power'])
                    gridSupp = data_out['payload'].get('gridReady', None)
                    dev_name = data_out['payload'].get('blynkName', None)
                    lat = data_out['payload'].get('lat', None)
                    long = data_out['payload'].get('long', None)


                    if gridSupp:
                        ready = int(gridSupp)
                    else:
                        ready = 0
                    signal = data_out['payload'].get('signal', None)
                    if signal:
                        connectivity = int(signal)
                    else:
                        connectivity = 0
                    providing = data_out['payload'].get('providing', None)
                    if providing:
                        prov = int(providing)
                    else:
                        prov = 0
                    if dev_name:
                        name = str(dev_name)
                    else:
                        name = 'lab'
                    
                    if lat == "null" or lat == None:
                        latitude = 0.0
                    else:
                        latitude = float(lat)
                    if long == "null" or long == None:
                        longitude = 0.0
                    else:
                        longitude = float(long)
                    online = Online.objects.all().count()

                    if online > 1000:
                        Online.objects.all().delete()
                    #print(prov)
                    if dev_id == 'sm-0000':
                        pass
                    else:
                        #print(timestamp)
                        for d in sm_coeff:
                            coeff = d.get(dev_id,None)
                            if coeff:
                                value = value*coeff
                                value = round(value, 2)
                        Online.objects.create(dev=dev_id, saved_date=timestamp, pow=value, ready=ready,signal=connectivity,providing = prov, dev_name = name, lat = latitude, long = longitude)

            if myList[0] == 'error':
                pass
                
                # print("ERRROORRR ERRRRORRRR ERRRRORRRR")
                # dev_id = myList[2]
                # data_out = json.loads(msg.payload.decode())
                # timestamp = int(data_out['payload']['timestamp'])
                # timestamp_iso = datetime.fromtimestamp(timestamp).isoformat()
                # value = float(data_out['payload']['power'])                
                # error_obj = {"devId":dev_id,"value":value,"created_date":timestamp_iso,"timestamp":timestamp}  
                # has_entry = any(x for x in self.error_objects if x["timestamp"] == timestamp and x["devId"] == dev_id) 
                # print(has_entry)                          
                # if has_entry:
                #     pass
                # else:                                             
                #     self.error_objects.append(error_obj)
                #     print(len(self.error_objects))
                #     if len(self.error_objects) >= 100:
                #         task_mqtt_error(self.error_objects)
                #         self.error_objects = []
                    
                
                
                
                
                
                #data_out = json.loads(msg.payload.decode())
                # timestamp = int(data_out['payload']['timestamp'])
                # timestamp_iso = datetime.fromtimestamp(timestamp).isoformat()
                # value = float(data_out['payload']['power'])
                
                # test = Post.objects.filter(created_date=timestamp_iso,devId = dev_id)
                # topic = dev_id + "/timestamp"
                # if test.count() == 1:
                #     jObj = {
                #         "time": timestamp,
                #         "pow": 0,
                #         }
                #     publish.single(topic, str(jObj), hostname="159.89.103.242", port=1883)
                # else:
                #     Post.objects.create(devId=dev_id,value=value,created_date=timestamp_iso)
                
                # jObj = {
                # "time": timestamp,
                # "pow": value,
                # }
                #publish.single(topic, str(jObj), hostname="159.89.103.242", port=1883)

            # if myList[0] == 'flexiResponse':
            #     dev_id = myList[1]
            #     data_out=json.loads(msg.payload.decode())
            #     time = int(data_out['payload']['date'])
            #     time_iso = datetime.fromtimestamp(time).isoformat()
            #     res_power = float(data_out['payload']['power'])
            #     durr = int(data_out['payload']['duration'])
            #     Flexi.objects.create(flexiDev = dev_id, response_time = time_iso, res_pow = res_power, res_durr = durr)


            # if myList[0] == 'corrResponse':
            #     dev_id = myList[1]
            #     data_out=json.loads(msg.payload.decode())
                #print(data_out)
            # if topic == 'windData':
            #     data_aris = json.loads(msg.payload.decode())
            #     wind = data_aris["wind"]
            #     wind = round(wind,2)
            #     power = data_aris["active"]
            #     power = round(power, 2)
            #     now = datetime.now()
            #     now = str(now)
            #     currDate = now.split(" ")[0]+"T"
            #     cur_hour = now.split(" ")[1].split(":")[0]
            #     cur_hour_min = now.split(" ")[1].split(":")[1]
            #     query_date = currDate+cur_hour+":"+cur_hour_min+":00Z"
            #     Aris.objects.create(power_aris=power,wind_aris=wind,timestamp_aris = query_date)
            
            if myList[0] == 'init':
                print("Received INIT")
                dev_id = myList[1]
                now = datetime.now(timezone('Europe/Sofia'))
                now_setTime = datetime.now(timezone('Europe/Sofia'))
                datem = str(datetime(now.year, now.month, 1))
                datem = datem.split(" ")[0]
                now = str(now)
                currDate = now.split(" ")[0]
                cur_hour = now.split(" ")[1].split(":")[0]
                query_hour = currDate+" "+cur_hour+":"+"00"
                for_last_hour = Post.objects.filter(created_date__gte=query_hour,devId = dev_id).aggregate(Sum('value'))
                if for_last_hour['value__sum']:                    
                    for_last_hour_consumption = float(for_last_hour['value__sum'])
                else:
                    for_last_hour_consumption = 0 
                
                for_today = Post.today.filter(devId=dev_id).aggregate(Sum('value'))
                if for_today['value__sum']:
                    for_today_consumption = float(for_today['value__sum'])
                else:
                    for_today_consumption = 0
                for_month = Post.objects.filter(devId=dev_id,created_date__gte=datem).aggregate(Sum('value'))
                if for_month['value__sum']:
                    for_month_consumption = float(for_month['value__sum'])
                else:
                    for_month_consumption = 0
                last_obj = Post.objects.filter(devId = dev_id).last()
                if last_obj:
                    grid = int(last_obj.grid)
                    costPerH = float(last_obj.costH)
                    costPerD = float(last_obj.costD)
                    costPerM = float(last_obj.costM)
                    budgetPerH = int(last_obj.budgetH)
                    budgetPerD = int(last_obj.budgetD)
                    budgetPerM = int(last_obj.budgetM)
                    
                else:
                    grid = 0
                    costPerH = 0
                    costPerD = 0
                    costPerM = 0
                    budgetPerH = 0
                    budgetPerD = 0
                    budgetPerM = 0
                
                consum_obj = {
                    # 'for_hour':for_last_hour_consumption,
                    # 'for_today':for_today_consumption,
                    # 'for_month':for_month_consumption,
                    # 'ready':grid,
                    # 'costH':costPerH,
                    # 'costD':costPerD,
                    # 'costM':costPerM,
                    # 'budgetH':budgetPerH,
                    # 'budgetD':budgetPerD,
                    # 'budgetM':budgetPerM,
                    'setY': now_setTime.year,
                    'setM': now_setTime.month,
                    'setD':now_setTime.day,
                    'setH':now_setTime.hour,
                    'setmm':now_setTime.minute,
                    'setS':now_setTime.second
                    
                }                
                #print(str(consum_obj))
                topic = "initial/"+dev_id
                publish.single(topic,str(consum_obj),hostname="159.89.103.242",port=1883)
                
            if myList[0] == 'forecast':
                dev_forecast = myList[1]
                range = myList[2]                      
                json_data = msg.payload.decode().replace("'", '"')
                data_out = json.loads(json_data)        
                date = data_out['date']
                power = float(data_out['power'])
                #print(f"Here: {data_out}")
                lost = float(data_out['loss'])
                mae = float(data_out['mae'])
                
                if range == 'today':
                    exist = PostForecast.today.filter(devId = dev_forecast+'F').count()
                    #print(f"EXIST SIZE:{exist}")    
                    if exist <= 24:
                        PostForecast.objects.create(devId = dev_forecast+'F',created_date=date, value=power,model_loss=lost, mean_abs_error = mae)
                        
                        
               
      
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message

        client.connect("159.89.103.242", 1883)
        #client.connect_async("159.89.103.242", 1883)

        client.loop_forever()
        #client.loop_start()

        #May be try also setConnectionTimeout(0)