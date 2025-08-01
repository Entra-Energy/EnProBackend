from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import viewsets, generics
from dash_back.serializers import PostSerializer, OnlineSerializer, PriceSerializer, FlexiSerializer, ArisSerializer, UserIpSerializer, PostForecastSerializer,PostConsistencySerializer,\
    FlexiSimSerializer,GridAsignSerializer, CapaAsignSerializer, PostForecastMonthSerializer, Resample15MinSerializer, SikoSerializer
from dash_back.models import Post, Online, Price, Flexi, FlexabilitySim, Aris, UserIp, PostForecast, GridAsign, CapaAsign, PostForecastMonth, PostConsistency
from datetime import datetime, timedelta
from dash_back.custom_filters import PriceFilter, ArisFilter
from dash_back.tasks import task_exec_all, task_clear
import paho.mqtt.publish as publish
import time
import datetime as dt
from pytz import timezone
from django.core.cache import cache
from django.db.models import Min, Max, Avg, Sum
from celery.result import AsyncResult
from django.utils.dateparse import parse_datetime
from rest_framework import status
import logging
import os
import csv
logger = logging.getLogger(__name__)


#from django.views.decorators.cache import cache_page
#from dash_back.paginations import CustomPagination



# class ArisViewset(viewsets.ModelViewSet):
#     queryset = Aris.objects.all().order_by('timestamp_aris')
#
#     serializer_class = ArisSerializer
#     filter_class = ArisFilter


class userIpViewset(viewsets.ModelViewSet):    
    queryset = UserIp.objects.all()
    serializer_class = UserIpSerializer
    
    
class GridViewset(viewsets.ModelViewSet):    
    queryset = GridAsign.objects.all()
    serializer_class = GridAsignSerializer
    
    
class CapaViewset(viewsets.ModelViewSet):    
    queryset = CapaAsign.objects.all()
    serializer_class = CapaAsignSerializer


class ArisViewset(viewsets.ModelViewSet):
    def get_queryset(self):
        today = datetime.today()
        datem = str(datetime(today.year, today.month, 1))
        datem = datem.split(" ")[0]
        range = self.request.query_params.get('date_range',None)
        if range is not None:
            if range == 'today':
                queryset = Aris.today.all().order_by('timestamp_aris')
            if range == 'year':
                queryset = Aris.month.all()
            if range == 'month':
                queryset = Aris.month.filter(created__gte=datem)
            return queryset


    serializer_class = ArisSerializer


class MinMaxAvg(APIView):
    '''
    endpoint for accumulate min,
    max and avarrage for a given period!
    '''
    def get(self, request):
        date_range = request.query_params.get('date_range', None)
        dev_id = request.query_params.get('devId', None)
        moment_value = 0
        if date_range == 'today':
            try:
                if dev_id is not None:                   
                   min_queryset = Post.today.filter(devId=dev_id).aggregate(min_value=Min('value'))['min_value']
                   max_queryset = Post.today.filter(devId=dev_id).aggregate(max_value=Max('value'))['max_value']
                   average_value = Post.today.filter(devId=dev_id).aggregate(average_value=Avg('value'))['average_value']
                   moment_value = Post.today.filter(devId=dev_id).last()
                   moment_value = moment_value.value
                   today_overview_single = {
                       'min':min_queryset,
                       'max':max_queryset,
                       'avg':average_value,
                       'mom':moment_value
                   }

                   return Response({'today_overview_single':today_overview_single })

                else:
                    today_overview = Post.todayMinMaxAvg.all()
                    return Response({'today_overview': today_overview})
            
                #return Response(serialized_data)
            except Exception as e:
                return Response({'error': str(e)}, status=500)
        
        elif date_range == 'month':
            try:
                if dev_id is not None:
                    min_queryset = Post.month.filter(devId=dev_id).aggregate(min_value=Min('value'))['min_value']
                    max_queryset = Post.month.filter(devId=dev_id).aggregate(max_value=Max('value'))['max_value']
                    average_value = Post.month.filter(devId=dev_id).aggregate(average_value=Avg('value'))['average_value']
                    # moment_value = Post.month.filter(devId=dev_id).last()
                    # moment_value = moment_value["value"]
                    month_overview_single = {
                       'min':min_queryset,
                       'max':max_queryset,
                       'avg':average_value,
                       'mom':moment_value
                   }
                    return Response({'month_overview_single':month_overview_single })

                else:
                    month_overview = Post.monthMinMaxAvg.all()            
                    return Response({'month_overview': month_overview})
            except Exception as e:
                return Response({'error': str(e)}, status=500)
        
        elif date_range == 'year':
            try:
                if dev_id is not None:
                    min_queryset = Post.year.filter(devId=dev_id).aggregate(min_value=Min('value'))['min_value']
                    max_queryset = Post.year.filter(devId=dev_id).aggregate(max_value=Max('value'))['max_value']
                    average_value = Post.year.filter(devId=dev_id).aggregate(average_value=Avg('value'))['average_value']
                    # moment_value = Post.year.filter(devId=dev_id).last()
                    # moment_value = moment_value["value"]
                    year_overview_single = {
                       'min':min_queryset,
                       'max':max_queryset,
                       'avg':average_value,
                       'mom':moment_value
                   }
                    return Response({'year_overview_single':year_overview_single })
                    
                else:
                    year_overview = Post.yearMinMaxAvg.all()

                    return Response({'year_overview': year_overview})               
            except Exception as e:
                return Response({'error': str(e)}, status=500)
        
        # else:
        #     return Response({'message': 'Invalid date range'}, status=400)

        # if date_range == 'today':                        

        # else:
        #     return Response({'error': 'Invalid date_range provided'}, status=400)




class SikoViewset(viewsets.ModelViewSet):
    serializer_class = PostSerializer

    def get_queryset(self):
        request = self.request
        date_range = request.query_params.get('date_range', None)
        device = request.query_params.get('dev', None)
        start_time_str = request.query_params.get('start_time', None)
        end_time_str = request.query_params.get('end_time', None)

        sikoDevs = [f"sm-{i}" for i in range(40, 66)] + [
            'sm-81', 'sm-82', 'sm-96', 'sm-97', 'sm-94', 'sm-95', 'sm-91', 'sm-92', 'sm-93'
        ]

        if device not in sikoDevs:
            device = None

        filter_kwargs = {
            'devId__in': [device] if device else sikoDevs
        }

        # ✅ Prioritize manual time range
        try:
            if start_time_str or end_time_str:
                if start_time_str:
                    start_time = datetime.fromisoformat(start_time_str)
                    filter_kwargs['created_date__gte'] = start_time
                if end_time_str:
                    end_time = datetime.fromisoformat(end_time_str)
                    filter_kwargs['created_date__lte'] = end_time

                queryset = Post.objects.filter(**filter_kwargs).order_by('created_date')
                return queryset
        except ValueError as e:
            print("Invalid datetime format:", e)
            return Post.objects.none()

        # ✅ If no manual time, fall back to date_range
        if not date_range:
            return Post.objects.none()

        now = datetime.now()

        if date_range == 'today':
            return Post.today.filter(**filter_kwargs).order_by('created_date')

        elif date_range == 'month':
            start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            return Post.month.filter(
                created__gte=start_of_month,
                created__lte=now,
                **filter_kwargs
            ).order_by('created')

        elif date_range == 'year':
            
            return Post.year.filter(               
                **filter_kwargs
            ).order_by('created')

        return Post.objects.none()
    
    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer = self.get_serializer(queryset, many=True)

        # Calculate total power consumption (sum of 'value')
        total_watts = queryset.aggregate(total=Sum('value'))['total'] or 0
        total_kwh = round((total_watts / 1000) * 0.25, 2)

        serializer = self.get_serializer(queryset, many=True)
        return Response({
            "total_consumption_kwh": total_kwh,
            "data": serializer.data
        })


class GenerateExcelAPIView(APIView):
    def get(self, request, *args, **kwargs):
        excel = request.query_params.get('excel', None)

        if excel is None:
            return Response({"detail": "Missing 'excel' parameter."}, status=status.HTTP_400_BAD_REQUEST)

        # Generate device IDs
        siko_devs_ids = [f"sm-{i}" for i in range(40, 66)]

        # Query the data
        queryset = Post.objects.filter(devId__in=siko_devs_ids).values('devId', 'created_date', 'value').order_by('created_date')

        # Define file path (adjust path as needed)
        filename = f"siko_data.csv"
        file_path = os.path.join('exports', filename)

        # Ensure directory exists
        os.makedirs('exports', exist_ok=True)

        # Write CSV
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['devId', 'timestamp', 'value'])
            writer.writeheader()
            for row in queryset:
                writer.writerow({
                    'devId': row['devId'],
                    'timestamp': row['created_date'],  # renamed field
                    'value': row['value'],
                })

        return Response({"detail": f"CSV file saved as {filename}."}, status=status.HTTP_200_OK)

       

class PostViewset(viewsets.ModelViewSet):
    def get_queryset(self):       
        date_range = self.request.query_params.get('date_range',None)        
        device = self.request.query_params.get('dev', None)
        not_resempled = self.request.query_params.get('not_res', None)
        on_minute = self.request.query_params.get('on_minute', None)        
        today = datetime.today()
        datem = str(datetime(today.year, today.month, 1))
        datem = datem.split(" ")[0]

        if date_range is not None:
            if date_range == 'today':
                if device is not None:
                    queryset = Post.today.filter(devId=device).order_by('created_date')
                else:
                    queryset = Post.today.all().order_by('created_date').values('devId', 'created_date', 'value')                              
                    #queryset = cache.get('cached_data_all_today')                                          
                    if queryset is None:       
                        queryset = Post.today.all().order_by('created_date').values('devId', 'created_date', 'value')   
             
                return queryset
            if date_range == 'year':
                errors = Post.objects.filter(created_date__lte='2023-01-01')
                errors.delete()
                if device is not None:
                    if not_resempled:
                        if on_minute:
                            queryset = Post.objects.filter(devId=device)
                        else:
                            queryset = Post.month.filter(devId=device)
                    else:
                        queryset = Post.year.filter(devId=device)
                else:                    
                    #queryset = Post.month.all()
                    queryset = cache.get('cached_data_all_year')
                return queryset
            if date_range == 'month':
                if device is not None:
                    if not_resempled:
                        queryset = Post.objects.filter(devId=device,created_date__gte=datem)
                    else:
                        queryset = Post.month.filter(devId=device,created__gte=datem)
                else:
                    #queryset = Post.month.filter(created__gte=datem)
                    queryset = cache.get('cached_data_all_month')
                    
                return queryset
    #queryset = Post.objects.all()
    serializer_class = PostSerializer
    #pagination_class = CustomPagination

    # filter_class = PostFilter
    # search_fields = (
    #         '^devId',
    #     )





class PostForecastViewset(viewsets.ModelViewSet):
    def get_queryset(self):
        range = self.request.query_params.get('date_range',None)
        device = self.request.query_params.get('dev', None)
        today = datetime.today()
        datem = str(datetime(today.year, today.month, 1))
        datem = datem.split(" ")[0]
        if range is not None:
            if range == 'today':
                if device is not None:
                    queryset = PostForecast.today.filter(devId=device).order_by('created_date')
                else:
                    queryset = PostForecast.today.all().order_by('created_date')
                return queryset
            if range == 'year':
                if device is not None:
                    queryset = PostForecast.month.filter(devId=device)
                else:
                    queryset = PostForecast.month.all()
                return queryset
            if range == 'month':
                if device is not None:
                    queryset = PostForecast.month.filter(devId=device,created__gte=datem)
                else:
                    queryset = PostForecast.month.filter(created__gte=datem)
                return queryset
    serializer_class = PostForecastSerializer
    
    
    
    
    
class PostForecastMonthViewset(viewsets.ModelViewSet):
    def get_queryset(self):
        range = self.request.query_params.get('date_range',None)
        device = self.request.query_params.get('dev', None)
        today = datetime.today()
        datem = str(datetime(today.year, today.month, 1))
        datem = datem.split(" ")[0]
        if range is not None:
            if range == 'today':
                if device is not None:
                    queryset = PostForecastMonth.today.filter(devId=device).order_by('created_date')
                else:
                    queryset = PostForecastMonth.today.all().order_by('created_date')
                return queryset
            if range == 'year':
                if device is not None:
                    queryset = PostForecastMonth.month.filter(devId=device)
                else:
                    queryset = PostForecastMonth.month.all()
                return queryset
            if range == 'month':
                if device is not None:
                    queryset = PostForecastMonth.month.filter(devId=device,created__gte=datem)
                else:
                    queryset = PostForecastMonth.month.filter(created__gte=datem)
                return queryset
    serializer_class = PostForecastMonthSerializer



class ConsistentDBView(APIView):
    serializer_class = PostConsistencySerializer
    
    def get(self, request):       
        
        date_range = request.query_params.get('date_range', None)       
        
        if date_range == 'today':
            try:
                queryset_today = PostConsistency.today.all().order_by('created_date').values('devId', 'created_date', 'value')                
                if queryset_today is None:
                    return Response({'message': 'Cached data not found'}, status=404)        
                serializer = PostConsistencySerializer(queryset_today, many=True)       
                return Response(serializer.data)  # Serialize the data here
                
            except Exception as e:
                return Response({'error': str(e)}, status=500)
        
        elif date_range == 'month':
            return Response({'message': 'Cached data not found'}, status=404)   
           
        
        elif date_range == 'year':
            return Response({'message': 'Cached data not found'}, status=404)   
    

class MissingDataCalculView(APIView):    
    
    def get(self, request):

        date_range = request.query_params.get('date_range', None)
        dev_id = request.query_params.get('devId', None)
        current_date = datetime.now(timezone('Europe/Sofia'))
        if date_range == 'today':
            if dev_id:
                try:
                    queryset_today = Post.today.filter(devId=dev_id)             
                    if queryset_today is None:
                        return Response({'message': 'Cached data not found'}, status=404)        
                    count_today = queryset_today.count()
                    curr_hour = current_date.hour
                    count_real = curr_hour*60 + current_date.minute
                    percentage = (count_today/count_real)*100
                    percentage = round(percentage, 2)
                    return Response({"missing":percentage})  # Serialize the data here
                    
                except Exception as e:
                    return Response({'error': str(e)}, status=500)
            else:
                return Response({'message': 'Empty DevId'}, status=404)

        
        elif date_range == 'month':
            if dev_id:
                try:
                    today = datetime.now(timezone('Europe/Sofia'))
                    today_date = today.date()
                    beginning_of_month = today_date.replace(day=1)
                    queryset_month = Post.objects.filter(created_date__gte=beginning_of_month, devId=dev_id)
                    if queryset_month:
                        count_month = queryset_month.count()
                        curr_hour = today.hour
                        curr_day = today.day
                        count_month_real = curr_day*24*60 + curr_hour*60 + today.minute
                        percentage_month = (count_month/count_month_real)*100
                        percentage_month = round(percentage_month, 2)
                        return Response({"missing":percentage_month})  # Serialize the data here
                except Exception as e:
                    return Response({'error': str(e)}, status=500) 
        
        elif date_range == 'year':
            if dev_id:
                try:
                    today = datetime.now(timezone('Europe/Sofia'))
                    today_date = today.date()
                    beginning_of_year_for_query = today_date.replace(month=1, day=1)
                    queryset_year = Post.objects.filter(created_date__gte=beginning_of_year_for_query, devId=dev_id)
                    if queryset_year:
                        queryset_year_count = queryset_year.count()
                        beginning_of_year = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                        time_elapsed = today - beginning_of_year
                        # Calculate the total minutes elapsed
                        total_minutes_elapsed = time_elapsed.days * 24 * 60 + time_elapsed.seconds // 60

                        percentage_year = (queryset_year_count/total_minutes_elapsed)*100                       
                        
                        percentage_year = round(percentage_year, 2)
                        return Response({"missing":percentage_year})  # Serialize the data here
                except Exception as e:
                    return Response({'error': str(e)}, status=500) 

            return Response({'message': 'Cached data not found'}, status=404)   
    



class  OnlineView(APIView):
    def get(self, request):
        online = Online.dist.all()

        serializer = OnlineSerializer(online, many=True)
        #print(serializer)
        return Response({"online": serializer.data})


class PriceViewset(viewsets.ModelViewSet):
    queryset = Price.objects.all().order_by('timestamp')
    serializer_class = PriceSerializer
    #filter_class = PriceFilter
    def get_queryset(self):
        queryset = super().get_queryset()
        start_date = self.request.query_params.get('start_date')
        end_date = self.request.query_params.get('end_date')
        if start_date:
            start_date = parse_datetime(start_date)
            queryset = queryset.filter(timestamp__gte=start_date)
        if end_date:
            end_date = parse_datetime(end_date)
            queryset = queryset.filter(timestamp__lte=end_date)
        return queryset



class FlexiViewset(viewsets.ModelViewSet):
    queryset = Flexi.objects.all().order_by('-response_time')
    serializer_class = FlexiSerializer

class SimLogViewset(viewsets.ModelViewSet):    
    queryset = FlexabilitySim.objects.all().order_by('-scheduled')
    serializer_class = FlexiSimSerializer


class ResampleToday15MinView(APIView):
    def get(self, request):
        
        resample_at = request.query_params.get('resample', None)
        dev_id = request.query_params.get('devId', None)
        if resample_at is not None and dev_id is not None:
            if resample_at == '15min':
                cache_key = f'cache_15min_today_{dev_id}'                
                dataset = cache.get(cache_key)                               
                if dataset is None:
                    return Response({"error": "No data in cache"}, status=400) 
                
            elif resample_at == '30min':
                cache_key = f'cache_30min_today_{dev_id}'
                dataset = cache.get(cache_key)                               
                if dataset is None:
                    return Response({"error": "No data in cache"}, status=400) 
            
            elif resample_at == '45min':
                cache_key = f'cache_45min_today_{dev_id}'
                dataset = cache.get(cache_key)                               
                if dataset is None:
                    return Response({"error": "No data in cache"}, status=400) 

                
            elif resample_at == '60min':                
                cache_key = f'cache_60min_today_{dev_id}'                
                dataset = cache.get(cache_key) 
                if dataset is None:
                    return Response({"error": "No data in cache"}, status=400)                      
         
            else:
                return Response({"error": "Invalid resample value. Use '15min' or '60min'."}, status=400)
            
            serializer = Resample15MinSerializer(dataset, many=True) 
            return Response(serializer.data)  



@api_view(['POST',])
def post_data(request):
    my_data = request.data
    publish.single("correction", str(my_data), hostname="159.89.103.242", port=1883)
    return Response({"Success": "ok"})

@api_view(['POST',])
def post_cali(request):
    cali_data = request.data["calibrate"]

    for key in cali_data:
        topic = "cali/"+key
        val = cali_data[key]
        publish.single(topic, str(val), hostname="159.89.103.242", port=1883)
    return Response({"Success": "ok"})

#flexi sim
@api_view(['POST',])
def post_single_correction(request):
    dev = request.data["dev"]
    pow = request.data["power"]
    timer = request.data["timer"]    

    topic = str(dev+"/correction")
    single_data = {
        "power":pow,
        "timer":timer
    }

    publish.single(topic, str(single_data), hostname="159.89.103.242", port=1883)
    return Response({"Success": "ok"})

#flexi sim with calendar
@api_view(['POST',])
def sched_flexi(request):
    
    key='14252)5q?12FGs'
    sched_data = request.data
      
    key_received = sched_data['key']
    device = sched_data['dev']
    date_for_sched = sched_data['date']
    pow = sched_data['pow']
    duration = sched_data['duration']    
    # Put the requested power into new db field into FlexabilitySim
    #requested = Flexi.objects.filter(response_time=date_for_sched,flexiDev=device)
    #requested_pow = requested.res_pow
    if key_received == key:
        if device and date_for_sched and pow and duration:
            FlexabilitySim.objects.create(provided_dev=device,scheduled=date_for_sched,sched_pow=pow,sched_durration=duration)
            return Response({"Success": "ok"})



@api_view(['POST',])
def reset_data(request):
    reset_data = request.data["reset"]
    print(reset_data)
    devId = reset_data['devId']
    topic = str(devId+"/reset")
    payload = reset_data["reset"]
    publish.single(topic, str(payload), hostname="159.89.103.242", port=1883)
    return Response({"Success": "ok"})


@api_view(['POST',])
def flexi_send(request):
    
    key='14252)5q?12FGs'    
    flexi_data = request.data    
    dev = flexi_data.get("dev",None)
    date = flexi_data.get("date",None)
    pow = flexi_data.get('pow',None)
    duration = flexi_data.get('duration',None)
    print(date)
    print(dev)
    # time_shift = 7200
    # date_part = date.split("T")[0]
    # hour_part = date.split("T")[1]
    # hours = hour_part.split(":")[0]
    # mins = hour_part.split(":")[1]
    # sec = ":00Z"
    # time_part = hours+":"+mins+sec
    # date_str = date_part+"T"+time_part
    # t = time.mktime(dt.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").timetuple())
    # t = str(t).split(".")[0]
    # timestamp = int(t) + time_shift   
    # date_to_import = datetime.fromtimestamp(timestamp).isoformat() 
    key_received = flexi_data['key']
    if key_received == key:
        if dev and date and pow and duration:
            Flexi.objects.create(flexiDev = dev, response_time = date, res_pow = pow, res_durr = duration)
            return Response({"Success": "ok"})      
    
    # topic = dev+"/flexi"
    # print(dev,date,pow,duration)
    # if key_received == key:
    #     if dev and date and pow and duration:
    #         payload = {
    #             "pow": pow,
    #             "duration": duration,
    #             "date": timestamp,
    #             "dev": dev
    #         }
    #         print(payload)
    #         publish.single(topic, str(payload), hostname="159.89.103.242", port=1883)
            


@api_view(['POST',])
def login_data(request):
    print("Test!!!!")
    login_data = request.data
    print(login_data)
    username = login_data["login"]['username']
    password = login_data["login"]["password"]
    ip = login_data["login"]["ip"]
    if username == 'admin' and password == 'aA12121212':   
        print(ip)
        UserIp.objects.get_or_create(user_ip=ip)
        return Response({"Success": "ok"})

@api_view(['POST',])
def exec_all(request):
    req_data = request.data["execAll"]
    if(req_data == 'all'):
        task_exec_all.delay()

    return Response({"Success": "ok"})

@api_view(['POST',])
def asign_node(request):
    node_data = request.data    
    dev_id = node_data["dev"]
    node = node_data.get("node", None)
    if node == None:        
        GridAsign.objects.filter(dev=dev_id).delete()
    else:
        dev = GridAsign.objects.filter(dev=dev_id)
        if dev:
            for d in dev:
                d.grid_name = node
                d.save()
        else:
            GridAsign.objects.create(dev=dev_id,grid_name=node)
   
    #print(node_data)   
    return Response({"Success": "ok"})



@api_view(['POST',])
def asign_capa(request):
    node_data = request.data
    dev_id = node_data["dev"]
    capacity = node_data["capacity"]
    dev = CapaAsign.objects.filter(dev=dev_id)
    if dev:
        for d in dev:
            d.capacity = capacity
            d.save()
    else:
        CapaAsign.objects.create(dev=dev_id,capacity=capacity)
   
    #print(node_data)   
    return Response({"Success": "ok"})

@api_view(['POST',])
def forecast_today(request):
    forecast_data = request.data["forecast"]["range"]
    print(forecast_data)
    range = forecast_data["range"]
    dev = forecast_data["dev"]
    topic = str("tensor/"+range)
    payload = dev
    publish.single(topic, payload, hostname="159.89.103.242", port=1883)
    # task_forecast_today.delay(range, dev)
    return Response({"Success": "ok"})

@api_view(['POST',])
def clear_today(request):
    clear_data = request.data["clear"]["range"]
    range = clear_data["range"]
    dev = clear_data["dev"]
    task_clear.delay(range, dev)
    return Response({"Success": "ok"})
