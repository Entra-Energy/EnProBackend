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
from dash_back.tasks import resample_range_data
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



class userIpViewset(viewsets.ModelViewSet):    
    queryset = UserIp.objects.all()
    serializer_class = UserIpSerializer
    
    
class GridViewset(viewsets.ModelViewSet):    
    queryset = GridAsign.objects.all()
    serializer_class = GridAsignSerializer
    
    
class CapaViewset(viewsets.ModelViewSet):    
    queryset = CapaAsign.objects.all()
    serializer_class = CapaAsignSerializer



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

     

class PostViewset(viewsets.ModelViewSet):
    def get_queryset(self):       
        date_range = self.request.query_params.get('date_range',None)        
        device = self.request.query_params.get('dev', None)
        not_resempled = self.request.query_params.get('not_res', None)
        on_minute = self.request.query_params.get('on_minute', None)  
        resample = self.request.query_params.get('resample', None)      
        today = datetime.today()
        datem = str(datetime(today.year, today.month, 1))
        datem = datem.split(" ")[0]

        if date_range is not None:
            
            if date_range == 'today' and resample:

                cache_key = f"resampled_today:{device or 'all'}:{resample}"
                cached = cache.get(cache_key)

                if cached:
                    return cached
                else:
                    resample_today_data.delay(device_id=device, interval=resample)
                    return []  # Or HTTP 202 Accepted

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
    serializer_class = PostSerializer


class PostResampleView(APIView):
    def get(self, request, *args, **kwargs):
        device = request.query_params.get("dev")
        resample = request.query_params.get("resample")
        date_range = request.query_params.get("date_range")

        if date_range in {"today", "month", "year"} and resample:
            cache_key = f"resampled_{date_range}:{device or 'all'}:{resample}"
            cached = cache.get(cache_key)
            if cached is not None:
                return Response(cached, status=status.HTTP_200_OK)

            resample_range_data.delay(date_range=date_range, device_id=device, interval=resample)
            return Response({"detail": "Resampling in progress, try again shortly."}, status=status.HTTP_202_ACCEPTED)

        return Response({"error": "Missing or invalid parameters."}, status=400)



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


