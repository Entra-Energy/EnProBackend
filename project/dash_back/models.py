from django.db import models
from datetime import datetime, timedelta, time
from django.conf import settings
from django.utils import timezone
from pytz import timezone
import pytz
from django.db.models import Avg, Sum, Min, Max
from django.db.models.functions import TruncHour, TruncMinute, TruncDay
from django.utils.dateparse import parse_datetime
from django.utils.timezone import make_aware, is_naive

class UniqueOnlineManager(models.Manager):
    def get_queryset(self):
        now = datetime.now(timezone('Europe/Sofia'))
        ten_minutes_ago = now - timedelta(minutes=5)
        ten_minutes_ago = str(ten_minutes_ago)
        currDate = ten_minutes_ago.split(" ")[0]+"T"
        cur_hour = ten_minutes_ago.split(" ")[1].split(":")[0]
        cur_hour_min = ten_minutes_ago.split(" ")[1].split(":")[1]
        query_date = currDate+cur_hour+":"+cur_hour_min+":00Z"

        last_active = super().get_queryset().filter(saved_date__gte = query_date)
        unique = {}
        for elem in reversed(last_active):
            if elem.dev not in unique.keys():                
                unique[elem.dev] = {'dev':elem.dev,'ready':elem.ready,'pow':elem.pow, 'providing':elem.providing, 'dev_name':elem.dev_name, 'lat':elem.lat, 'long':elem.long}        
        return unique.values()
        



class TodayPostManager(models.Manager):
    def get_queryset(self):
        today = datetime.now(timezone('Europe/Sofia')).date()
        tomorrow = today + timedelta(1)
        today_start = str(today)+'T'+'00:00:00Z'
        today_end = str(tomorrow)+'T'+'00:00:00Z'
        return super().get_queryset().filter(created_date__gt = today_start, created_date__lt = today_end)
        
    
    
class MonthPostManager(models.Manager):
    def get_queryset(self):
        return (
            super().get_queryset()
            .annotate(created=TruncHour('created_date'))
            .values('devId', 'created')  # group by devId and created hour
            .annotate(
                value=Avg('value'),
                grid=Avg('grid'),
                actualCorr=Avg('actualCorr'),
                actualProviding=Avg('actualProviding'),
                providingAmount=Avg('providingAmount')
            )
            .order_by('created')
        )
class YearPostManager(models.Manager):
    def get_queryset(self):
        today = datetime.today()
        datem = str(datetime(today.year, 1, 1))
        datem = datem.split(" ")[0]
        dataset = super().get_queryset().filter(created_date__gte=datem).annotate(
            created=TruncDay('created_date')
        ).values('devId', 'created').annotate(
            value=Avg('value'),
            grid=Avg('grid'),
            actualCorr=Avg('actualCorr'),
            actualProviding=Avg('actualProviding'),
            providingAmount=Avg('providingAmount')
        ).order_by('created')
        return dataset
    
class MonthPostConsistencyManager(models.Manager):
    def get_queryset(self):
        dataset = super().get_queryset().annotate(created=TruncHour('created_date')).values('created').annotate(value=Avg('value')).values('devId','created','value').order_by('created')
        return dataset



class YearPostConsistencyManager(models.Manager):
    def get_queryset(self):
        dataset = super().get_queryset().annotate(
            created=TruncDay('created_date')
        ).values('devId', 'created').annotate(
            value=Avg('value'),            
        ).order_by('created')
        return dataset


class MonthPostForecastManager(models.Manager):
    def get_queryset(self):
        dataset = super().get_queryset().annotate(created=TruncHour('created_date')).values('created').annotate(value=Avg('value')).values('devId','created','value').order_by('created')
        return dataset


class TodayMinMaxAvgStackedValueManager(models.Manager):
    def get_queryset(self):
        today = datetime.now(timezone('Europe/Sofia')).date()
        tomorrow = today + timedelta(1)
        today_start = datetime.combine(today, datetime.min.time())
        today_end = datetime.combine(tomorrow, datetime.min.time())

        # Group by created_date and sum the 'value' for each group
        grouped_values = super().get_queryset().filter(
            created_date__gte=today_start,
            created_date__lt=today_end
        ).values('created_date').annotate(total_value=Sum('value'))

        grouped_values = list(grouped_values)[:-1]  # Exclude the last element from the list
        moment_power = grouped_values[-1]
        # Calculate the minimum stacked value from the grouped values
        min_stacked_value = min(grouped_values, key=lambda x: x['total_value'])['total_value'] if grouped_values else None
        max_stacked_value = max(grouped_values, key=lambda x: x['total_value'])['total_value'] if grouped_values else None
        avg_stacked_value = sum(grouped_value['total_value'] for grouped_value in grouped_values) / len(grouped_values) if grouped_values else None
        return {
                "min":round(min_stacked_value, 2),
                "max":round(max_stacked_value,2),
                "avg":round(avg_stacked_value,2),
                "mom":moment_power
                }
class MonthMinMaxAvgStackedValueManager(models.Manager):
    def get_queryset(self):
        today = datetime.today()
        tomorrow = today + timedelta(1)
        today_start = datetime.combine(today, datetime.min.time())
        today_end = datetime.combine(tomorrow, datetime.min.time())
        datem = str(datetime(today.year, today.month, 1))
        datem = datem.split(" ")[0]
        # Group by today just to get the moment power to be equal with the today
        grouped_values_today = super().get_queryset().filter(
            created_date__gte=today_start,
            created_date__lt=today_end
        ).values('created_date').annotate(total_value=Sum('value'))
        grouped_values_today = list(grouped_values_today)[:-1]
        moment_power = grouped_values_today[-1]

        # Group by created_date and sum the 'value' for each group
        grouped_values = super().get_queryset().filter(
            created_date__gte=datem,            
        ).values('created_date').annotate(total_value=Sum('value'))
        grouped_values = list(grouped_values)[:-1]  # Exclude the last element from the list
        
        min_stacked_value = min(grouped_values, key=lambda x: x['total_value'])['total_value'] if grouped_values else None
        max_stacked_value = max(grouped_values, key=lambda x: x['total_value'])['total_value'] if grouped_values else None
        avg_stacked_value = sum(grouped_value['total_value'] for grouped_value in grouped_values) / len(grouped_values) if grouped_values else None
        return {
                "min":round(min_stacked_value, 2),
                "max":round(max_stacked_value,2),
                "avg":round(avg_stacked_value,2),
                "mom":moment_power
                }

class YearMinMaxAvgStackedValueManager(models.Manager):
    def get_queryset(self):
        today = datetime.today()
        tomorrow = today + timedelta(1)
        today_start = datetime.combine(today, datetime.min.time())
        today_end = datetime.combine(tomorrow, datetime.min.time())
        datem = str(datetime(today.year, 1, 1))
        datem = datem.split(" ")[0]
        # Group by today just to get the moment power to be equal with the today
        grouped_values_today = super().get_queryset().filter(
            created_date__gte=today_start,
            created_date__lt=today_end
        ).values('created_date').annotate(total_value=Sum('value'))
        grouped_values_today = list(grouped_values_today)[:-1]
        moment_power = grouped_values_today[-1]      
        
        grouped_values = super().get_queryset().filter(
            created_date__gte=datem,            
        ).values('created_date').annotate(total_value=Sum('value'))
        grouped_values = list(grouped_values)[:-1]  # Exclude the last element from the list
        grouped_values = grouped_values[1:]
        moment_power = grouped_values[-1]
        min_stacked_value = min(grouped_values, key=lambda x: x['total_value'])['total_value'] if grouped_values else None
        max_stacked_value = max(grouped_values, key=lambda x: x['total_value'])['total_value'] if grouped_values else None
        avg_stacked_value = sum(grouped_value['total_value'] for grouped_value in grouped_values) / len(grouped_values) if grouped_values else None
        return {
                "min":round(min_stacked_value, 2),
                "max":round(max_stacked_value,2),
                "avg":round(avg_stacked_value,2),
                "mom":moment_power
                }
    

   

class Post(models.Model):
    devId = models.CharField(max_length=200)
    created_date = models.DateTimeField(default=datetime.now())
    value = models.FloatField()
    objects = models.Manager()
    grid = models.IntegerField(default=0, null=True)
    today = TodayPostManager()
    month = MonthPostManager()
    year = YearPostManager()
    costH = models.FloatField(default=0, null=True)
    costD = models.FloatField(default=0, null=True)
    costM = models.FloatField(default=0, null=True)
    budgetH = models.IntegerField(default=0, null=True)
    budgetD = models.IntegerField(default=0, null=True)
    budgetM = models.IntegerField(default=0, null=True)
    actualCorr = models.FloatField(default=0, null=True)
    actualProviding = models.IntegerField(default=0, null=True)
    providingAmount = models.FloatField(default=0, null=True)
    todayMinMaxAvg = TodayMinMaxAvgStackedValueManager()
    monthMinMaxAvg = MonthMinMaxAvgStackedValueManager()
    yearMinMaxAvg = YearMinMaxAvgStackedValueManager()
    cost = models.FloatField(default=0, null=True)

    def save(self, *args, **kwargs):
        if isinstance(self.created_date, str):
            self.created_date = parse_datetime(self.created_date)

        if self.created_date:
            # Make timezone-aware if naive
            if is_naive(self.created_date):
                self.created_date = make_aware(self.created_date)

            ts_hour = self.created_date.replace(minute=0, second=0, microsecond=0)
            price_obj = Price.objects.filter(timestamp=ts_hour).first()

            if price_obj:
                self.cost = (self.value / 1000) * (price_obj.value / 1000)
                self.cost = round(self.cost, 2)

        super().save(*args, **kwargs)
    
class PostConsistency(models.Model):    
    devId = models.CharField(max_length=200)
    created_date = models.DateTimeField(default=datetime.now())
    value = models.FloatField()
    objects = models.Manager()
    today = TodayPostManager()
    month = MonthPostConsistencyManager()
    year = YearPostConsistencyManager()
    
    def __str__(self):
        return self.devId

    
class PostForecast(models.Model):
    devId = models.CharField(max_length=200)
    created_date = models.DateTimeField(default=datetime.now())
    value = models.FloatField()
    today = TodayPostManager()
    objects = models.Manager()
    month = MonthPostForecastManager()
    model_loss = models.FloatField(default=0, null=True)
    mean_abs_error = models.FloatField(default=0, null=True)
    
    
    
class PostForecastMonth(models.Model):
    devId = models.CharField(max_length=200)
    created_date = models.DateTimeField(default=datetime.now())
    value = models.FloatField()
    today = TodayPostManager()
    objects = models.Manager()
    month = MonthPostForecastManager()
    model_loss = models.FloatField(default=0, null=True)
    mean_abs_error = models.FloatField(default=0, null=True)


class Online(models.Model):
    dev = models.CharField(max_length=300)
    saved_date = models.DateTimeField(default=datetime.now())
    pow = models.FloatField()
    ready = models.IntegerField(default=0, null=True)
    signal = models.IntegerField(default=0, null=True)
    providing = models.IntegerField(default=0, null=True)
    objects = models.Manager()
    dist = UniqueOnlineManager()
    dev_name = models.CharField(default='lab', max_length=300)
    lat = models.FloatField(default=0, null=True)
    long = models.FloatField(default=0, null=True)

class Price(models.Model):
    timestamp = models.DateTimeField(default=datetime.now())
    value = models.FloatField()


class Flexi(models.Model):
    flexiDev = models.CharField(max_length=300)
    response_time = models.DateTimeField(default=datetime.now())
    res_pow = models.FloatField()
    res_durr = models.IntegerField()

class FlexabilitySim(models.Model):
    provided_dev = models.CharField(max_length=300)
    scheduled = models.DateTimeField(default=datetime.now())
    sched_pow = models.FloatField()
    sched_durration = models.IntegerField()
    #requested_power = models.FloatField()

class Aris(models.Model):
    power_aris = models.FloatField()
    timestamp_aris = models.DateTimeField(default=datetime.now())
    wind_aris = models.FloatField()
    objects = models.Manager()


class Neykovo(models.Model):
    power_neykovo = models.FloatField()
    timestamp_neykovo = models.DateTimeField(default=datetime.now())
    wind_neykovo = models.FloatField()

class UserIp(models.Model):
    user_ip = models.CharField(max_length=900)
    
class Hydro(models.Model):
    timestamp_hydro = models.DateTimeField(default=datetime.now())
    hydro_pow = models.FloatField()
    guide_vains = models.FloatField()
    level = models.FloatField()
    gen_temp = models.FloatField()
    gen_curr = models.FloatField()
    gen_vol = models.FloatField()
    
class GridAsign(models.Model):
    dev = models.CharField(max_length=100)
    grid_name = models.CharField(max_length=300)
    
class CapaAsign(models.Model):
    dev = models.CharField(max_length=100)
    capacity = models.FloatField()
    