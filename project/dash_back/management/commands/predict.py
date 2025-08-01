from django.core.management.base import BaseCommand
from dash_back.models import Post, Online #type: ignore
from django.db import models
from datetime import datetime, timedelta, time
from django.conf import settings
from django.utils import timezone
from django.utils.timezone import activate
from pytz import timezone
import pytz


class Command(BaseCommand):

    help = 'update db with predicted values'

    def handle(self, *args, **kwargs):


        def predicted_values(i):
            today = datetime.now(timezone('Europe/Sofia')).date()
            now = datetime.now(timezone('Europe/Sofia'))
            now = str(now)
            cur_hour = now.split(" ")[1].split(":")[0]
            current_hour = int(cur_hour)+i
            #print(current_hour)
            timestamp= str(today)+'T'+str(current_hour)+':00:00Z'
            Post.objects.create(devId='sm-0009F',created_date=timestamp,value = 3000)


        now = datetime.now(timezone('Europe/Sofia'))
        now = str(now)
        cur_hour = now.split(" ")[1].split(":")[0]
        current_hour = int(cur_hour)
        r = 24 - current_hour
        for i in range(r):
            predicted_values(i)


            #Post.objects.create(devId='sm-0009',created_date=)
