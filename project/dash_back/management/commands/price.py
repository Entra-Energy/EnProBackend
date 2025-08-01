from django.core.management.base import BaseCommand
import json
from dash_back.models import Price #type: ignore
from django.conf import settings
from datetime import datetime,tzinfo,timedelta
from datetime import date
import pytz
from pytz import timezone
import os
class Command(BaseCommand):
    help = 'Prices from json to db'

    def handle(self, *args, **kwargs):
        # print(settings.BASE_DIR)
        def convert(self, str):
            todays_date = date.today()
            year = todays_date.year
            month = todays_date.month
            day = todays_date.day

            str = str.split(':')
            hour = int(str[0])
            #est = pytz.timezone('Europe/Sofia')
            time_hour = datetime(year, month, day, hour, 0, 0, tzinfo=pytz.utc)
            return time_hour

        test = os.path.join(settings.BASE_DIR, 'ibex.json')
        print(test)

        #priceJson = [json.loads(line) for line in open(test,'r')]
        with open(test, 'r') as f:
            #d_old_str = f.read().replace('\n', '') # remove all \n
            my_json_obj = json.load(f)


        for data in my_json_obj:
            time = convert(self, data["time"])
            price = float(data["price"])
            print(time)
            Price.objects.get_or_create(timestamp=time, value = price)
