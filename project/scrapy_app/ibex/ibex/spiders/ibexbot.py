import scrapy
from scrapy_app.ibex.ibex.items import ScrapItemsItem
from datetime import datetime,tzinfo,timedelta
from datetime import date
import pytz

from pytz import timezone




class IbexbotSpider(scrapy.Spider):
    name = 'ibexbot'
    allowed_domains = ['www.ibex.bg/']
    start_urls = ['https://ibex.bg/']



    def convert(self, str):
        todays_date = date.today() + timedelta(days=1)
        year = todays_date.year
        month = todays_date.month
        day = todays_date.day

        str = str.split(':')
        hour = int(str[0])
        #est = pytz.timezone('Europe/Sofia')
        time_hour = datetime(year, month, day, hour, 0, 0, tzinfo=pytz.utc)
        return time_hour


    def parse(self, response):
        #Extracting the content using css selectors
        price = response.css('#dam-table tr td.column-price_eur::text').extract()
        time = response.css('#dam-table tr td.column-time_part::text').extract()
        date = response.css('#dam-table tr td.column-date_part::text').extract()

        for line in zip(time,price):
            item = ScrapItemsItem()

            item["timestamp"] = IbexbotSpider.convert(self,line[0])
            item["value"] = line[1]

            yield item
