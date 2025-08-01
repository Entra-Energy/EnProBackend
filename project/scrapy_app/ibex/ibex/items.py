import scrapy
from scrapy_djangoitem import DjangoItem
from dash_back.models import Price


class ScrapItemsItem(DjangoItem):
    # define the fields for your item here like:
    # name = scrapy.Field()
    django_model = Price
