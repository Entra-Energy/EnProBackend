from django.core.management.base import BaseCommand
from scrapy_app.ibex.ibex.spiders import ibexbot
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os
from django.conf import settings
import sys

class Command(BaseCommand):
    help = 'Release the spiders'

    def handle(self, *args, **kwargs):
        #settings = get_project_settings()
        #file = 'items.json'  
        
        # path = os.path.join(base, file)
        # if path:  
        #     os.remove(path)
        if "twisted.internet.reactor" in sys.modules:
            del sys.modules["twisted.internet.reactor"]
        print("RUNNING!")    
        process = CrawlerProcess(settings={
                                "FEEDS": {
                                "items.json": {"format": "json"},
                                },
                                "ITEM_PIPELINES" : {"scrapy_app.ibex.ibex.pipelines.IbexPipeline": 300},
                                "FEED_EXPORT_ENCODING" : 'utf-8',
                                "ROBOTSTXT_OBEY" : False,
                                })
        process.crawl(ibexbot.IbexbotSpider)
        process.start()
