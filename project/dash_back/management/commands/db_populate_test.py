from django.core.management.base import BaseCommand
from dash_back.models import Post
from datetime import datetime, timedelta
from dash_back.utils import db_consistency



class Command(BaseCommand):

    help = 'testing populate missing db values'
    def handle(self, *args, **kwargs):
        test = db_consistency()
        



       
