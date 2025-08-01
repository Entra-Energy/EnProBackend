from django.core.management.base import BaseCommand
from dash_back.models import Post
from datetime import datetime, timedelta
from django.db.models import Min, Subquery




class Command(BaseCommand):

    help = 'testing populate missing db values'
    def handle(self, *args, **kwargs):
                
        # Filter queryset to include objects with devId equal to 'sm-0004'
        queryset = Post.today.filter(devId='sm-0004')

        # Get the oldest entry for each created_date
        queryset_dist = queryset.values_list('created_date', flat=True).distinct()

        for data in queryset_dist:
            Post.today.filter(pk__in=Post.today.filter(created_date=data).values_list('id', flat=True)[1:]).delete()


        



       
