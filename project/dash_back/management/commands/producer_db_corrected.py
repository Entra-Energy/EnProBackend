from django.core.management.base import BaseCommand
from dash_back.models import Post
from django.db.models import F, Case, When, Value
from django.db.models.functions import Round




class Command(BaseCommand):

    help = 'Adjust sm-0001 and sm-0016 history values, because producers'
    def handle(self, *args, **kwargs):

        queryset = Post.objects.filter(devId='sm-0001', value__gt=30)

        # Reverting the 'value' field based on the reverse formula
        queryset.update(
            value= 30 - F('value')
        )
                
        # # Define the queryset
        # queryset = Post.objects.filter(devId='sm-0001')

        # # Update the 'value' field based on your condition
        
        # queryset.update(
        #     value=Case(
        #         When(value__gt=30, then=Round(F('value') - 2 * (F('value') - 30), 2)),
        #         default=F('value'),  # If value <= 30, it remains the same
        #     )
        # )

        
        