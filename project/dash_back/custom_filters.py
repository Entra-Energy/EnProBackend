from django_filters import FilterSet, AllValuesFilter,IsoDateTimeFilter,NumberFilter,DateRangeFilter,DateFilter,CharFilter,DateTimeFilter

from dash_back.models import Post, Price, Aris


# class PostFilter(FilterSet):

#     start_date = IsoDateTimeFilter(field_name='created_date',lookup_expr=('gte'),)
#     end_date = IsoDateTimeFilter(field_name='created_date',lookup_expr=('lte'))
#     date_range = DateRangeFilter(field_name='created_date')
#     dev = AllValuesFilter(field_name='devId')
#     class Meta:
#         model = Post
#         fields = ('start_date','end_date','date_range','dev')

class PriceFilter(FilterSet):
    start_date = IsoDateTimeFilter(field_name='timestamp',lookup_expr=('gte'),)
    end_date = IsoDateTimeFilter(field_name='timestamp',lookup_expr=('lte'))
    date_range = DateRangeFilter(field_name='timestamp')
    class Meta:
        model = Price
        fields = ['timestamp']

class ArisFilter(FilterSet):
    start_date = IsoDateTimeFilter(field_name='timestamp_aris',lookup_expr=('gte'),)
    end_date = IsoDateTimeFilter(field_name='timestamp_aris',lookup_expr=('lte'))
    date_range = DateRangeFilter(field_name='timestamp_aris')
    class Meta:
        model = Aris
        fields = ['timestamp_aris']
