# from django.urls import path,re_path
# from dash_back.views import PostView,OnlineView
#from dash_back import views
from rest_framework.routers import DefaultRouter
from dash_back import views
from django.urls import path
from dash_back.views import PostResampleView


app_name = "dash_back"
router = DefaultRouter()
#
# router.register(r'posts', views.PostViewset, basename='posts')
router.register(r'price', views.PriceViewset, basename='price')
router.register(r'post_forecast', views.PostForecastViewset, basename='postForecast')
router.register(r'month_post_forecast', views.PostForecastMonthViewset, basename='postForecastMonth')
router.register(r'grid_asign', views.GridViewset, basename='gridset')
router.register(r'capa_asign', views.CapaViewset, basename='capaasign')





urlpatterns = [
    path("posts/", PostResampleView.as_view(), name="post-resample"),
    path('online/', views.OnlineView.as_view(), name = 'test'),
    path('asign/',views.asign_node, name='asign'),
    path('capa/',views.asign_capa, name='capa'),
    path('forecast_today/',views.forecast_today, name='forecast_today'),
    #aggregate min, max, avg
    path('aggregate/', views.MinMaxAvg.as_view(), name='today_avg_value'),   
    
]

urlpatterns += router.urls
