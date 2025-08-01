# from celery.task.schedules import crontab # type: ignore
# from celery.decorators import periodic_task # type: ignore
from celery.utils.log import get_task_logger # type: ignore
from celery import shared_task #type: ignore

from dash_back.utils import scheduled_flexi, exec_all, get_hydro, timeSet, manage_comm, price_csv, fetch_simavi, clear_forecast_data,\
    make_auto_forecast, slow_month_all, slow_year_all, slow_today_all, db_consistency, fill_prices, today_resample_15min, today_resample_60min, today_resample_30min, today_resample_45min
    
from dash_back.models import Post
import paho.mqtt.publish as publish
from datetime import datetime,tzinfo,timedelta
from datetime import date
import json
import pytz



logger = get_task_logger(__name__)



@shared_task()
def task_schedule():
    """
    scheduleFlexi!!!
    """
    scheduled_flexi()
    logger.info("FLEXI")

@shared_task
def task_fill_prices_dam():
    fill_prices()


@shared_task()
def task_exec_all():
    """
    Execute all requests
    """
    exec_all()
    #logger.info("execute all")

@shared_task()
def task_hydro():
    get_hydro()
    #logger.info("Hydro")
    

@shared_task()
def task_setTime():
    timeSet()
    #logger.info("TimeSet")
    
    
    
    #logger.info("MQTT ERROR")

# @shared_task()
# def task_update_db():
#     update_db_coeff()
#     logger.info("COEFF")

@shared_task()
def task_command_run():
    manage_comm()
    logger.info("managmentCommand")

@shared_task()
def task_price_csv():
    price_csv()
    logger.info("priceCSV")

@shared_task()
def task_simavi():
    fetch_simavi()
    logger.info("SIMAVI")
    
# @shared_task()
# def task_forecast_day():
#     forecast_day()
#     logger.info("ForecastDay")
    
# @shared_task()
# def task_forecast_today(range, dev):
#     forecast_today_calc(range, dev)
    
    
@shared_task()
def task_clear(range, dev):
    clear_forecast_data(range, dev)
    
@shared_task()
def task_auto_forecast():
    make_auto_forecast()
    logger.info("TensorFlowForecast")


@shared_task
def slow_query_all_devs_month_task():
    slow_month_all()
    logger.info("AllDevsMonthSlowQuery")
    
@shared_task
def slow_query_all_devs_year_task():
    slow_year_all()
    logger.info("AllDevsYearSlowQuery")
    
@shared_task
def slow_query_all_devs_today_task():
    print("TASK TRIGGS")
    today_data = slow_today_all() 
    # print(f"into the task:{today_data[:3]}")   
    logger.info("AllDevsTodaySlowQuery")
    return today_data
      
@shared_task
def populate_missing_task():
    db_consistency()
    logger.info("Populate")

@shared_task
def resample_at_15_min_task():
    today_resample_15min()
    logger.info("Resample at 15 min")

@shared_task
def resample_at_30_min_task():
    today_resample_30min()
    logger.info("Resample at 30 min")

@shared_task
def resample_at_45_min_task():
    today_resample_45min()
    logger.info("Resample at 45 min")

@shared_task
def resample_at_60_min_task():
    today_resample_60min()
    logger.info("Resample at 60 min")



