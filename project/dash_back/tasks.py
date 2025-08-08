# from celery.task.schedules import crontab # type: ignore
# from celery.decorators import periodic_task # type: ignore
from celery.utils.log import get_task_logger # type: ignore
from celery import shared_task #type: ignore

from dash_back.utils import timeSet, manage_comm, resample_today_task
    
from dash_back.models import Post
import paho.mqtt.publish as publish
from datetime import datetime,tzinfo,timedelta
from datetime import date
import json
import pytz



logger = get_task_logger(__name__)


    

@shared_task()
def task_setTime():
    timeSet()


@shared_task()
def task_command_run():
    manage_comm()
    logger.info("managmentCommand")


@shared_task()
def resample_today_data(device_id=None, interval='15min'):
    resample_today_task(device_id, interval)
    




      




