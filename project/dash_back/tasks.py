# from celery.task.schedules import crontab # type: ignore
# from celery.decorators import periodic_task # type: ignore
from celery.utils.log import get_task_logger # type: ignore
from celery import shared_task #type: ignore

from dash_back.utils import timeSet, manage_comm, resample_range_task
    
from dash_back.models import Post
import paho.mqtt.publish as publish
from datetime import datetime,tzinfo,timedelta
from datetime import date
import json
import pytz
from typing import Optional



logger = get_task_logger(__name__)

LOCK_TTL = 60
def _lock(key): return cache.add(f"lock:{key}", 1, LOCK_TTL)
def _unlock(key): cache.delete(f"lock:{key}")
    

@shared_task()
def task_setTime():
    timeSet()


@shared_task()
def task_command_run():
    manage_comm()
    logger.info("managmentCommand")


# @shared_task()
# def resample_range_data(date_range: str, device_id: Optional[str] = None, interval: str = "15min"):
#     return resample_range_task(date_range, device_id, interval)
    
@shared_task()
def resample_range_data(date_range: str, interval: str):
    norm = _normalize_resample_format(interval)
    # single-flight lock for the aggregate key
    suffix = cache_version_for_today(norm) if date_range == "today" else ""
    agg_key = f"resampled_{date_range}:all:{norm}:{suffix}"
    if not _lock(agg_key):
        return "busy"
    try:
        # This call computes once and writes:
        # - aggregate key
        # - per-device keys
        resample_range_task(date_range=date_range, device_id=None, interval=norm)
        return "ok"
    finally:
        _unlock(agg_key)



      




