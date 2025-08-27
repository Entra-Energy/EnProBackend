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
def resample_range_data(date_range: str, device_id: Optional[str] = None, interval: str = "15min"):
    """
    Warm the cache for a specific device if device_id is provided,
    otherwise warm the aggregate ("all devices") and (if your helper supports it)
    also fan out per-device keys.
    """
    norm = _normalize_resample_format(interval)

    # choose the target cache key to lock against
    suffix = cache_version_for_today(norm) if date_range == "today" else ""
    target_key = f"resampled_{date_range}:{(device_id or 'all')}:{norm}:{suffix}"

    if not _lock(target_key):
        return "busy"
    try:
        # If you want to compute once for all devices and write many cache entries,
        # call with device_id=None. If you only want to warm one device, pass device_id.
        #
        # Assuming your helper `resample_range_task`:
        # - when device_id=None -> computes ALL devices and writes:
        #       resampled_{date_range}:all:{norm}:{suffix}
        #   and (optionally) per-device keys.
        # - when device_id is not None -> computes just that device and writes only its key.
        resample_range_task(
            date_range=date_range,
            device_id=device_id,   # None => all devices; str => single device
            interval=norm
        )
        return "ok"
    finally:
        _unlock(target_key)



      




