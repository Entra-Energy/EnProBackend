import pytz
from datetime import datetime


pst = pytz.timezone("Europe/Sofia")

now = pst.localize(datetime.now())
print(now)