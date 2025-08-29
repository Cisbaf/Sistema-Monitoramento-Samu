from datetime import datetime
from zoneinfo import ZoneInfo



def current_time_sp():
    utc_now = datetime.now(tz=ZoneInfo("UTC"))
    date_time = utc_now.astimezone(ZoneInfo("America/Sao_Paulo"))
    return date_time

