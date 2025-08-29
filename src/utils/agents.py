from datetime import datetime, timedelta
from typing import List, Dict
from src.utils.json import remove_keys

def filter_agents(dict_data, hours: int):
    """
    Filters agents based on their status and date_status.

    Rules:
    - If status != "0", the agent is always included.
    - If status == "0", the agent is only included if:
        - date_status is a valid time (HH:MM:SS)
        - and the difference from now is <= 1 hour
    - If date_status looks invalid (e.g., "1909.07:41:24"), the agent is ignored.
    """

    json_data = dict_data.get("system", {}).get("status_agente", [])

    result = []
    
    now = datetime.now()

    for agent in json_data:
        status = agent.get("status")
        date_status = agent.get("date_status")

        # Skip if missing values
        if status is None or date_status is None:
            continue

        # If status is not "0", always include
        if status != "0":
            result.append(agent)
            continue

        try:
            # Ignore invalid date_status formats like "1909.07:41:24"
            if "." in date_status or len(date_status) > 8:
                continue

            status_time = datetime.strptime(date_status, "%H:%M:%S").time()
            status_datetime = datetime.combine(now.date(), status_time)

            # Handle case when time is in the future (crossing midnight)
            if status_datetime > now:
                status_datetime -= timedelta(days=1)

            # Include only if difference <= 1 hour
            if now - status_datetime <= timedelta(hours=hours):
                result.append(agent)

        except ValueError:
            # If parsing fails, skip this agent
            continue

    return result

def subtract_time_from_datetime(time: str, datetime: datetime):
    hours, minutes, seconds = map(int, time.split(':'))
    return datetime - timedelta(hours=hours, minutes=minutes, seconds=seconds)

def remove_fields_not_utilized(datas_json: List[Dict]):
    keys = ['dialer', 'TAB_wait', 'DIALER_CAMPANHA', 'DIALER_CLIENTE', 'DIALER_CLIENTE',
        'DIALER_VALORES', 'DIALER_STATUS', 't_call', 't_t_acd', 'r_agente_time',
        'VIRTUAL_GRP_CALLERID', 'MSG', 'TAB', 'remoteview', 'custom_vars', 'protocol'
    ]
    new_datas = []
    for data_json in datas_json:
        new_datas.append(remove_keys(data_json, keys))
    return new_datas

def normalize_datetime_fields(datas_json: List[Dict], datetime: datetime):
    for data_json in datas_json:

        # Convertendo date status em date_status_dt
        date_status = data_json.get('date_status', None)
        if date_status != '00:00:00' and not '.' in date_status and date_status is not None:
            date_status_dt = subtract_time_from_datetime(date_status, datetime)
            data_json['date_status_dt'] = date_status_dt.replace(microsecond=0).isoformat()

        # convertendo last date in last_status_dt
        last_date = data_json.get('last_date', None)
        if last_date != '00:00:00' and not '.' in last_date and last_date is not None:
            last_date_dt = subtract_time_from_datetime(last_date, datetime)
            data_json['last_date_dt'] = last_date_dt.replace(microsecond=0).isoformat()

    return datas_json
  