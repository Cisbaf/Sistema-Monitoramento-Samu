from datetime import datetime, timedelta

def filter_agents(json_data):
    """
    Filters agents based on their status and date_status.

    Rules:
    - If status != "0", the agent is always included.
    - If status == "0", the agent is only included if:
        - date_status is a valid time (HH:MM:SS)
        - and the difference from now is <= 1 hour
    - If date_status looks invalid (e.g., "1909.07:41:24"), the agent is ignored.
    """
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
            if now - status_datetime <= timedelta(hours=12):
                result.append(agent)

        except ValueError:
            # If parsing fails, skip this agent
            continue

    return result
