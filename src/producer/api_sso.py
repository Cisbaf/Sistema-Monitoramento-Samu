import requests

def queryApiData(url: str, timeout_conn: float, timeout_read: float):
    response = requests.get(url=url, timeout=(timeout_conn, timeout_read))
    response.raise_for_status()
    return response