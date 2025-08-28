from datetime import datetime
from pydantic import BaseModel
from datetime import timedelta

class DateRange(BaseModel):
    start: str
    end: str

    def get_date_range(self) -> list[str]:
        """Retorna um array com todas as datas no intervalo (formato DD/MM/YYYY)"""
        date_format = "%d/%m/%Y"
        
        start = datetime.strptime(self.start, date_format)
        end = datetime.strptime(self.end, date_format)
        date_list = []
        current_date = start
        
        while current_date <= end:
            date_list.append(current_date.strftime(date_format))
            current_date += timedelta(days=1)
        return date_list

def get_collection(name: str, client):
    collection = client[name]
    return collection


def execute_query_filter_date(id_callroute: str, date: DateRange):
    collection = get_collection(id_callroute)
    date_range = date.get_date_range()
    results = []
    for date in date_range:
        query = {
            "date_status_dt": {
                "$regex": f"^{date}"
            }
        }
        results.extend(list(collection.find(query)))
    return results