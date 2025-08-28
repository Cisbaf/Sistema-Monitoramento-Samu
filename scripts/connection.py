
from pymongo import MongoClient
from pydantic import BaseModel

class ClientData(BaseModel):
    username: str
    password: str
    host: str
    port: int
    auth_db: str

def connection(data: ClientData):
    return MongoClient(
        f"mongodb://{data.username}:{data.password}@{data.host}:{data.port}/?authSource={data.auth_db}",
        maxPoolSize=50,  # padrão 100, mas ajuste conforme threads
        wtimeoutMS=2500
    )
