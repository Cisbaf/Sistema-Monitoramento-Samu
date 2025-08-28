from pymongo import MongoClient
from datetime import datetime

from convert import converter_datas
from connection import connection, ClientData
from copy_db import copiar_database

# Marca o tempo inicial total
inicio_total = datetime.now()

banco_nuvem = connection(ClientData(
    username="root",
    password="7890380",
    host="45.231.133.116",
    port=27017,
    auth_db="admin"
))

banco_local = connection(ClientData(
    username="root",
    password="7890380",
    host="192.168.1.10",
    port=27018,
    auth_db="admin"
))

# Mede o tempo da cópia do banco
inicio_copia = datetime.now()
copiar_database(banco_nuvem, banco_local, nome_db="status_agent")
fim_copia = datetime.now()
tempo_copia = fim_copia - inicio_copia
print(f"Tempo de copiar_database: {tempo_copia} (ou {tempo_copia.total_seconds()}s)")

# # Mede o tempo da conversão de datas
# inicio_converter = datetime.now()
# converter_datas(banco_local)
# fim_converter = datetime.now()
# tempo_converter = fim_converter - inicio_converter
# print(f"Tempo de converter_datas: {tempo_converter} (ou {tempo_converter.total_seconds()}s)")

# # Marca o tempo final total
# fim_total = datetime.now()
# tempo_total = fim_total - inicio_total
# print(f"Tempo total de execução: {tempo_total} (ou {tempo_total.total_seconds()}s)")
