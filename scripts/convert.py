from datetime import datetime

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient, UpdateOne


def converter_datas(client):
    date_format = "%d/%m/%Y, %H:%M:%S"
    campos_para_converter = ["date_status_dt", "last_date_dt"]
    ignored_dbs = {"admin", "local", "config"}

    def process_collection(db_name, collection_name):
        db = client[db_name]
        collection = db[collection_name]

        bulk_ops = []

        for campo in campos_para_converter:
            filtro = {campo: {"$type": "string"}}
            for doc in collection.find(filtro):
                valor = doc.get(campo)
                try:
                    data_convertida = datetime.strptime(valor, date_format)
                    bulk_ops.append(
                        UpdateOne({"_id": doc["_id"]}, {"$set": {campo: data_convertida}})
                    )
                except Exception as e:
                    print(f"⚠️ Erro {db_name}/{collection_name} campo '{campo}' _id={doc['_id']}: {e}")

        if bulk_ops:
            result = collection.bulk_write(bulk_ops)
            print(f"✅ Atualizados {result.modified_count} documentos em {db_name}/{collection_name}")

    # Criar lista de tarefas (coleções)
    tasks = []
    for db_name in client.list_database_names():
        if db_name in ignored_dbs:
            continue
        db = client[db_name]
        for collection_name in db.list_collection_names():
            tasks.append((db_name, collection_name))

    # Executar coleções em paralelo
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(lambda args: process_collection(*args), tasks)
