from datetime import datetime


def converter_datas(client):
    # Novo formato da string de data e hora
    date_format = "%d/%m/%Y, %H:%M:%S"

    # Campos que você deseja converter
    campos_para_converter = ["date_status_dt", "last_date_dt"]

    # Bancos internos que devem ser ignorados
    ignored_dbs = {"admin", "local", "config"}

    # Iterar por todos os bancos
    for db_name in client.list_database_names():
        if db_name in ignored_dbs:
            continue
        
        print(db_name)
        db = client[db_name]
        print(f"\n📁 Banco: {db_name}")

        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            print(f"  📂 Coleção: {collection_name}")

            for campo in campos_para_converter:
                # Filtrar documentos que têm o campo como string
                filtro = {campo: {"$type": "string"}}

                for doc in collection.find(filtro):
                    valor = doc.get(campo)
                    try:
                        data_convertida = datetime.strptime(valor, date_format)

                        # Atualiza o campo com datetime
                        collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {campo: data_convertida}}
                        )
                    except Exception as e:
                        print(f"    ⚠️ Erro no campo '{campo}' (_id={doc['_id']}): {e}")