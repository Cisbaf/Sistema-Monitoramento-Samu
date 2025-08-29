from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurações de paralelismo
CHUNK_SIZE = 1000
MAX_WORKERS_CHUNK = 2
MAX_WORKERS_COLECOES = 8
LIMITE_PESADO = 1000

# Lista de agentes prioritários
AGENTES_PRIORITARIOS = [
  "105291648767",
  "105654979795",
  "111401111750",
  "181166524787",
  "174231413720",
  "117786253718",
  "117562517703",
  "118549604747",
  "5594018760",
  "12184347752",
  "8625998739",
  "14797471735",
  "8865006722",
  "5650996717",
  "12846583765",
  "214116302708",
  "203621986758",
  "200694175714",
  "221186020733",
  "213874658724"
]


def copiar_lote(colecao_origem, colecao_destino, skip, limit):
    documentos = list(colecao_origem.find({}).skip(skip).limit(limit))
    if documentos:
        colecao_destino.insert_many(documentos)
    return len(documentos)

def copiar_colecao_pesada(nome_colecao, db_origem, db_destino):
    colecao_origem = db_origem[nome_colecao]
    colecao_destino = db_destino[nome_colecao]
    colecao_destino.drop()

    total_docs = colecao_origem.estimated_document_count()
    total_copiados = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHUNK) as executor:
        tarefas = []
        for skip in range(0, total_docs, CHUNK_SIZE):
            tarefas.append(executor.submit(copiar_lote, colecao_origem, colecao_destino, skip, CHUNK_SIZE))
        for future in as_completed(tarefas):
            total_copiados += future.result()

    print(f"Coleção '{nome_colecao}' copiada com sucesso ({total_copiados} docs).")
    if nome_colecao == AGENTES_PRIORITARIOS[-1]:
        print("ultima coleção prioritaria copiada!")

    return total_copiados

def copiar_database(conexao_origem: MongoClient, conexao_destino: MongoClient, nome_db: str):
    db_origem = conexao_origem[nome_db]
    db_destino = conexao_destino[nome_db]
    quantitade_total = 0

    colecoes = db_origem.list_collection_names()
    posicao_prioridade = {agente: i for i, agente in enumerate(AGENTES_PRIORITARIOS)}

    # Ordena: prioriza agentes da lista
    colecoes_ordenadas = sorted(
        colecoes,
        key=lambda x: posicao_prioridade.get(x, len(AGENTES_PRIORITARIOS))
    )
    colecoes_pesadas = []
    colecoes_leves = []

    # Separar coleções leves e pesadas
    for nome_colecao in colecoes_ordenadas:
        qtd_docs = db_origem[nome_colecao].estimated_document_count()
        if qtd_docs > LIMITE_PESADO:
            colecoes_pesadas.append(nome_colecao)
        else:
            colecoes_leves.append(nome_colecao)

    print("Começando copiar pesadas")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_COLECOES) as executor:
        tarefas = {executor.submit(copiar_colecao_pesada, nome, db_origem, db_destino): nome for nome in colecoes_pesadas}
        for future in as_completed(tarefas):
            quantitade_total += future.result()

    print("Começando copiar leves")

    # Copiar coleções leves sequencialmente
    for nome_colecao in colecoes_leves:
        qtd_docs = db_origem[nome_colecao].estimated_document_count()
        quantitade_total += copiar_lote(db_origem[nome_colecao], db_destino[nome_colecao], 0, qtd_docs)


    print("leves copiadas")

    print(f"Quantidade total de documentos copiados {quantitade_total}")
    print(f"Database '{nome_db}' copiada com sucesso para destino.")

# Exemplo de uso:
# conexao_origem = MongoClient("mongodb://root:7890380@host:27017/")
# conexao_destino = MongoClient("mongodb://root:7890380@localhost:27017/")
# copiar_database(conexao_origem, conexao_destino, "nome_da_db")
