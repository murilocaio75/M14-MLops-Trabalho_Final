# Trabalho Final - Etapa 1: ingestão do CSV DataCo para o Postgres

import pandas as pd

from dataco import db
from dataco.config import CAMINHO_CSV, COLUNAS_CSV, ENCODING_CSV, FORMATO_DATA


# Lê só as colunas necessárias do CSV original e padroniza nomes e tipos
def le_csv_dataco(caminho=CAMINHO_CSV):
    df = pd.read_csv(caminho, encoding=ENCODING_CSV, usecols=list(COLUNAS_CSV))
    df = df.rename(columns=COLUNAS_CSV)
    df["data_pedido"] = pd.to_datetime(df["data_pedido"], format=FORMATO_DATA)
    df["quantidade"] = df["quantidade"].astype(int)
    return df[list(COLUNAS_CSV.values())]


# Tarefa do Airflow: recarrega a tabela pedidos do zero (pode rodar várias vezes)
def carrega_pedidos():
    db.cria_tabelas()
    pedidos = le_csv_dataco()
    db.executa("TRUNCATE pedidos;")
    db.copia_df(pedidos, "pedidos")
    print(f"Pedidos carregados: {len(pedidos)} linhas, "
          f"de {pedidos['data_pedido'].min()} a {pedidos['data_pedido'].max()}")
    return len(pedidos)
