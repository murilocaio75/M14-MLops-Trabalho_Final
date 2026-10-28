# Trabalho Final - Acesso ao banco do negócio (dataco)

import io

import pandas as pd
import psycopg2

from dataco.config import DB

# Tabelas do negócio. Todas as etapas da DAG dependem só delas.
DDL = """
CREATE TABLE IF NOT EXISTS pedidos (
    data_pedido TIMESTAMP,
    categoria   TEXT,
    quantidade  INTEGER,
    status      TEXT
);

CREATE TABLE IF NOT EXISTS decisoes_modelo (
    id               SERIAL PRIMARY KEY,
    executado_em     TIMESTAMP DEFAULT NOW(),
    data_corte       DATE,
    versao_candidata INTEGER,
    rmse_candidato   DOUBLE PRECISION,
    rmse_baseline    DOUBLE PRECISION,
    versao_champion  INTEGER,
    rmse_champion    DOUBLE PRECISION,
    decisao          TEXT,
    motivo           TEXT
);

CREATE TABLE IF NOT EXISTS previsoes (
    id               SERIAL PRIMARY KEY,
    gerado_em        TIMESTAMP DEFAULT NOW(),
    data_corte       DATE,
    semana           DATE,
    categoria        TEXT,
    demanda_real     DOUBLE PRECISION,
    demanda_prevista DOUBLE PRECISION,
    versao_modelo    INTEGER
);
"""


# Abre conexão com o banco dataco
def conecta():
    return psycopg2.connect(**DB)


# Executa um comando sem retorno (DDL, DELETE, INSERT)
def executa(sql, params=None):
    conn = conecta()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
    finally:
        conn.close()


# Cria as tabelas, se ainda não existirem
def cria_tabelas():
    executa(DDL)


# Lê o resultado de uma consulta como DataFrame
def le_df(sql, params=None):
    conn = conecta()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            colunas = [c[0] for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=colunas)
    finally:
        conn.close()


# Grava um DataFrame numa tabela via COPY (rápido para os ~180 mil pedidos)
def copia_df(df, tabela):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    conn = conecta()
    try:
        with conn, conn.cursor() as cur:
            cur.copy_expert(f"COPY {tabela} ({', '.join(df.columns)}) FROM STDIN WITH CSV", buffer)
    finally:
        conn.close()
