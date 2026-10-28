# Trabalho Final - Etapa 2: feature store de demanda semanal por categoria (padrão da Aula 3)

import pandas as pd

from dataco import db
from dataco.config import STATUS_EXCLUIDOS, TOP_CATEGORIAS

COLUNAS_NUMERICAS = ["lag_1", "lag_2", "lag_4", "media_4", "semana_ano", "mes"]

# Agregação feita no próprio Postgres: semana começando na segunda-feira
SQL_DEMANDA_SEMANAL = """
SELECT categoria,
       date_trunc('week', data_pedido)::date AS semana,
       SUM(quantidade)                        AS demanda
FROM pedidos
WHERE status NOT IN %s
GROUP BY 1, 2
"""


# Transforma a demanda semanal agregada em features (função pura, testada em testes/)
def calcula_features(agregado, top_n=TOP_CATEGORIAS):
    df = agregado.copy()
    df["semana"] = pd.to_datetime(df["semana"])
    df["demanda"] = df["demanda"].astype(float)

    # Período coberto pela base inteira (antes de filtrar categorias)
    semanas = pd.date_range(df["semana"].min(), df["semana"].max(), freq="W-MON")

    # Mantém as categorias de maior volume
    top = df.groupby("categoria")["demanda"].sum().nlargest(top_n).index
    df = df[df["categoria"].isin(top)]

    # Semanas sem venda viram demanda 0 até o fim da base (senão os lags pulariam semanas
    # e uma categoria que parou de vender simplesmente sumiria, sem aparecer no monitoramento)
    grade = pd.MultiIndex.from_product([sorted(top), semanas], names=["categoria", "semana"])
    df = df.set_index(["categoria", "semana"]).reindex(grade, fill_value=0.0).reset_index()

    # Lags e média móvel por categoria, sempre olhando só para o passado
    por_categoria = df.groupby("categoria")["demanda"]
    df["lag_1"] = por_categoria.shift(1)
    df["lag_2"] = por_categoria.shift(2)
    df["lag_4"] = por_categoria.shift(4)
    df["media_4"] = por_categoria.transform(lambda s: s.shift(1).rolling(4).mean())
    df["semana_ano"] = df["semana"].dt.isocalendar().week.astype(float)
    df["mes"] = df["semana"].dt.month.astype(float)

    return df.dropna(subset=COLUNAS_NUMERICAS).reset_index(drop=True)


# Lê a feature store do banco
def carrega_features():
    df = db.le_df("SELECT * FROM features_demanda_semanal ORDER BY categoria, semana")
    df["semana"] = pd.to_datetime(df["semana"])
    return df


# Tarefa do Airflow: recalcula a feature store a partir dos pedidos
def gera_features():
    agregado = db.le_df(SQL_DEMANDA_SEMANAL, (STATUS_EXCLUIDOS,))
    features = calcula_features(agregado)

    db.executa("""
        DROP TABLE IF EXISTS features_demanda_semanal;
        CREATE TABLE features_demanda_semanal (
            categoria TEXT, semana DATE, demanda DOUBLE PRECISION,
            lag_1 DOUBLE PRECISION, lag_2 DOUBLE PRECISION, lag_4 DOUBLE PRECISION,
            media_4 DOUBLE PRECISION, semana_ano DOUBLE PRECISION, mes DOUBLE PRECISION
        );
    """)
    db.copia_df(features, "features_demanda_semanal")
    print(f"Features: {len(features)} linhas, {features['categoria'].nunique()} categorias, "
          f"{features['semana'].nunique()} semanas")
    return len(features)
