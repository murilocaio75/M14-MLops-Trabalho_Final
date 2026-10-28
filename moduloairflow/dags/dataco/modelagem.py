# Trabalho Final - Funções de modelagem compartilhadas por treino, gate e previsão

import numpy as np
import pandas as pd

from dataco.config import SEMANAS_HORIZONTE, SEMANAS_VALIDACAO
from dataco.features import COLUNAS_NUMERICAS


# Divide no tempo, a partir da data de corte (que simula "hoje"):
#   treino    -> semanas até corte - SEMANAS_VALIDACAO
#   validacao -> as SEMANAS_VALIDACAO semanas até o corte (usadas pelo gate)
#   futuro    -> as SEMANAS_HORIZONTE semanas depois do corte (usadas no monitoramento)
def separa_periodos(df, data_corte):
    corte = pd.Timestamp(data_corte)
    inicio_validacao = corte - pd.Timedelta(weeks=SEMANAS_VALIDACAO)
    fim_futuro = corte + pd.Timedelta(weeks=SEMANAS_HORIZONTE)

    treino = df[df["semana"] <= inicio_validacao]
    validacao = df[(df["semana"] > inicio_validacao) & (df["semana"] <= corte)]
    futuro = df[(df["semana"] > corte) & (df["semana"] <= fim_futuro)]
    return treino, validacao, futuro


# Monta X (categoria em one-hot + numéricas) e y. Tudo float para bater com a assinatura do MLflow.
def prepara_X_y(df):
    X = pd.concat(
        [pd.get_dummies(df["categoria"], prefix="cat", dtype=float), df[COLUNAS_NUMERICAS]],
        axis=1,
    ).astype(float)
    return X, df["demanda"].astype(float)


# Deixa X com as mesmas colunas, na mesma ordem, que o modelo viu no treino
def alinha_colunas(X, colunas_modelo):
    return X.reindex(columns=colunas_modelo, fill_value=0.0)


def rmse(y_real, y_previsto):
    return float(np.sqrt(np.mean((np.asarray(y_real) - np.asarray(y_previsto)) ** 2)))


def mae(y_real, y_previsto):
    return float(np.mean(np.abs(np.asarray(y_real) - np.asarray(y_previsto))))
