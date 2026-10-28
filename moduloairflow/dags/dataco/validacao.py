# Trabalho Final - Etapa 3: validação de dados antes do treino (o pipeline para se falhar)

from dataco.config import MIN_SEMANAS_HISTORICO
from dataco.features import COLUNAS_NUMERICAS, carrega_features

COLUNAS_OBRIGATORIAS = ["categoria", "semana", "demanda"] + COLUNAS_NUMERICAS


# Devolve a lista de problemas encontrados (lista vazia = dados aprovados)
def valida_features(df):
    faltando = [c for c in COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltando:
        return [f"Colunas ausentes: {faltando}"]

    problemas = []
    if df.empty:
        problemas.append("Feature store vazia")
    if df[COLUNAS_OBRIGATORIAS].isna().any().any():
        problemas.append("Existem valores nulos nas features")
    if (df["demanda"] < 0).any():
        problemas.append("Existe demanda negativa")
    if df.duplicated(subset=["categoria", "semana"]).any():
        problemas.append("Semana duplicada para a mesma categoria")
    if df["semana"].nunique() < MIN_SEMANAS_HISTORICO:
        problemas.append(f"Histórico curto: {df['semana'].nunique()} semanas (mínimo {MIN_SEMANAS_HISTORICO})")
    return problemas


# Tarefa do Airflow: falha a DAG se os dados não passarem
def valida_dados():
    problemas = valida_features(carrega_features())
    if problemas:
        raise ValueError("Validação de dados reprovada: " + "; ".join(problemas))
    print("Validação de dados aprovada")
