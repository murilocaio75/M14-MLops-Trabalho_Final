# Trabalho Final - Etapa 6: previsões do champion gravadas para o monitoramento no Grafana

from dataco import db
from dataco.config import MLFLOW_TRACKING_URI
from dataco.features import carrega_features
from dataco.modelagem import mae, prepara_X_y, separa_periodos
from dataco.promocao import carrega_champion, preve


# Tarefa do Airflow: o champion prevê as semanas depois do corte (uma semana à frente por vez,
# usando os lags reais) e as previsões ficam lado a lado com a demanda real
def gera_previsoes(data_corte):
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    versao, champion = carrega_champion()
    if champion is None:
        print("Nenhum champion no Model Registry: nada a prever")
        return 0

    _, _, futuro = separa_periodos(carrega_features(), data_corte)
    if futuro.empty:
        print(f"Não há semanas depois de {data_corte} na base")
        return 0

    X, _ = prepara_X_y(futuro)
    previsoes = futuro[["semana", "categoria", "demanda"]].rename(columns={"demanda": "demanda_real"})
    previsoes["demanda_prevista"] = preve(champion, X)
    previsoes["data_corte"] = data_corte
    previsoes["versao_modelo"] = versao

    # Idempotente: reexecutar o mesmo corte substitui as previsões anteriores
    db.cria_tabelas()
    db.executa("DELETE FROM previsoes WHERE data_corte = %s", (data_corte,))
    db.copia_df(previsoes[["data_corte", "semana", "categoria", "demanda_real",
                           "demanda_prevista", "versao_modelo"]], "previsoes")

    print(f"{len(previsoes)} previsões gravadas com o champion v{versao} | "
          f"MAE {mae(previsoes['demanda_real'], previsoes['demanda_prevista']):.2f}")
    return len(previsoes)
