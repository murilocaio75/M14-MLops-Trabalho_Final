# Trabalho Final - Etapa 5: gate de promoção no Model Registry

from dataco import db
from dataco.config import (ALIAS_PRODUCAO, MARGEM_PROMOCAO, MLFLOW_TRACKING_URI,
                           NOME_MODELO)
from dataco.features import carrega_features
from dataco.modelagem import alinha_colunas, prepara_X_y, rmse, separa_periodos


# Regra do gate (função pura, testada em testes/):
# o candidato precisa vencer o baseline ingênuo E o modelo que está em produção
def decide_promocao(rmse_candidato, rmse_baseline, rmse_champion):
    if rmse_candidato >= rmse_baseline:
        return "rejeitado", "pior que o baseline ingênuo (lag_1)"
    if rmse_champion is None:
        return "promovido", "primeiro modelo: não há champion"
    if rmse_candidato < rmse_champion * (1 - MARGEM_PROMOCAO):
        return "promovido", "melhor que o champion atual"
    return "rejeitado", "não supera o champion atual"


# Carrega a versão apontada pelo alias e devolve (versão, modelo pyfunc), ou (None, None)
def carrega_champion():
    import mlflow
    from mlflow.exceptions import MlflowException

    cliente = mlflow.MlflowClient()
    try:
        versao = cliente.get_model_version_by_alias(NOME_MODELO, ALIAS_PRODUCAO).version
    except MlflowException:
        return None, None
    return int(versao), mlflow.pyfunc.load_model(f"models:/{NOME_MODELO}@{ALIAS_PRODUCAO}")


# Previsão com um modelo pyfunc, alinhando as colunas com a assinatura registrada
def preve(modelo, X):
    colunas = modelo.metadata.get_input_schema().input_names()
    return modelo.predict(alinha_colunas(X, colunas))


# Tarefa do Airflow: compara candidato x champion na mesma janela de validação
def avalia_e_promove(data_corte, ti):
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    candidato = ti.xcom_pull(task_ids="treina_modelo")

    _, validacao, _ = separa_periodos(carrega_features(), data_corte)
    X_val, y_val = prepara_X_y(validacao)

    versao_champion, champion = carrega_champion()
    rmse_champion = rmse(y_val, preve(champion, X_val)) if champion else None

    decisao, motivo = decide_promocao(candidato["rmse_candidato"], candidato["rmse_baseline"], rmse_champion)

    # A API REST do Model Registry recebe a versão como texto
    cliente = mlflow.MlflowClient()
    versao_candidata = str(candidato["versao"])
    if decisao == "promovido":
        cliente.set_registered_model_alias(NOME_MODELO, ALIAS_PRODUCAO, versao_candidata)
    cliente.set_model_version_tag(NOME_MODELO, versao_candidata, "decisao_gate", f"{decisao}: {motivo}")

    db.cria_tabelas()
    db.executa(
        """INSERT INTO decisoes_modelo (data_corte, versao_candidata, rmse_candidato, rmse_baseline,
                                        versao_champion, rmse_champion, decisao, motivo)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (data_corte, candidato["versao"], candidato["rmse_candidato"], candidato["rmse_baseline"],
         versao_champion, rmse_champion, decisao, motivo),
    )

    texto_champion = f"v{versao_champion} RMSE {rmse_champion:.2f}" if champion else "nenhum"
    print(f"Candidato v{candidato['versao']} RMSE {candidato['rmse_candidato']:.2f} | "
          f"champion {texto_champion} -> {decisao.upper()} ({motivo})")
    return decisao
