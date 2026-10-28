# Trabalho Final - Etapa 4: treino com Optuna, rastreado e registrado no MLflow (padrão da Aula 1)

import hashlib

import pandas as pd

from dataco.config import (MLFLOW_TRACKING_URI, NOME_EXPERIMENTO, NOME_MODELO,
                           SEMANAS_TUNING, SEMANAS_VALIDACAO)
from dataco.features import carrega_features
from dataco.modelagem import alinha_colunas, mae, prepara_X_y, rmse, separa_periodos

# Model card curto gravado na descrição de cada versão (lacuna de Governança da Atividade 1)
MODEL_CARD = """Modelo: previsão de demanda semanal (unidades) por categoria - DataCo Supply Chain.
Uso: apoiar a reposição de estoque. Não usar para decisões por produto individual.
Dados: pedidos DataCo até {data_corte}, sem pedidos cancelados ou suspeitos de fraude, {n_categorias} maiores categorias.
Algoritmo: XGBoost (XGBRegressor) com hiperparâmetros escolhidos pelo Optuna ({n_trials} tentativas).
Validação: últimas {semanas_validacao} semanas antes do corte. RMSE {rmse:.2f} | MAE {mae:.2f} | baseline ingênuo (lag_1) RMSE {rmse_baseline:.2f}.
Limites: só prevê 1 semana à frente; categorias novas ficam sem coluna própria no modelo.
Hash dos dados de treino: {hash_dados}"""


# Espaço de busca do Optuna
def sugere_parametros(trial):
    return {
        "n_estimators": trial.suggest_int("n_estimators", 50, 400),
        "max_depth": trial.suggest_int("max_depth", 2, 8),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
    }


# Hash dos dados de treino: identifica exatamente com quais dados o modelo foi treinado
def hash_dados(df):
    return hashlib.sha256(pd.util.hash_pandas_object(df, index=False).values.tobytes()).hexdigest()[:16]


# Tarefa do Airflow: treina o candidato e registra uma nova versão no Model Registry
def treina_modelo(data_corte, n_trials):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import mlflow
    import optuna
    from mlflow.models import infer_signature
    from xgboost import XGBRegressor

    n_trials = int(n_trials)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(NOME_EXPERIMENTO)
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    treino, validacao, _ = separa_periodos(carrega_features(), data_corte)
    if treino.empty or validacao.empty:
        raise ValueError(f"Sem dados suficientes para a data de corte {data_corte}")

    # Optuna usa só o treino: as últimas SEMANAS_TUNING semanas do treino viram validação interna.
    # Assim a janela de validação do gate fica limpa para comparar candidato e champion.
    corte_tuning = treino["semana"].max() - pd.Timedelta(weeks=SEMANAS_TUNING)
    X_tr, y_tr = prepara_X_y(treino[treino["semana"] <= corte_tuning])
    X_tu, y_tu = prepara_X_y(treino[treino["semana"] > corte_tuning])
    X_tu = alinha_colunas(X_tu, X_tr.columns)

    with mlflow.start_run(run_name=f"corte_{data_corte}") as run_pai:

        # Cada tentativa do Optuna vira uma run filha (como na Aula 1)
        def objetivo(trial):
            with mlflow.start_run(nested=True, run_name=f"trial_{trial.number}"):
                params = sugere_parametros(trial)
                modelo = XGBRegressor(objective="reg:squarederror", random_state=42, **params)
                modelo.fit(X_tr, y_tr)
                erro = rmse(y_tu, modelo.predict(X_tu))
                mlflow.log_params(params)
                mlflow.log_metric("rmse_tuning", erro)
                return erro

        # Semente fixa: reexecutar com os mesmos dados reproduz o mesmo modelo
        estudo = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=42))
        estudo.optimize(objetivo, n_trials=n_trials)

        # Modelo final: melhores parâmetros, treinado com todo o período de treino
        X_treino, y_treino = prepara_X_y(treino)
        X_val, y_val = prepara_X_y(validacao)
        X_val = alinha_colunas(X_val, X_treino.columns)
        modelo = XGBRegressor(objective="reg:squarederror", random_state=42, **estudo.best_params)
        modelo.fit(X_treino, y_treino)
        previsto = modelo.predict(X_val)

        metricas = {
            "rmse_validacao": rmse(y_val, previsto),
            "mae_validacao": mae(y_val, previsto),
            # Baseline ingênuo: "a próxima semana repete a anterior"
            "rmse_baseline": rmse(y_val, validacao["lag_1"]),
            "melhor_rmse_tuning": estudo.best_value,
        }
        info = {
            "data_corte": data_corte,
            "hash_dados": hash_dados(treino),
            "n_categorias": treino["categoria"].nunique(),
            "n_trials": n_trials,
            "semanas_validacao": SEMANAS_VALIDACAO,
        }

        mlflow.log_params({f"best_{k}": v for k, v in estudo.best_params.items()})
        mlflow.log_metrics(metricas)
        mlflow.set_tags({**info, "semanas_treino": treino["semana"].nunique()})

        # Artefatos: previsões da validação e gráfico real x previsto
        resultado = validacao[["categoria", "semana", "demanda"]].assign(previsto=previsto)
        mlflow.log_text(resultado.to_csv(index=False), "validacao/previsoes_validacao.csv")
        fig, ax = plt.subplots(figsize=(10, 5))
        total = resultado.groupby("semana")[["demanda", "previsto"]].sum()
        total.plot(ax=ax, marker="o", title=f"Demanda total - validação (corte {data_corte})")
        mlflow.log_figure(fig, "validacao/real_vs_previsto.png")
        plt.close(fig)

        # Registra o modelo: cada execução cria uma nova versão de dataco_demanda
        mlflow.xgboost.log_model(
            modelo,
            artifact_path="modelo",
            signature=infer_signature(X_val, previsto),
            input_example=X_val.head(3),
            registered_model_name=NOME_MODELO,
        )
        run_id = run_pai.info.run_id

    # Descobre o número da versão criada e grava o model card e as tags nela
    cliente = mlflow.MlflowClient()
    versao = cliente.search_model_versions(f"name='{NOME_MODELO}' and run_id='{run_id}'")[0].version
    cliente.update_model_version(
        NOME_MODELO, versao,
        description=MODEL_CARD.format(rmse=metricas["rmse_validacao"], mae=metricas["mae_validacao"],
                                      rmse_baseline=metricas["rmse_baseline"], **info),
    )
    cliente.set_model_version_tag(NOME_MODELO, versao, "data_corte", data_corte)
    cliente.set_model_version_tag(NOME_MODELO, versao, "rmse_validacao", f"{metricas['rmse_validacao']:.4f}")

    print(f"Versão {versao} registrada | RMSE {metricas['rmse_validacao']:.2f} "
          f"| baseline {metricas['rmse_baseline']:.2f}")

    # Vai para o XCom e é lido pelo gate
    return {"versao": int(versao), "run_id": run_id,
            "rmse_candidato": metricas["rmse_validacao"], "rmse_baseline": metricas["rmse_baseline"]}
