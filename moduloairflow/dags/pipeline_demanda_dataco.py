# Trabalho Final - DAG do pipeline de MLOps de previsão de demanda (DataCo Supply Chain)
#
# testes_unitarios -> ingere_dados -> gera_features -> valida_dados
#   -> treina_modelo (Optuna + MLflow) -> avalia_e_promove (gate no Model Registry) -> gera_previsoes
#
# O parâmetro data_corte simula "hoje": o modelo só enxerga dados até essa data.
# Rodar a DAG de novo com um corte mais recente simula a passagem do tempo (retreino).

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dataco.features import gera_features
from dataco.ingestao import carrega_pedidos
from dataco.previsao import gera_previsoes
from dataco.promocao import avalia_e_promove
from dataco.treino import treina_modelo
from dataco.validacao import valida_dados

argumentos_padrao = {
    "owner": "grupo_dataco",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_demanda_dataco",
    description="Previsão de demanda semanal DataCo: dados -> MLflow -> gate -> previsões -> Grafana",
    default_args=argumentos_padrao,
    start_date=datetime(2026, 10, 1),
    # Disparo manual na demo. Em produção seria "@weekly".
    schedule=None,
    catchup=False,
    params={
        "data_corte": Param("2016-12-31", type="string", description="Simula 'hoje' (AAAA-MM-DD)"),
        "n_trials": Param(20, type="integer", minimum=1, description="Tentativas do Optuna"),
    },
    tags=["mlops", "mlflow", "dataco"],
) as dag:

    # Testes de unidade da lógica do pipeline (a ação "CI com testes" da Atividade 1)
    testes_unitarios = BashOperator(
        task_id="testes_unitarios",
        bash_command="cd /opt/airflow && PYTHONPATH=/opt/airflow/dags python -m unittest discover -s testes -v",
    )

    ingere_dados = PythonOperator(task_id="ingere_dados", python_callable=carrega_pedidos)

    tarefa_gera_features = PythonOperator(task_id="gera_features", python_callable=gera_features)

    tarefa_valida_dados = PythonOperator(task_id="valida_dados", python_callable=valida_dados)

    tarefa_treina_modelo = PythonOperator(
        task_id="treina_modelo",
        python_callable=treina_modelo,
        op_kwargs={"data_corte": "{{ params.data_corte }}", "n_trials": "{{ params.n_trials }}"},
    )

    tarefa_avalia_e_promove = PythonOperator(
        task_id="avalia_e_promove",
        python_callable=avalia_e_promove,
        op_kwargs={"data_corte": "{{ params.data_corte }}"},
    )

    tarefa_gera_previsoes = PythonOperator(
        task_id="gera_previsoes",
        python_callable=gera_previsoes,
        op_kwargs={"data_corte": "{{ params.data_corte }}"},
    )

    (testes_unitarios >> ingere_dados >> tarefa_gera_features >> tarefa_valida_dados
     >> tarefa_treina_modelo >> tarefa_avalia_e_promove >> tarefa_gera_previsoes)
