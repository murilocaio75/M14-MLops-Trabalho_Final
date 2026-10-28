# Trabalho Final - Configurações do pipeline de demanda DataCo

import os

# Arquivo original do Kaggle (DataCo Smart Supply Chain), colocado em dags/dados/
CAMINHO_CSV = "/opt/airflow/dags/dados/DataCoSupplyChainDataset.csv"
ENCODING_CSV = "latin-1"

# Colunas do CSV original -> nomes usados no pipeline
COLUNAS_CSV = {
    "order date (DateOrders)": "data_pedido",
    "Category Name": "categoria",
    "Order Item Quantity": "quantidade",
    "Order Status": "status",
}
FORMATO_DATA = "%m/%d/%Y %H:%M"

# Pedidos que não representam demanda real
STATUS_EXCLUIDOS = ("CANCELED", "SUSPECTED_FRAUD")

# Conexão com o banco do negócio (mesmo Postgres do Airflow, banco separado)
DB = {
    "dbname": "dataco",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432",
}

# Modelagem
TOP_CATEGORIAS = 10          # categorias com maior volume (as demais são pouco frequentes)
SEMANAS_VALIDACAO = 12       # janela antes da data de corte usada pelo gate
SEMANAS_TUNING = 8           # fatia final do treino usada pelo Optuna (evita tunar no holdout do gate)
SEMANAS_HORIZONTE = 12       # janela depois da data de corte usada para monitorar o modelo
MIN_SEMANAS_HISTORICO = 52   # validação de dados: precisa de pelo menos 1 ano

# MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
NOME_EXPERIMENTO = "dataco_demanda_semanal"
NOME_MODELO = "dataco_demanda"
ALIAS_PRODUCAO = "champion"
MARGEM_PROMOCAO = 0.0        # ex.: 0.02 exige que o candidato seja 2% melhor que o champion
