# Trabalho Final - Servidor MLflow (tracking + Model Registry)

# Mesma versão do cliente instalado na imagem do Airflow
FROM ghcr.io/mlflow/mlflow:v2.17.2

# Driver do PostgreSQL, usado como backend store (runs, métricas e registry)
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
