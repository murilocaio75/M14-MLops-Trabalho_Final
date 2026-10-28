# Trabalho Final - Imagem do Airflow com as bibliotecas de ML (mesma base da Aula 4)

FROM apache/airflow:2.10.2

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

# Fixa a versão do Airflow na instalação para o pip não trocá-la ao resolver as dependências
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r /opt/airflow/requirements.txt

# O xgboost puxa o numpy mais novo (2.x), mas o pandas 2.1.4 do Airflow foi compilado com numpy 1.x
# ("numpy.dtype size changed"). Volta para as versões compatíveis com a imagem do Airflow.
RUN pip install --no-cache-dir numpy==1.26.4 scipy==1.14.1
