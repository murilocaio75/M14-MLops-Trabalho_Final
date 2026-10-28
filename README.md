# Trabalho Final - Pipeline de MLOps com MLflow: Previsão de Demanda DataCo

Caso de negócio: **previsão de demanda semanal por categoria** da DataCo Global (supply chain), o mesmo case
do diagnóstico de maturidade da Atividade 1 (nota 6,0 de 12). O trabalho implementa as 3 ações propostas lá:

| Ação da Atividade 1 | Onde está no projeto | Aula |
|---|---|---|
| 3 · Versionar dados e modelos | MLflow Tracking + Model Registry (alias `champion`), hash dos dados, model card | 1 |
| 2 · Automatizar com testes | DAG do Airflow com testes de unidade + validação de dados antes do treino | 3 e 4 |
| 1 · Monitorar | Previsões x demanda real no Postgres, painel provisionado no Grafana | 4 |

## Arquitetura

```
                       Airflow (DAG pipeline_demanda_dataco)
 ┌────────────────────────────────────────────────────────────────────────────────────┐
 │ testes_unitarios → ingere_dados → gera_features → valida_dados → treina_modelo      │
 │                                                                    → avalia_e_promove│
 │                                                                    → gera_previsoes  │
 └──────┬─────────────────────────┬──────────────────────────┬────────────────────────┘
        │                         │                          │
  CSV DataCo  →  Postgres (banco dataco)          MLflow (runs, métricas, artefatos,
                 pedidos, features_demanda_semanal,  Model Registry dataco_demanda@champion)
                 decisoes_modelo, previsoes
                        │
                     Grafana (dashboard "DataCo - Monitoramento do modelo de demanda")
```

| Serviço | Endereço | Login |
|---|---|---|
| Airflow | http://localhost:8081 | airflow / airflow |
| MLflow | http://localhost:5000 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Postgres | localhost:5433 | airflow / airflow (bancos `airflow`, `mlflow`, `dataco`) |

## Estrutura

```
Trabalho_Final/
├── docker-compose.yaml          # base da Aula 4 sem Elasticsearch/Redis/LLM, com MLflow
├── postgres/init.sql            # cria os bancos mlflow e dataco
├── mlflow/Dockerfile            # servidor MLflow 2.17.2 + driver Postgres
├── grafana/
│   ├── provisioning/            # datasource e dashboard entram sozinhos
│   └── dashboards/dataco_demanda.json
└── moduloairflow/
    ├── Dockerfile, requirements.txt   # Airflow 2.10.2 + mlflow-skinny, xgboost, optuna
    ├── testes/test_pipeline.py        # 13 testes de unidade (rodam como 1ª tarefa da DAG)
    └── dags/
        ├── pipeline_demanda_dataco.py # a DAG
        ├── dados/DataCoSupplyChainDataset.csv
        └── dataco/                    # config, db, ingestao, features, validacao,
                                       # modelagem, treino, promocao, previsao
```

## Como o pipeline funciona

- **`data_corte`** (parâmetro da DAG) simula "hoje": o modelo só enxerga dados até essa data.
  Rodar de novo com um corte mais recente simula a passagem do tempo (retreino).
- **Divisão temporal**: treino até `corte − 12 semanas`; validação nas 12 semanas antes do corte;
  monitoramento nas 12 semanas depois do corte. O Optuna tuna só dentro do treino (últimas 8 semanas dele),
  para a janela de validação ficar limpa para o gate.
- **Gate de promoção**: o candidato só vira `champion` se tiver RMSE menor que o **baseline ingênuo**
  ("a próxima semana repete a anterior") **e** menor que o **champion atual**, medidos na mesma janela.
- **Governança**: cada versão registrada leva model card na descrição, tags (`data_corte`, `rmse_validacao`,
  `decisao_gate`) e o hash dos dados de treino. Toda decisão do gate fica na tabela `decisoes_modelo`.

## Execução (passo a passo)

0. **Baixe o dataset** (não fica no repositório: são 92 MB, e versionar dados brutos no Git foi justamente
   uma das lacunas do diagnóstico da Atividade 1). No Kaggle, em
   [DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis),
   baixe o arquivo, descompacte e coloque o `DataCoSupplyChainDataset.csv` em `moduloairflow/dags/dados/`.
1. Inicie o Docker Desktop.
2. Se a stack da Aula 4 estiver de pé, derrube-a (as portas 8081, 5433 e 3000 são as mesmas):

   ```
   cd ../Aula_4
   docker-compose down
   ```

3. Suba a stack do trabalho (a primeira vez demora alguns minutos para montar as imagens):

   ```
   cd ../Trabalho_Final
   docker-compose up --build -d
   docker-compose ps
   ```

   Esperado: `dataco_postgres` (healthy), `dataco_mlflow`, `dataco_airflow_webserver`, `dataco_airflow_scheduler`
   e `dataco_grafana` **Up**; `dataco_airflow_init` **Exited (0)**.

4. Abra o Airflow (localhost:8081). Na DAG `pipeline_demanda_dataco`, clique em **▶ Trigger DAG**,
   informe `data_corte` e `n_trials` e confirme. Acompanhe no **Graph**.

5. Confira os resultados:
   - **MLflow** (localhost:5000): experimento `dataco_demanda_semanal` (run pai + runs filhas do Optuna) e
     **Models → dataco_demanda** (versões, alias `champion`, model card, tag `decisao_gate`).
   - **Grafana** (localhost:3000): pasta **DataCo**, escolha a **Data de corte** no topo do painel.
   - **Logs** da tarefa `avalia_e_promove` mostram a decisão do gate em uma linha.

## Roteiro da demo (3 execuções)

Disparar a DAG 3 vezes, uma por vez, com `n_trials = 15`:

| # | data_corte | O que mostrar |
|---|---|---|
| 1 | `2016-06-27` | Primeiro modelo: gate promove v1 (não há champion). MAE do período monitorado ≈ 20. |
| 2 | `2017-03-27` | Retreino: v2 é comparado com v1 na mesma janela e o gate decide (no teste local, promoveu). |
| 3 | `2017-09-25` | **O gate aprova, mas o monitoramento acusa o problema**: a partir de outubro de 2017 as 10 categorias principais praticamente param de vender (128 unidades reais contra cerca de 8 mil previstas em 12 semanas). |

Números de referência de um teste local com os dados reais (podem variar um pouco no Docker por versão de biblioteca):

| corte | RMSE candidato | RMSE baseline | RMSE champion anterior | decisão | MAE monitorado |
|---|---|---|---|---|---|
| 2016-06-27 | 27,9 | 36,4 | — | promovido | 20,0 |
| 2017-03-27 | 31,5 | 39,2 | 32,5 (v1) | promovido | 23,4 |
| 2017-09-25 | 26,8 | 35,8 | 35,3 (v2) | promovido | 66,2 (pico na 1ª semana de outubro) |

Ponto para a apresentação: depois do pico, o MAE volta para ≈ 20 **mesmo com demanda real zero**,
porque o modelo segue prevendo vendas pequenas. O MAE sozinho engana; o painel "real x prevista" mostra a ruptura.
Lição de MLOps: o gate valida o passado; só o monitoramento enxerga o que acontece depois do deploy.

## Rodar os testes fora da DAG

```
docker exec dataco_airflow_scheduler bash -c "cd /opt/airflow && PYTHONPATH=dags python -m unittest discover -s testes -v"
```

## Recomeçar do zero

```
docker-compose down -v
```

O `-v` apaga os volumes (Postgres, artefatos do MLflow e Grafana). Sem `-v`, as execuções anteriores continuam:
reexecutar o mesmo corte substitui as previsões dele, mas cria uma nova versão do modelo e uma nova linha de decisão.

## Limitações conhecidas

- O cliente MLflow da imagem do Airflow é o `mlflow-skinny`, igual em versão ao servidor (2.17.2).
- O Dockerfile do Airflow força `numpy==1.26.4` e `scipy==1.14.1` depois das bibliotecas de ML: o xgboost
  puxa o numpy 2.x, que quebra o pandas 2.1.4 da imagem do Airflow ("numpy.dtype size changed").
- A DAG é disparada manualmente na demo (`schedule=None`); em produção seria `@weekly`.
- Previsão de 1 semana à frente, usando os lags reais (não é previsão multi-semana).
- Não há alerta automático configurado no Grafana; o limiar de MAE = 50 aparece como linha no painel.
