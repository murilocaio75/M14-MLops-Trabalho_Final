# Trabalho Final - Pipeline de MLOps com MLflow: Previsão de Demanda DataCo

| | |
|---|---|
| **Curso** | Generative AI Academy - Dell / Unisinos |
| **Disciplina** | MLOps |
| **Professor** | Marcos Leandro Hoffmann |
| **Alunos** | Caio Murilo, Gustavo Rost, Lucca Becker, Lyon Alves, Victor Franceschini |
| **Apresentação** | [apresentacao/DataCo_MLOps_Trabalho_Final.pptx](apresentacao/DataCo_MLOps_Trabalho_Final.pptx) |

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
├── apresentacao/                # slides da apresentação final (.pptx)
├── prints/                      # telas do Airflow, MLflow e Grafana usadas nos slides
├── PLANO_DE_TRABALHO.md         # plano e decisões do grupo
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
        ├── dados/                     # colocar aqui o CSV do Kaggle (passo 0)
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
2. As portas 8081, 5433, 5000 e 3000 precisam estar livres. Se a stack da Aula 4 estiver rodando, que usa as
   mesmas portas, derrube-a com `docker-compose down` na pasta dela.

3. Na pasta deste projeto, suba a stack (a primeira vez demora alguns minutos para montar as imagens):

   ```
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

## Roteiro da demo (4 execuções)

Disparar a DAG uma vez por linha, sempre esperando a anterior terminar:

| # | data_corte | n_trials | O que mostrar |
|---|---|---|---|
| 1 | `2016-06-27` | 14 | Primeiro modelo: o gate promove a v1 (não há champion). |
| 2 | `2017-03-27` | 15 | Retreino: a v2 é comparada com a v1 na mesma janela e promovida. |
| 3 | `2017-09-25` | 15 | **O gate aprova, mas o monitoramento acusa o problema**: a partir de outubro de 2017 as 10 categorias principais praticamente param de vender. |
| 4 | `2017-09-25` | 15 | **O gate recusa**: sem dados novos, o candidato empata com o champion e não é promovido. A execução feita ao vivo na apresentação. |

Resultados obtidos no Docker (27/10/2026). Com a semente fixa do Optuna, os números se repetem a cada execução:

| Corte | Candidato | RMSE candidato | RMSE baseline | Champion anterior (RMSE na mesma janela) | Decisão | MAE monitorado | Demanda real x prevista (12 semanas) |
|---|---|---|---|---|---|---|---|
| 2016-06-27 | v1 | 27,9 | 36,4 | — | promovido | 20,0 | 27.661 x 27.393 |
| 2017-03-27 | v2 | 31,5 | 39,2 | v1 (32,5) | promovido | 23,4 | 26.188 x 27.486 |
| 2017-09-25 | v3 | 26,8 | 35,8 | v2 (35,3) | promovido | **66,2** | **128 x 8.077** |
| 2017-09-25 | v4 | 26,8 | 35,8 | v3 (26,8) | **rejeitado** | 66,2 (champion segue v3) | 128 x 8.077 |

Como ler:

- **Cada linha é uma prova diferente**: os modelos são medidos nas 12 semanas antes daquele corte, então a
  comparação vale dentro da linha.
- **Modelos envelhecem**: a v1 errava 27,9 quando foi criada e 32,5 nos dados de 2017; a v2 foi de 31,5 para 35,3.
- **Depois do pico de outubro de 2017, o MAE volta para ≈ 20 mesmo com demanda real zero**, porque o modelo segue
  prevendo vendas pequenas. O MAE sozinho engana; o painel "real x prevista" mostra a ruptura.
- **Lição de MLOps**: o gate valida o passado; só o monitoramento enxerga o que acontece depois do deploy.

## Achado: o R² de 0,97 do repositório original

Na Atividade 1 desconfiamos do R² de 0,97 informado pelo repositório
[McGill-MMA-EnterpriseAnalytics/DataCo_Supply_Chain](https://github.com/McGill-MMA-EnterpriseAnalytics/DataCo_Supply_Chain).
Conferindo o código (`models/demand_forecast/Demand_Forecast_v3.py` e `Demand_Forecast_v6.ipynb`):

- **Vazamento de dados**: o modelo prevê `Order Item Quantity` usando todas as outras colunas, inclusive `Sales`.
  No CSV original, `Sales` é exatamente preço × quantidade em 100% das linhas, e cada um dos 118 produtos tem
  preço único. A quantidade sai de uma divisão; o modelo não prevê demanda.
- **Divisão aleatória** (`split_frame(ratios=[0.6])`) em dados com data.
- **Recorte pequeno**: só o 1º trimestre de 2015.

Este projeto evita os três problemas: as entradas são só o passado (lags e médias das semanas anteriores), a
divisão é sempre temporal e a avaliação usa 3 anos de dados. Por isso os números são mais modestos (20 a 25%
melhores que a regra simples), mas representam o que o modelo faria em produção.

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
