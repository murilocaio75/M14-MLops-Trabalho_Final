# Plano de Trabalho — Trabalho Final de MLOps

**Curso:** Generative AI Academy · Dell / Unisinos
**Disciplina:** MLOps · Prof. Marcos Leandro Hoffmann
**Alunos:** Caio Murilo, Gustavo Rost, Lucca Becker, Lyon Alves, Victor Franceschini
**Case:** DataCo Global Supply Chain — previsão de demanda semanal por categoria
**Apresentado em:** 28/10/2026

---

## 1. O que o professor pediu

Uma **apresentação** com uma **solução de negócio** que aplique o
conteúdo das **aulas 1 a 4**, com **ênfase nas aplicações da aula 4**, usando o case escolhido na Atividade 1.
O foco é **demonstrar o pipeline de MLflow aplicado ao dataset**.

## 2. Nossa ideia em uma frase

> Na Atividade 1 diagnosticamos a DataCo com **6,0 de 12** e propusemos **3 ações**.
> No trabalho final, **implementamos as 3 ações** num pipeline que roda de ponta a ponta e mostramos
> o ciclo de vida do modelo funcionando ao vivo.

A história fecha sozinha: diagnóstico → lacunas → ações → **ações implementadas** → nova nota.

## 3. Retomada da Atividade 1

| Componente | Nota | Lacuna apontada | Ação proposta |
|---|---|---|---|
| Experimentação | 2,0 | Dados e modelos sem versão | **Ação 3:** versionar dados e modelos (MLflow Registry) |
| Implantação | 1,5 | Deploy manual e sem teste | **Ação 2:** automatizar com testes |
| Monitoramento | 1,0 | O modelo piora sem ninguém ver | **Ação 1:** monitorar e alertar |
| Governança | 1,5 | Sem model card nem rastro de versões | (coberta junto com a Ação 3) |

## 4. Por que esta abordagem

- A **aula 4 não usa MLflow**: ela usa Airflow + Postgres + Elasticsearch + LLM (BERT) + Streamlit + Grafana.
  O MLflow vem da **aula 1** (MLflow + Optuna) e a Feature Store e os testes vêm da **aula 3**.
- Para dar ênfase à aula 4 **e** demonstrar MLflow, colocamos o **pipeline de MLflow dentro da stack da aula 4**:
  o **Airflow orquestra**, o **MLflow rastreia e versiona** e o **Grafana monitora**.
- **Descartamos replicar o RAG da aula 4** com o DataCo: o dataset é uma tabela de pedidos, não um conjunto
  de documentos com perguntas e respostas. Ficaria forçado e não mostraria MLflow.
- **Escolhemos previsão de demanda** (e não risco de atraso ou fraude) porque foi o exemplo principal que
  defendemos na Atividade 1 e porque o custo do erro é fácil de explicar: estoque parado ou ruptura.

## 5. O que cada aula contribui

| Aula | Conteúdo | Como aparece no trabalho |
|---|---|---|
| 1 | MLflow + Optuna (otimização de hiperparâmetros) | Treino com Optuna; cada tentativa é uma run filha no MLflow; modelo vai para o Model Registry |
| 2 | Pipeline de dados com Postgres em Docker | Ingestão do CSV para o Postgres; pipeline em etapas |
| 3 | Feature Store + testes de unidade | Tabela `features_demanda_semanal`; 13 testes rodam como 1ª tarefa da DAG |
| 4 | Airflow + Postgres + Grafana em docker-compose | A mesma stack, trocando Elasticsearch/LLM por MLflow; DAG no Airflow; painel no Grafana |

## 6. Arquitetura da solução

```
                     Airflow — DAG pipeline_demanda_dataco
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ 1 testes_unitarios → 2 ingere_dados → 3 gera_features → 4 valida_dados        │
 │   → 5 treina_modelo → 6 avalia_e_promove → 7 gera_previsoes                   │
 └───────┬────────────────────────────┬───────────────────────────┬─────────────┘
         │                            │                           │
   CSV DataCo  →  Postgres (banco dataco)          MLflow: runs, métricas, artefatos
                  pedidos, features,               e Model Registry
                  decisões do gate, previsões      (dataco_demanda@champion)
                         │
                      Grafana — painel de monitoramento
```

| Serviço | Endereço | Login |
|---|---|---|
| Airflow | http://localhost:8081 | airflow / airflow |
| MLflow | http://localhost:5000 | — |
| Grafana | http://localhost:3000 | admin / admin |

### As 7 etapas da DAG

| # | Tarefa | O que faz | Ação da Atividade 1 |
|---|---|---|---|
| 1 | `testes_unitarios` | Roda os 13 testes de unidade; se algum falhar, o pipeline para | Ação 2 |
| 2 | `ingere_dados` | Lê os ~180 mil pedidos do CSV e grava no Postgres | — |
| 3 | `gera_features` | Agrega a demanda semanal das 10 maiores categorias e cria lags (feature store) | — |
| 4 | `valida_dados` | Checa nulos, demanda negativa, duplicatas e histórico mínimo; se falhar, o pipeline para | Ação 2 |
| 5 | `treina_modelo` | XGBoost + Optuna; registra runs, métricas, gráfico e uma nova versão no Model Registry com model card | Ação 3 |
| 6 | `avalia_e_promove` | **Gate:** só promove se vencer o baseline e o champion atual; grava a decisão | Ação 3 + Governança |
| 7 | `gera_previsoes` | O champion prevê as semanas seguintes; previsões e demanda real vão para o Postgres | Ação 1 |

### Conceitos que vale explicar na apresentação

- **`data_corte` simula "hoje".** O modelo só enxerga dados até essa data. Rodar a DAG de novo com um corte
  mais recente simula a passagem do tempo e o retreino.
- **Divisão temporal, não aleatória.** Treino no passado, validação nas 12 semanas antes do corte e
  monitoramento nas 12 semanas depois. Isso fecha a dúvida da Atividade 1 sobre o R² de 0,97 do repositório
  original: conferindo o código, a causa principal é vazamento de dados (o modelo recebia `Sales`, que é
  preço × quantidade), somado à divisão aleatória e ao uso de só um trimestre. Detalhes no README.
- **Gate de dois critérios.** O candidato precisa ter RMSE menor que o **baseline ingênuo** ("a semana que vem
  repete a anterior") **e** menor que o **champion atual**, medidos na mesma janela.
- **Rastreabilidade.** Cada versão do modelo leva model card, data de corte, RMSE, decisão do gate e o hash dos
  dados de treino. Dá para responder "quem treinou, com quais dados e qual versão está em produção".

## 7. A demo: quatro execuções da DAG

| # | data_corte | Resultado | Mensagem |
|---|---|---|---|
| 1 | `2016-06-27` | v1 promovida (não havia champion). MAE ≈ 20 | O pipeline coloca o primeiro modelo em produção sozinho |
| 2 | `2017-03-27` | v2 comparada com v1 na mesma janela; promovida (RMSE 31,5 contra 32,5) | Retreino com critério, não troca às cegas |
| 3 | `2017-09-25` | v3 promovida, mas o MAE sobe para ≈ 66 | **O gate aprova, mas o monitoramento acusa o problema** |
| 4 | `2017-09-25` (ao vivo) | candidato empata com a v3 (26,8) e é rejeitado | **Sem dados novos, o modelo de produção não muda**; o pipeline é reprodutível |

### O ponto alto: a ruptura de outubro de 2017

Nos dados reais, a partir de outubro de 2017 as 10 categorias principais praticamente param de vender:
**128 unidades reais contra cerca de 8 mil previstas** em 12 semanas.

- O **gate** olha para o passado (as semanas antes do corte), então aprova o modelo.
- Só o **monitoramento** enxerga o que acontece depois do deploy.
- Detalhe: depois do pico, o MAE volta para ≈ 20 **mesmo com demanda real zero**, porque o modelo continua
  prevendo vendas pequenas. **O MAE sozinho engana**; o painel "real x prevista" mostra a ruptura.

### Resultado real das 3 execuções no Docker (27/10/2026; n_trials = 14 no 1º corte e 15 nos demais)

| Corte | Candidato | RMSE candidato | RMSE baseline | Champion anterior (RMSE) | Decisão | MAE monitorado | Demanda real x prevista (12 semanas) |
|---|---|---|---|---|---|---|---|
| 2016-06-27 | v1 | 27,9 | 36,4 | — | promovido | 20,0 | 27.661 x 27.393 |
| 2017-03-27 | v2 | 31,5 | 39,2 | v1 (32,5) | promovido | 23,4 | 26.188 x 27.486 |
| 2017-09-25 | v3 | 26,8 | 35,8 | v2 (35,3) | promovido | **66,2** | **128 x 8.077** |

Os números são iguais aos do teste local: com a semente fixa do Optuna, o pipeline é reprodutível. A 4ª execução,
feita ao vivo, repetiu o corte 2017-09-25 com `n_trials = 15`: o candidato empatou com a v3 (RMSE 26,8) e foi rejeitado.

## 8. Resultado esperado na régua de maturidade

Estimativa nossa, para o slide de fechamento (não é nota oficial):

| Componente | Antes | Depois | O que mudou |
|---|---|---|---|
| Experimentação | 2,0 | 2,5 | Pipeline orquestrado, Optuna rastreado, dados e modelos versionados |
| Implantação | 1,5 | 2,0 | Docker Compose, testes automáticos e gate antes de promover |
| Monitoramento | 1,0 | 2,0 | Previsão x real no Grafana, com limiar de erro |
| Governança | 1,5 | 2,0 | Model card, tags, hash dos dados e histórico de decisões |
| **Total** | **6,0** | **≈ 8,5** | |

O que **não** fizemos (vale dizer na apresentação): CI/CD externo (GitHub Actions), alerta automático por
e-mail, detecção formal de drift (Evidently) e deploy em nuvem.

## 9. Estrutura da apresentação final (12 slides)

Arquivo: [apresentacao/DataCo_MLOps_Trabalho_Final.pptx](apresentacao/DataCo_MLOps_Trabalho_Final.pptx)

| # | Slide | Conteúdo |
|---|---|---|
| 1 | Capa | Título, curso, alunos e link do repositório |
| 2 | Retomada | Diagnóstico 6,0/12, as lacunas e como o trabalho responde |
| 3 | Caso de negócio | Custo do erro (estoque parado x ruptura) e números da base |
| 4 | Arquitetura | Fluxo CSV → Airflow → MLflow → Postgres → Grafana e o que veio de cada aula |
| 5 | A DAG | Print do Airflow com as 7 etapas |
| 6 | Simular o tempo | Data de corte, treino, validação e monitoramento |
| 7 | MLflow Tracking | Experimento com as runs do Optuna e a run do corte 3 |
| 8 | O gate | Regra de promoção, log do Airflow e Model Registry |
| 9 | Tempo normal | Grafana nos cortes 1 e 2: −1,0% e +5,0% |
| 10 | A ruptura | 128 vendidas x 8.077 previstas: o gate aprovou, o monitoramento acusou |
| 11 | Régua | De 6,0 para ≈ 8,5, componente por componente |
| 12 | Próximos passos | O que falta para o nível Avançado |


## 10. Como rodar (resumo)

Detalhes completos no [README.md](README.md).

1. Baixar o dataset `DataCoSupplyChainDataset.csv` do Kaggle (~95 MB, fora do Git) e colocar em
   `moduloairflow/dags/dados/` (link no README, passo 0).
2. Iniciar o Docker Desktop, com as portas 8081, 5433, 5000 e 3000 livres.
3. Na pasta do projeto: `docker-compose up --build -d` e conferir com `docker-compose ps`.
4. No Airflow (localhost:8081), disparar a DAG `pipeline_demanda_dataco` com **▶ Trigger DAG**,
   informando `data_corte` e `n_trials`. Repetir para as execuções da seção 7, uma por vez.
5. Conferir MLflow (localhost:5000) e Grafana (localhost:3000, pasta **DataCo**).


## 11. Cuidados ao executar

| Situação | Como lidar |
|---|---|
| Conflito de portas com a Aula 4 (8081, 5433, 3000) ou porta 5000 ocupada | Derrubar a Aula 4; se a 5000 estiver em uso, trocar para `5001:5000` no compose |
| Falha ao montar a imagem do Airflow (conflito de versões no pip) | Copiar o erro do build e ajustar as versões em `moduloairflow/requirements.txt` |
| Pouca memória no Docker Desktop | Reduzir `n_trials` (ex.: 5) |
| Encerrar sem perder as execuções | `docker-compose down` (sem `-v`; o `-v` apaga os volumes) |

## 12. Estrutura dos arquivos

```
Trabalho_Final/
├── PLANO_DE_TRABALHO.md         # este documento
├── README.md                    # instruções de execução
├── apresentacao/                # slides da apresentação final
├── prints/                      # telas usadas nos slides
├── docker-compose.yaml          # Postgres, Airflow, MLflow, Grafana
├── postgres/init.sql            # cria os bancos mlflow e dataco
├── mlflow/Dockerfile            # servidor MLflow 2.17.2
├── grafana/                     # datasource e dashboard provisionados
└── moduloairflow/
    ├── Dockerfile, requirements.txt
    ├── testes/test_pipeline.py  # 13 testes de unidade
    └── dags/
        ├── pipeline_demanda_dataco.py
        ├── dados/               # CSV do DataCo (baixar do Kaggle)
        └── dataco/              # config, db, ingestao, features, validacao,
                                 # modelagem, treino, promocao, previsao
```
