# Case Study — Pipeline MLOps de Previsão de Demanda (DataCo Supply Chain)

**Equipe:** Caio Murilo, Gustavo Rost, Lucca Becker, Lyon Alves, Victor Franceschini
**Contexto:** Trabalho final de MLOps — Dell GenAI Academy (Unisinos), out/2026
**Stack:** Airflow · MLflow · XGBoost · Optuna · PostgreSQL · Grafana · Docker Compose · Python

> Como executar o projeto: veja o [README.md](README.md). Slides: [apresentacao/](apresentacao/).

---

## O problema

A DataCo Global (dataset público de supply chain, 180 mil pedidos) precisava prever a **demanda semanal por categoria** para apoiar a reposição de estoque — errar para cima gera estoque parado; para baixo, ruptura.

O diagnóstico de maturidade MLOps apontou **6,0 de 12**: modelos sem versionamento, deploy manual sem testes e **nenhum monitoramento** — o modelo podia piorar em silêncio por meses. Este projeto implementa as 3 ações propostas no diagnóstico, em um pipeline que roda de ponta a ponta com dados reais.

Havia ainda um agravante: o repositório de referência do case reportava **R² de 0,97**, número que investigamos antes de confiar (spoiler: era vazamento de dados — ver Aprendizados).

## A solução

Pipeline orquestrado pelo Airflow em 7 etapas, com cada decisão registrada:

```
testes_unitarios → ingere_dados → gera_features → valida_dados
    → treina_modelo → avalia_e_promove → gera_previsoes
```

- **Modelo:** XGBoost único para as 10 maiores categorias (lags de 1/2/4 semanas, média móvel, sazonalidade, one-hot de categoria), com hiperparâmetros buscados pelo Optuna.
- **Divisão sempre temporal:** treino → janela de tuning (8 sem.) → validação (12 sem.) → monitoramento (12 sem. após o corte). O Optuna nunca enxerga a janela do gate — senão a comparação com o champion seria injusta.
- **Gate de promoção:** um candidato só vira `champion` no MLflow Model Registry se tiver RMSE menor que o **baseline ingênuo** ("semana que vem repete esta") **e** que o champion atual, medidos na mesma janela. Empate não promove.
- **Governança:** cada versão carrega model card, tags (`data_corte`, `rmse_validacao`, `decisao_gate`) e **hash dos dados de treino**; toda decisão do gate vira linha na tabela `decisoes_modelo`. Seed fixa = treino reprodutível (comprovado em reexecução).
- **Qualidade:** 13 testes de unidade rodam como **primeira tarefa da DAG** — se falham, nada é treinado. Etapas idempotentes (reexecutáveis sem corromper dados).
- **Monitoramento:** previsto × real no Postgres, dashboard provisionado no Grafana (sem configuração manual).
- **Parâmetro `data_corte`** simula a passagem do tempo: rodar com cortes sucessivos demonstra retreino e envelhecimento de modelos.

## Os resultados

| Corte | RMSE candidato | RMSE baseline | Decisão do gate | Real × previsto (12 sem.) |
|---|---|---|---|---|
| 2016-06-27 (v1) | 27,9 | 36,4 | promovido | −1,0% |
| 2017-03-27 (v2) | 31,5 | 39,2 | promovido | +5,0% |
| 2017-09-25 (v3) | 26,8 | 35,8 | promovido | ruptura detectada |
| 2017-09-25 (v4) | 26,8 | 35,8 | **rejeitado ao vivo** | champion mantido |

- **20–25% menos erro que o baseline** em três épocas distintas — consistência, não sorte.
- **Modelos envelhecem, medimos isso:** o RMSE da v1 degradou +16% com o tempo (sem ninguém mexer nela); o retreino com gate existe por isso.
- **A ruptura de out/2017** (queda abrupta de vendas nos dados originais): o monitoramento acusou **na primeira semana** (MAE saltou de ≈20 para ≈215), não no fechamento do trimestre.
- **O gate funcionou em demonstração ao vivo:** rejeitou um candidato equivalente ao champion.
- Maturidade estimada após o trabalho: **6,0 → 8,5 de 12**.
- Leitura de negócio: errar 20–25% menos permite ≈20–25% menos **estoque de segurança** com o mesmo nível de serviço (fator de serviço × variabilidade do erro × √prazo).

## Os aprendizados

1. **Métrica alta demais é alerta, não conquista.** O R² de 0,97 do repositório original vinha de vazamento: o modelo recebia `Sales` (= preço × quantidade) para prever quantidade. Pergunta-padrão que levo para qualquer projeto: *"essa informação existiria antes do evento que quero prever?"*
2. **Ausência de dado também é dado.** A ruptura só apareceu no monitoramento depois de preencher com zero as semanas sem venda até o fim da base.
3. **O gate valida o passado; só o monitoramento enxerga o que vem depois do deploy.** São proteções complementares, não redundantes.
4. **Separar lógica pura de orquestração** (DAG "maestro", módulos "músicos") foi o que tornou o pipeline testável.
5. **Teste no ambiente real:** o SQLite local não pegou uma diferença da API REST do Model Registry que quebrou a primeira execução; conflitos de versão numpy/pandas só apareceram no build do container.
6. **Honestidade técnica vende melhor:** não prometemos economia em reais (não medida) nem "prever crises" (detectamos rápido). A frase de fechamento da apresentação: *"a pergunta não é se o modelo é perfeito; é se a empresa prefere descobrir um problema na primeira semana ou no fechamento do trimestre."*

---

_Dataset: DataCo Supply Chain (Kaggle, público). Projeto desenvolvido na disciplina de MLOps do programa Generative AI Academy (Dell / Unisinos), prof. Marcos Leandro Hoffmann._
