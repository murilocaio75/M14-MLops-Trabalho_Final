# Trabalho Final - Testes de unidade da lógica do pipeline (mesmo estilo unittest da Aula 3)
# Executar: PYTHONPATH=dags python -m unittest discover -s testes -v

import unittest

import pandas as pd

from dataco.features import calcula_features
from dataco.modelagem import alinha_colunas, prepara_X_y, separa_periodos
from dataco.promocao import decide_promocao
from dataco.validacao import valida_features


# Demanda semanal sintética: 2 categorias, 80 semanas, com uma semana sem venda na categoria B
def agregado_sintetico():
    semanas = pd.date_range("2015-01-05", periods=80, freq="W-MON")
    linhas = [("A", s, 100.0 + i) for i, s in enumerate(semanas)]
    linhas += [("B", s, 50.0) for i, s in enumerate(semanas) if i != 10]
    return pd.DataFrame(linhas, columns=["categoria", "semana", "demanda"])


class TestFeatures(unittest.TestCase):

    def test_semana_sem_venda_vira_zero(self):
        features = calcula_features(agregado_sintetico(), top_n=2)
        semana_11 = pd.Timestamp("2015-01-05") + pd.Timedelta(weeks=10)
        linha = features[(features["categoria"] == "B") & (features["semana"] == semana_11)]
        self.assertEqual(linha["demanda"].item(), 0.0)

    def test_lag_usa_apenas_o_passado(self):
        features = calcula_features(agregado_sintetico(), top_n=2)
        a = features[features["categoria"] == "A"].reset_index(drop=True)
        self.assertTrue((a["lag_1"] == a["demanda"] - 1).all())

    def test_categoria_que_para_de_vender_continua_com_zero(self):
        agregado = agregado_sintetico()
        agregado = agregado[~((agregado["categoria"] == "B") & (agregado["semana"] >= "2016-01-04"))]
        features = calcula_features(agregado, top_n=2)
        b = features[features["categoria"] == "B"]
        self.assertEqual(b["semana"].max(), features["semana"].max())
        self.assertEqual(b[b["semana"] >= "2016-01-04"]["demanda"].sum(), 0.0)

    def test_top_n_filtra_categorias(self):
        features = calcula_features(agregado_sintetico(), top_n=1)
        self.assertEqual(set(features["categoria"]), {"A"})


class TestValidacao(unittest.TestCase):

    def test_dados_validos_passam(self):
        self.assertEqual(valida_features(calcula_features(agregado_sintetico(), top_n=2)), [])

    def test_demanda_negativa_reprova(self):
        features = calcula_features(agregado_sintetico(), top_n=2)
        features.loc[0, "demanda"] = -1
        self.assertIn("Existe demanda negativa", valida_features(features))

    def test_coluna_ausente_reprova(self):
        features = calcula_features(agregado_sintetico(), top_n=2).drop(columns=["lag_1"])
        self.assertTrue(valida_features(features)[0].startswith("Colunas ausentes"))


class TestModelagem(unittest.TestCase):

    def test_periodos_nao_se_sobrepoem(self):
        features = calcula_features(agregado_sintetico(), top_n=2)
        treino, validacao, futuro = separa_periodos(features, "2016-01-04")
        self.assertLess(treino["semana"].max(), validacao["semana"].min())
        self.assertLess(validacao["semana"].max(), futuro["semana"].min())

    def test_alinha_colunas_preenche_categoria_ausente(self):
        X, _ = prepara_X_y(calcula_features(agregado_sintetico(), top_n=1))
        alinhado = alinha_colunas(X, ["cat_A", "cat_Z", "lag_1"])
        self.assertEqual(list(alinhado.columns), ["cat_A", "cat_Z", "lag_1"])
        self.assertTrue((alinhado["cat_Z"] == 0).all())


class TestGate(unittest.TestCase):

    def test_primeiro_modelo_e_promovido(self):
        self.assertEqual(decide_promocao(10, 12, None)[0], "promovido")

    def test_pior_que_baseline_e_rejeitado(self):
        self.assertEqual(decide_promocao(13, 12, None)[0], "rejeitado")

    def test_pior_que_champion_e_rejeitado(self):
        self.assertEqual(decide_promocao(10, 12, 9)[0], "rejeitado")

    def test_melhor_que_champion_e_promovido(self):
        self.assertEqual(decide_promocao(8, 12, 9)[0], "promovido")


if __name__ == "__main__":
    unittest.main()
