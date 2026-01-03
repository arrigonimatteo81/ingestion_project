import unittest

from helpers.query_resolver import QueryContext, QueryRenderer


class TestQueryRenderer(unittest.TestCase):
    def test_return_correct_query_with_all_parameters(self):
        query_ctx = QueryContext(
                    cod_abi=3239,
                    cod_provenienza="PR",
                    num_periodo_rif=202510,
                    cod_colonna_valore="VALORE_01",
                    num_ambito=1,
                    num_max_data_va=20251231151000,
                )
        query_text = QueryRenderer.render("SELECT BANCA as NUM_BANCA, LTRIM(RTRIM(COD_UO)) as COD_UO, LTRIM(RTRIM(NUM_PARTITA)) as COD_NUM_PARTITA,LTRIM(RTRIM(COD_PRODOTTO)) as COD_PRODOTTO,"
                "LTRIM(RTRIM(TIPO_OPERAZIONE)) as COD_TIPO_OPERAZIONE,LTRIM(RTRIM(CANALE)) as COD_CANALE,LTRIM(RTRIM(PRODOTTO_COMM)) as COD_PRODOTTO_COMM,LTRIM(RTRIM(PORTAFOGLIO)) as COD_PORTAFOGLIO,"
                "LTRIM(RTRIM(DESK_RENDICONTATIVO)) as COD_DESK_RENDICONTATIVO,LTRIM(RTRIM(PTF_SPECCHIO)) as COD_PTF_SPECCHIO,LTRIM(RTRIM(COD_DATO)) as cod_dato,"
                "LTRIM(RTRIM(COD_PROVN_DATI_RED)) as COD_PROVN_DATI_RED, ${num_periodo_rif} as NUM_PERIODO_RIF,${cod_colonna_valore} as IMP_VALORE, ${num_periodo_rif} as NUM_PERIODO_COMP "
                "FROM READVC with(nolock) where BANCA=${cod_abi} and COD_PROVN_DATI_RED='${cod_provenienza}' and AMBITO = ${num_ambito} and isnull(${cod_colonna_valore},0)<>0",
            query_ctx.params
        )
        query_result=("SELECT BANCA as NUM_BANCA, LTRIM(RTRIM(COD_UO)) as COD_UO, LTRIM(RTRIM(NUM_PARTITA)) as COD_NUM_PARTITA,LTRIM(RTRIM(COD_PRODOTTO)) as COD_PRODOTTO,"
            "LTRIM(RTRIM(TIPO_OPERAZIONE)) as COD_TIPO_OPERAZIONE,LTRIM(RTRIM(CANALE)) as COD_CANALE,LTRIM(RTRIM(PRODOTTO_COMM)) as COD_PRODOTTO_COMM,LTRIM(RTRIM(PORTAFOGLIO)) as COD_PORTAFOGLIO,"
            "LTRIM(RTRIM(DESK_RENDICONTATIVO)) as COD_DESK_RENDICONTATIVO,LTRIM(RTRIM(PTF_SPECCHIO)) as COD_PTF_SPECCHIO,LTRIM(RTRIM(COD_DATO)) as cod_dato,"
            "LTRIM(RTRIM(COD_PROVN_DATI_RED)) as COD_PROVN_DATI_RED, 202510 as NUM_PERIODO_RIF,VALORE_01 as IMP_VALORE, 202510 as NUM_PERIODO_COMP "
            "FROM READVC with(nolock) where BANCA=3239 and COD_PROVN_DATI_RED='PR' and AMBITO = 1 and isnull(VALORE_01,0)<>0")
        self.assertEqual(query_text,query_result)

    def test_return_correct_query_without_all_parameters(self):
        query_ctx = QueryContext(
                    cod_abi=3239,
                    cod_provenienza='AN',
                    num_periodo_rif=202510,
                    cod_colonna_valore='',
                    num_ambito=0,
                    num_max_data_va=20251231151000,
                )
        query_text = QueryRenderer.render("SELECT BANCA as NUM_BANCA, LTRIM(RTRIM(NDG)) as COD_NDG, LTRIM(RTRIM(INTESTAZIONE)) as COD_INTESTAZIONE, "
                                        "case when convert(numeric, data_va) <= 20000000 then 20010101 else coalesce(convert(numeric, data_va), 20010101)"
                                        "end as NUM_DATA_VA, DATA_INS as NUM_DATA_INS, PERIODO_RIF as NUM_PERIODO_RIF,${num_periodo_rif} as NUM_PERIODO_COMP "
                                        "FROM REAGDG_PROD with (nolock) where BANCA=${cod_abi} and "
                                        "case when convert(int, data_va) <= 20000000 then 20010101 else coalesce(convert(int, data_va), 20010101) end >= ${num_max_data_va}",
            query_ctx.params
        )
        query_result = ("SELECT BANCA as NUM_BANCA, LTRIM(RTRIM(NDG)) as COD_NDG, LTRIM(RTRIM(INTESTAZIONE)) as COD_INTESTAZIONE, "
                                        "case when convert(numeric, data_va) <= 20000000 then 20010101 else coalesce(convert(numeric, data_va), 20010101)"
                                        "end as NUM_DATA_VA, DATA_INS as NUM_DATA_INS, PERIODO_RIF as NUM_PERIODO_RIF,202510 as NUM_PERIODO_COMP "
                                        "FROM REAGDG_PROD with (nolock) where BANCA=3239 and "
                                        "case when convert(int, data_va) <= 20000000 then 20010101 else coalesce(convert(int, data_va), 20010101) end >= 20251231151000")

        self.assertEqual(query_text, query_result)