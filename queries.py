main_query = """
        WITH cpv AS (
            SELECT
                cp."DATE_INSERT",
                cp."CONTROLE_PATIO_ID",
                cp."DATA_PREVISTA_ENTRADA",
                cp."DATA_PREVISTA_SAIDA",
                cp."DATA_EFETIVA_ENTRADA",
                cp."DATA_EFETIVA_SAIDA",
                cp."SITUACAO_ID",
                cp."NUM_ROMANEIO",
                cp."EQUIPAMENTO_ID",
                e."COD_EQUIPAMENTO" AS "PLACA"
            FROM manutencao.controle_patio cp
            JOIN almoxarifado.equipamento e
              ON cp."EQUIPAMENTO_ID" = e."EQUIPAMENTO_ID"
            WHERE cp."DATE_INSERT" >= '2024-08-01'
        ),
        completo AS (
            SELECT cpv.*, vc."PLACA_2"
            FROM cpv
            LEFT JOIN veiculo.veiculo_composicao vc
              ON cpv."PLACA" = vc."PLACA_1"
             AND cpv."DATA_EFETIVA_ENTRADA" BETWEEN vc."DATA_HORA_ENGATE" AND COALESCE(vc."DATA_HORA_DESENGATE", NOW())
        )
        SELECT completo.*, vc."PLACA_2" AS "PLACA_3"
        FROM completo
        LEFT JOIN veiculo.veiculo_composicao vc
          ON completo."PLACA_2" = vc."PLACA_1"
         AND completo."DATA_EFETIVA_ENTRADA" BETWEEN vc."DATA_HORA_ENGATE" AND COALESCE(vc."DATA_HORA_DESENGATE", NOW());
    """
ref_query = """
        SELECT DISTINCT rf."PLACA_CONTROLE", rf."REFERENCIA"
        FROM oper.rank_frota rf
        WHERE rf."DIA" IS NOT NULL;
    """
shipping_query = """
        WITH CombinedData AS (
            SELECT
                "SHIPPING_CODE_ID",
                "NEGOCIADOR",
                "PAIS_ORIGEM_SHIPPING",
                "PAIS_DESTINO_SHIPPING",
                "ROMANEIO_ATUAL",
                'TSC' AS "TABELA"
            FROM customizacoes_932.tracking_shipping_code
            UNION ALL 
            SELECT
                "SHIPPING_CODE_ID",
                "NEGOCIADOR",
                "PAIS_ORIGEM_SHIPPING",
                "PAIS_DESTINO_SHIPPING",
                "ROMANEIO_ATUAL",
                'TSCH' AS "TABELA"
            FROM customizacoes_932.tracking_shipping_code_historico
            WHERE "DATA_INICIO_CARGA" >= '2024-08-01'
            UNION ALL
            SELECT
                "SHIPPING_CODE_ID",
                "NEGOCIADOR",
                "PAIS_ORIGEM_SHIPPING",
                "PAIS_DESTINO_SHIPPING",
                "ROMANEIO_ATUAL",
                'TSCSR' AS "TABELA"
            FROM customizacoes_932.tracking_shipping_code_sem_romaneio
            WHERE "DATA_INICIO_CARGA" >= '2024-08-01'
        ),
        RankedData AS (
            SELECT
                "SHIPPING_CODE_ID",
                "NEGOCIADOR",
                "PAIS_ORIGEM_SHIPPING",
                "PAIS_DESTINO_SHIPPING",
                "ROMANEIO_ATUAL",
                "TABELA",
                ROW_NUMBER() OVER (PARTITION BY "ROMANEIO_ATUAL" ORDER BY "TABELA") AS rn
            FROM CombinedData
        )
        SELECT
            "SHIPPING_CODE_ID",
            "NEGOCIADOR",
            "PAIS_ORIGEM_SHIPPING",
            "PAIS_DESTINO_SHIPPING",
            "ROMANEIO_ATUAL",
            "TABELA"
        FROM RankedData
        WHERE rn = 1 AND "ROMANEIO_ATUAL" IS NOT NULL
        ORDER BY "ROMANEIO_ATUAL";
    """
