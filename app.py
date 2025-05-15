import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pytz
import os
load_dotenv()
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

@st.cache_resource
def conectar_postgres():
    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@database.datalake.kmm.app.br:5430/datalake"
    engine = create_engine(url)
    return engine

@st.cache_data(ttl=300)
def carregar_dados():
    engine = conectar_postgres()

    query_principal = """
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

    query_ref = """
        SELECT DISTINCT rf."PLACA_CONTROLE", rf."REFERENCIA"
        FROM oper.rank_frota rf
        WHERE rf."DIA" IS NOT NULL;
    """

    query_shipping = """
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

    # Executar consultas
    df_principal = pd.read_sql(query_principal, engine)
    df_ref = pd.read_sql(query_ref, engine)
    df_shipping = pd.read_sql(query_shipping, engine)

    # Merge com rank_frota
    df_merge = pd.merge(df_principal, df_ref, how="left", left_on="PLACA", right_on="PLACA_CONTROLE")

    # Merge com shipping (ROMANEIO_ATUAL x NUM_ROMANEIO)
    df_final = pd.merge(df_merge, df_shipping, how="left", left_on="NUM_ROMANEIO", right_on="ROMANEIO_ATUAL")

    return df_final


    # App Streamlit
st.set_page_config(page_title="Monitoramento Pátio", layout="wide")
st.markdown("""
    <style>
        /* Remove o cabeçalho e o rodapé padrão do Streamlit */
        header, footer {
            visibility: hidden;
        }

        /* Reduz margem/padding superior */
        .main > div:first-child {
            padding-top: 0rem;
        }

        /* Opcional: remover espaçamento lateral */
        .block-container {
            padding-top: 0rem;
            padding-bottom: 0rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
    </style>
""", unsafe_allow_html=True)
timezone = pytz.timezone("America/Sao_Paulo")
now_adjusted = datetime.now(timezone)
def barra_superior():
    st.markdown(f"""
    <div style='
        background-color: #f0f2f6;
        padding: 15px 30px;
        border-radius: 10px;
        position: relative;
        display: flex;
        align-items: center;
        justify-content: space-between;
    '>
        <div style='flex: 1;'>
            <img src="https://letsara.com.br/imagens/letsara-brand.png" style="width: 180px;">
        </div>
        <div style='
            position: absolute;
            left: 50%;
            transform: translateX(-50%);
            text-align: center;
        '>
            <h2 style='margin: 0;'>MONITORAMENTO DE SAÍDA DE PÁTIO</h2>
        </div>
        <div style='flex: 1; text-align: right;'>
            <strong>ÚLTIMA ATUALIZAÇÃO:</strong> {now_adjusted.strftime('%d/%m/%Y %H:%M:%S')}
        </div>
    </div>
    """, unsafe_allow_html=True)

df = carregar_dados()
df["DATA_PREVISTA_SAIDA"] = pd.to_datetime(df["DATA_PREVISTA_SAIDA"], errors="coerce")
df["DATA_EFETIVA_SAIDA"] = pd.to_datetime(df["DATA_EFETIVA_SAIDA"], errors="coerce")
df["EXISTE_PREVISAO"] = df["DATA_PREVISTA_SAIDA"].apply(
    lambda x: "COM PREVISÃO" if pd.notnull(x) else "SEM PREVISÃO"
)
df["EXISTE_SAIDA"] = df["DATA_EFETIVA_SAIDA"].apply(
    lambda x: "EXISTE SAÍDA" if pd.notnull(x) else "SEM SAÍDA"
)


def ajustar_timezone(dt):
    """Garante que o datetime tenha o timezone de Brasília usando pytz"""
    if pd.notnull(dt) and dt.tzinfo is None:
        return timezone.localize(dt)
    return dt

def calcular_tempo_saida(row):
    # Ajusta os datetimes para timezone de Brasília
    data_prevista = ajustar_timezone(row["DATA_PREVISTA_SAIDA"])
    data_efetiva = ajustar_timezone(row["DATA_EFETIVA_SAIDA"])

    if row["EXISTE_SAIDA"] == "EXISTE SAÍDA":
        referencia = data_prevista if pd.notnull(data_prevista) else data_efetiva
        if pd.notnull(referencia) and pd.notnull(data_efetiva):
            return int((referencia - data_efetiva).total_seconds())
    elif pd.notnull(data_prevista):
        return int((data_prevista - now_adjusted).total_seconds()) 
    return None

df["TEMPO_ATE_SAIDA"] = df.apply(calcular_tempo_saida, axis=1)
def definir_prioridade(row):
    if pd.isnull(row["DATA_PREVISTA_SAIDA"]):
        return "BAIXA"
    tempo = row["TEMPO_ATE_SAIDA"]
    if tempo is None:
        return "BAIXA"
    elif tempo > 7200:
        return "NORMAL"
    elif 1800 < tempo <= 7200:
        return "ATENÇÃO"
    elif 0 < tempo <= 1800:
        return "URGÊNCIA"
    elif tempo < 0:
        return "CRÍTICA"
    return None

df["PRIORIDADE"] = df.apply(definir_prioridade, axis=1)
def formatar_tempo_h(row):
    T = row["TEMPO_ATE_SAIDA"]
    if pd.isnull(T) or T is None:
        return None
    T = int(T)
    abs_t = abs(T)
    horas = abs_t // 3600
    minutos = (abs_t % 3600) // 60
    resultado = horas * 100 + minutos
    return -resultado if T < 0 else resultado

df["TEMPO_ATE_SAIDA_H"] = df.apply(formatar_tempo_h, axis=1)
def formatar_tempo_legivel(row):
    T = row["TEMPO_ATE_SAIDA"]
    if pd.isnull(T) or T is None:
        return ""
    
    T = int(T)
    sinal = "-" if T < 0 else ""
    T = abs(T)

    horas = T // 3600
    minutos = (T % 3600) // 60

    if horas > 0 and minutos > 0:
        return f"{sinal}{horas}h {minutos}min"
    elif horas > 0:
        return f"{sinal}{horas}h"
    elif minutos > 0:
        return f"{sinal}{minutos}min"
    else:
        return f"{sinal}0min"

df["TEMPO_FORMATADO"] = df.apply(formatar_tempo_legivel, axis=1)
def classificar_rumo(row):
    origem = row["PAIS_ORIGEM_SHIPPING"]
    destino = row["PAIS_DESTINO_SHIPPING"]
    
    if pd.isnull(origem) or pd.isnull(destino):
        return None
    if origem == destino:
        return "NAC"
    elif destino == "Brasil":
        return "RN"
    else:
        return "RS"

df["RUMO"] = df.apply(classificar_rumo, axis=1)

df_filtrado = df[
    (df["EXISTE_SAIDA"] == "SEM SAÍDA") &
    (df["SITUACAO_ID"].isin([2, 3])) &
    (df["DATA_PREVISTA_SAIDA"].notna())
]
colunas_exibir = [
    "PLACA",
    "PLACA_2",
    "PLACA_3",
    "NEGOCIADOR",
    "RUMO",
    "DATA_EFETIVA_ENTRADA",
    "DATA_PREVISTA_SAIDA",
    "DATA_EFETIVA_SAIDA",
    "TEMPO_FORMATADO",
    "PRIORIDADE",
    "REFERENCIA"
]
nomes_alterados = {
    "PLACA": "CAVALO",
    "PLACA_2": "CARRETA",
    "PLACA_3": "2ª CARRETA",
    "NEGOCIADOR":"NEGOCIADOR",
    "RUMO":"RUMO",
    "DATA_EFETIVA_ENTRADA":"ENTRADA",
    "DATA_PREVISTA_SAIDA":"PREVISÃO SAÍDA",
    "DATA_EFETIVA_SAIDA":"SAÍDA",
    "TEMPO_FORMATADO":"TEMPO ATÉ SAÍDA",
    "PRIORIDADE":"PRIORIDADE",
    "REFERENCIA":"REFERÊNCIA ATUAL"
}

def aplicar_estilo_prioridade(df):
    def colorir_linha(row):
        cor = "white"
        if row["PRIORIDADE"] == "CRÍTICA":
            cor = "#ff4d4f"  # vermelho forte
        elif row["PRIORIDADE"] == "URGÊNCIA":
            cor = "#ffa39e"  # vermelho claro
        elif row["PRIORIDADE"] == "ATENÇÃO":
            cor = "#fff566"  # amarelo
        return [f"background-color: {cor}; font-weight: bold;" for _ in row]

    return df.style.apply(colorir_linha, axis=1)
barra_superior()
df_exibir = df_filtrado[colunas_exibir].rename(columns=nomes_alterados)
df_exibir["SAÍDA"] = df_exibir["SAÍDA"].dt.strftime("%d/%m/%Y %H:%M")
df_exibir["SAÍDA"] = df_exibir["SAÍDA"].fillna("").replace("NaT", "")
df_exibir["ENTRADA"] = df_exibir["ENTRADA"].dt.strftime("%d/%m/%y %H:%M")
df_exibir["PREVISÃO SAÍDA"] = df_exibir["PREVISÃO SAÍDA"].dt.strftime("%d/%m/%y %H:%M")
qtd_placas = df_filtrado["PLACA"].nunique()
st.markdown("<div style='margin: 20px 0;'></div>", unsafe_allow_html=True)
st.markdown(f"""
    <div style='
        background-color:#f0f2f6;
        padding:10px;
        border-radius:10px;
        text-align: center;
    '>
        <h4>Total de Saída Previstas: {qtd_placas}</h4>
    </div>
""", unsafe_allow_html=True)
# Substitui NaN/None por string vazia
st.markdown("<div style='margin: 10px 0;'></div>", unsafe_allow_html=True)
df_exibir = df_exibir.fillna("")
# Aplica estilos visuais
def estilo_personalizado(df):
    def colorir_linha(row):
        cores = {
            "CRÍTICA": "#FF0000",
            "URGÊNCIA": "#F87474",
            "ATENÇÃO": "#F6F93F"
        }
        cor = cores.get(row["PRIORIDADE"], "white")
        return [f"background-color: {cor}" for _ in row]

    # Oculta os 'None' e trata valores nulos
    df = df.fillna("").replace("None", "")

    styled = df.style.apply(colorir_linha, axis=1)

    # Garante alinhamento com estilo visual
    colunas_centralizar = df.columns[:-1]
    ultima_coluna = df.columns[-1]

    for col in colunas_centralizar:
        styled = styled.set_properties(subset=[col], **{
            'text-align': 'center',
            'vertical-align': 'middle',
            'font-weight': 'bold'
        })

    styled = styled.set_properties(subset=[ultima_coluna], **{
        'text-align': 'left',
        'vertical-align': 'middle',
        'font-weight': 'bold'
    })
    styled = styled.set_table_styles([
        {'selector': 'td', 'props': [('font-family', 'Arial, sans-serif'), ('font-weight', 'bold')]}
    ])
    # Oculta o índice
    styled = styled.hide(axis="index")
    return styled



# Exibe a tabela sem índice
df_exibir = df_exibir.sort_values(by="PREVISÃO SAÍDA")  # <-- Ordena aqui
# Aplica estilo e converte para HTML
styled_df = estilo_personalizado(df_exibir.fillna(""))
html = styled_df.to_html(index=False, escape=False)

# Estilo CSS para rolagem e aparência
scroll_style = """
<style>
.tabela-custom {
    max-height: 850px;
    overflow-y: auto;
    overflow-x: auto;
    display: block;
    border: 1px solid #ccc;
    border-radius: 10px;
}
.tabela-custom table {
    border-collapse: collapse;
    width: 100%;
    font-family: Arial, sans-serif;
    font-size: 14px;
}
.tabela-custom th, .tabela-custom td {
    text-align: center;
    padding: 8px;
    border: 1px solid #ddd;
    font-weight: bold;
}
</style>
"""

# Renderiza com scroll e estilo
st.markdown(scroll_style, unsafe_allow_html=True)
st.markdown(f"<div class='tabela-custom'>{html}</div>", unsafe_allow_html=True)
st_autorefresh(interval=450000, key="refresh")