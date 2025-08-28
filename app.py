import streamlit as st
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from sqlalchemy import create_engine
from dotenv import load_dotenv
from queries import main_query, ref_query, shipping_query
import pytz
import os

load_dotenv()
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")


@st.cache_resource
def connect_db():
    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@database.datalake.kmm.app.br:5430/datalake"
    engine = create_engine(url)
    return engine


@st.cache_data(ttl=300)
def load_data():
    engine = connect_db()

    df_main = pd.read_sql(main_query, engine)
    df_ref = pd.read_sql(ref_query, engine)
    df_shipping = pd.read_sql(shipping_query, engine)
    df_merge = pd.merge(
        df_main, df_ref, how="left", left_on="PLACA", right_on="PLACA_CONTROLE"
    )

    df_final = pd.merge(
        df_merge,
        df_shipping,
        how="left",
        left_on="NUM_ROMANEIO",
        right_on="ROMANEIO_ATUAL",
    )

    return df_final


st.set_page_config(page_title="Monitoramento Pátio", layout="wide")
st.markdown(
    """
    <style>
        header, footer {
            visibility: hidden;
        }
        .main > div:first-child {
            padding-top: 0rem;
        }

        .block-container {
            padding-top: 0rem;
            padding-bottom: 0rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        .stApp {
            background-color: white;
        }
        .block-container {
            background-color: white;
        }

    </style>
""",
    unsafe_allow_html=True,
)

timezone = pytz.timezone("America/Sao_Paulo")
now_adjusted = datetime.now(timezone)


def top_bar():
    st.markdown(
        f"""
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
            <h2 style='margin: 0; color:black'>MONITORAMENTO DE SAÍDA DE PÁTIO</h2>
        </div>
        <div style='flex: 1; text-align: right; color:#434343'>
            <strong>ÚLTIMA ATUALIZAÇÃO:</strong> {now_adjusted.strftime('%d/%m/%Y %H:%M:%S')}
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )


df = load_data()
df["DATA_PREVISTA_SAIDA"] = pd.to_datetime(df["DATA_PREVISTA_SAIDA"], errors="coerce")
df["DATA_EFETIVA_SAIDA"] = pd.to_datetime(df["DATA_EFETIVA_SAIDA"], errors="coerce")
df["EXISTE_PREVISAO"] = df["DATA_PREVISTA_SAIDA"].apply(
    lambda x: "COM PREVISÃO" if pd.notnull(x) else "SEM PREVISÃO"
)
df["EXISTE_SAIDA"] = df["DATA_EFETIVA_SAIDA"].apply(
    lambda x: "EXISTE SAÍDA" if pd.notnull(x) else "SEM SAÍDA"
)


def timezone_adjust(dt):
    if pd.notnull(dt) and dt.tzinfo is None:
        return timezone.localize(dt)
    return dt


def calc_tempo_saida(row):
    data_prevista = timezone_adjust(row["DATA_PREVISTA_SAIDA"])
    data_efetiva = timezone_adjust(row["DATA_EFETIVA_SAIDA"])

    if row["EXISTE_SAIDA"] == "EXISTE SAÍDA":
        referencia = data_prevista if pd.notnull(data_prevista) else data_efetiva
        if pd.notnull(referencia) and pd.notnull(data_efetiva):
            return int((referencia - data_efetiva).total_seconds())
    elif pd.notnull(data_prevista):
        return int((data_prevista - now_adjusted).total_seconds())
    return None


df["TEMPO_ATE_SAIDA"] = df.apply(calc_tempo_saida, axis=1)


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


def format_tempo_h(row):
    T = row["TEMPO_ATE_SAIDA"]
    if pd.isnull(T) or T is None:
        return None
    T = int(T)
    abs_t = abs(T)
    horas = abs_t // 3600
    minutos = (abs_t % 3600) // 60
    resultado = horas * 100 + minutos
    return -resultado if T < 0 else resultado


df["TEMPO_ATE_SAIDA_H"] = df.apply(format_tempo_h, axis=1)


def format_time(row):
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


df["TEMPO_FORMATADO"] = df.apply(format_time, axis=1)


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
df["REFERENCIA"] = df["REFERENCIA"].str.upper()
df["MOTORISTA"] = df["MOTORISTA"].str.upper()
df_filtrado = df[
    (df["EXISTE_SAIDA"] == "SEM SAÍDA")
    & (df["SITUACAO_ID"].isin([2, 3]))
    & (df["DATA_PREVISTA_SAIDA"].notna())
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
    "MOTORISTA",
    "REFERENCIA",
]
nomes_alterados = {
    "PLACA": "CAVALO",
    "PLACA_2": "CARRETA",
    "PLACA_3": "2ª CARRETA",
    "NEGOCIADOR": "NEGOCIADOR",
    "RUMO": "RUMO",
    "DATA_EFETIVA_ENTRADA": "ENTRADA",
    "DATA_PREVISTA_SAIDA": "PREVISÃO SAÍDA",
    "DATA_EFETIVA_SAIDA": "SAÍDA",
    "TEMPO_FORMATADO": "TEMPO ATÉ SAÍDA",
    "PRIORIDADE": "PRIORIDADE",
    "MOTORISTA":"MOTORISTA",
    "REFERENCIA": "REFERÊNCIA ATUAL",
}
def apply_priority_style(df):
    def line_color(row):
        cor = "white"
        if row["PRIORIDADE"] == "CRÍTICA":
            cor = "#ff4d4f"  # vermelho forte
        elif row["PRIORIDADE"] == "URGÊNCIA":
            cor = "#ffa39e"  # vermelho claro
        elif row["PRIORIDADE"] == "ATENÇÃO":
            cor = "#fff566"  # amarelo
        return [f"background-color: {cor}; font-weight: bold;" for _ in row]

    return df.style.apply(line_color, axis=1)


top_bar()
df_exibir = df_filtrado[colunas_exibir].rename(columns=nomes_alterados)
df_exibir["SAÍDA"] = df_exibir["SAÍDA"].dt.strftime("%d/%m/%Y %H:%M")
df_exibir["SAÍDA"] = df_exibir["SAÍDA"].fillna("").replace("NaT", "")
df_exibir["ENTRADA"] = df_exibir["ENTRADA"].dt.strftime("%d/%m/%y %H:%M")
df_exibir["PREVISÃO SAÍDA"] = df_exibir["PREVISÃO SAÍDA"].dt.strftime("%d/%m/%y %H:%M")
qtd_placas = df_filtrado["PLACA"].nunique()
st.markdown("<div style='margin: 20px 0;'></div>", unsafe_allow_html=True)
st.markdown(
    f"""
    <div style='
        background-color:#f0f2f6;
        padding:10px;
        border-radius:10px;
        text-align: center;
        color: #434343;
    '>
        <h4>Total de Saída Previstas: {qtd_placas}</h4>
    </div>
""",
    unsafe_allow_html=True,
)
st.markdown("<div style='margin: 10px 0;'></div>", unsafe_allow_html=True)
df_exibir = df_exibir.fillna("")


def estilo_personalizado(df):
    def line_color(row):
        cores = {"CRÍTICA": "#FF0000", "URGÊNCIA": "#F87474", "ATENÇÃO": "#F6F93F"}
        cor = cores.get(row["PRIORIDADE"], "white")
        return [f"background-color: {cor}" for _ in row]

    df = df.fillna("").replace("None", "")

    styled = df.style.apply(line_color, axis=1)

    colunas_centralizar = df.columns[:-1]
    ultima_coluna = df.columns[-1]

    for col in colunas_centralizar:
        styled = styled.set_properties(
            subset=[col],
            **{
                "text-align": "center",
                "vertical-align": "middle",
                "font-weight": "bold",
                "color":"#434343",
            },
        )

    styled = styled.set_properties(
        subset=[ultima_coluna],
        **{"text-align": "left", "vertical-align": "middle", "font-weight": "bold"},
    )
    styled = styled.set_table_styles(
        [
            {
                "selector": "td",
                "props": [
                    ("font-family", "Arial, sans-serif"),
                    ("font-weight", "bold"),
                ],
            }
        ]
    )

    styled = styled.hide(axis="index")
    return styled

df_exibir["PREVISÃO SAÍDA"] = pd.to_datetime(df_exibir["PREVISÃO SAÍDA"], format="%d/%m/%y %H:%M")
df_exibir = df_exibir.sort_values(by="PREVISÃO SAÍDA")
df_exibir["PREVISÃO SAÍDA"] = df_exibir["PREVISÃO SAÍDA"].dt.strftime("%d/%m/%Y %H:%M")
styled_df = estilo_personalizado(df_exibir.fillna(""))
html = styled_df.to_html(index=False, escape=False)
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
    color: #434343;
}
</style>
"""
st.markdown(scroll_style, unsafe_allow_html=True)
st.markdown(f"<div class='tabela-custom'>{html}</div>", unsafe_allow_html=True)
st_autorefresh(interval=450000, key="auto-refresh")
