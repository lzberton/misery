import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from queries import main_query, ref_query, shipping_query
import pytz
import os
import json
from pathlib import Path

# =========================
# Environment / Config
# =========================
load_dotenv()
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

timezone = pytz.timezone("America/Sao_Paulo")

# Persistent cache file (same folder as app.py)
CACHE_FILE = Path(__file__).with_name("controle_patio_cache.json")

# Refresh cadence (15 minutes)
REFRESH_EVERY = timedelta(minutes=15)

# Streamlit autorefresh interval (ms)
AUTOREFRESH_INTERVAL_MS = 900000  # 15 min


# =========================
# DB Engine (reusable)
# =========================
@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@database.datalake.kmm.app.br:5430/datalake"
    return create_engine(url, pool_pre_ping=True)


# =========================
# DB Loaders
# =========================
@st.cache_data(ttl=900)
def load_data():
    engine = get_engine()
    with engine.connect() as conn:
        df_main = pd.read_sql(main_query, conn)
        df_ref = pd.read_sql(ref_query, conn)
        df_shipping = pd.read_sql(shipping_query, conn)

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


@st.cache_data(ttl=900)
def get_last_update():
    sql_last_update = """
    SELECT MAX(cp."DATE_UPDATE") AS last_update
    FROM manutencao.controle_patio cp;
    """
    engine = get_engine()
    with engine.connect() as conn:
        return conn.execute(text(sql_last_update)).scalar()

params = st.query_params
force_refresh_param = params.get("force") == "1"
if force_refresh_param:
    st.cache_data.clear()
    st.cache_resource.clear()
    should_refresh = True
# =========================
# Persistent Disk Cache (JSON)
# =========================
def read_persistent_cache():
    if not CACHE_FILE.exists():
        return None

    try:
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))

        rows = payload.get("rows", [])
        df_cached = pd.DataFrame(rows)

        last_update_iso = payload.get("last_update")
        last_update = pd.to_datetime(last_update_iso) if last_update_iso else None

        saved_at_iso = payload.get("saved_at")
        saved_at = pd.to_datetime(saved_at_iso) if saved_at_iso else None

        qtd_placas = payload.get("qtd_placas", 0)

        return {
            "df": df_cached,
            "last_update": last_update,
            "saved_at": saved_at,
            "qtd_placas": qtd_placas,
        }
    except Exception:
        return None


def write_persistent_cache(df_exibir, last_update, qtd_placas):
    now_sp = datetime.now(timezone)

    # Ensure serializable
    last_update_dt = None
    if last_update is not None:
        # pandas Timestamp -> python datetime
        if hasattr(last_update, "to_pydatetime"):
            last_update_dt = last_update.to_pydatetime()
        else:
            last_update_dt = last_update

    payload = {
        "saved_at": now_sp.isoformat(),
        "last_update": last_update_dt.isoformat() if last_update_dt is not None else None,
        "qtd_placas": int(qtd_placas),
        "rows": df_exibir.to_dict(orient="records"),
    }

    CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
def build_view_from_raw(df_raw: pd.DataFrame):
    now_adjusted = datetime.now(timezone)

    df = df_raw.copy()

    # Dates
    df["DATA_PREVISTA_SAIDA"] = pd.to_datetime(df["DATA_PREVISTA_SAIDA"], errors="coerce")
    df["DATA_EFETIVA_SAIDA"] = pd.to_datetime(df["DATA_EFETIVA_SAIDA"], errors="coerce")
    df["DATA_EFETIVA_ENTRADA"] = pd.to_datetime(df["DATA_EFETIVA_ENTRADA"], errors="coerce")

    # Flags (NEEDED before any calculation that uses EXISTE_SAIDA)
    df["EXISTE_PREVISAO"] = df["DATA_PREVISTA_SAIDA"].apply(
        lambda x: "COM PREVISÃO" if pd.notnull(x) else "SEM PREVISÃO"
    )
    df["EXISTE_SAIDA"] = df["DATA_EFETIVA_SAIDA"].apply(
        lambda x: "EXISTE SAÍDA" if pd.notnull(x) else "SEM SAÍDA"
    )

    # Timezone helper (NEEDED before calculations)
    def timezone_adjust(dt):
        if pd.notnull(dt) and getattr(dt, "tzinfo", None) is None:
            return timezone.localize(dt)
        return dt

    # Formatter helper (used for multiple columns)
    def format_duracao_segundos(T):
        if T is None or (isinstance(T, float) and np.isnan(T)):
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
        return f"{sinal}0min"

    # ===== NEW: Tempo desde entrada até agora (somente SEM SAÍDA) =====
    def calc_tempo_desde_entrada(row):
        if row["EXISTE_SAIDA"] != "SEM SAÍDA":
            return None

        entrada = row["DATA_EFETIVA_ENTRADA"]
        if pd.isnull(entrada):
            return None

        entrada = timezone_adjust(entrada)
        return int((now_adjusted - entrada).total_seconds())

    df["TEMPO_DESDE_ENTRADA"] = df.apply(calc_tempo_desde_entrada, axis=1)
    df["TEMPO_ENTRADA_ATE_AGORA"] = df["TEMPO_DESDE_ENTRADA"].apply(format_duracao_segundos)
    # ================================================================

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
        return "BAIXA"

    df["PRIORIDADE"] = df.apply(definir_prioridade, axis=1)

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
        origem = row.get("PAIS_ORIGEM_SHIPPING")
        destino = row.get("PAIS_DESTINO_SHIPPING")

        if pd.isnull(origem) or pd.isnull(destino):
            return None
        if origem == destino:
            return "NAC"
        elif destino == "Brasil":
            return "RN"
        else:
            return "RS"

    df["RUMO"] = df.apply(classificar_rumo, axis=1)

    # Reference uppercase
    if "REFERENCIA" in df.columns:
        df["REFERENCIA"] = df["REFERENCIA"].astype("string").str.upper()

    # Motorista -> first name uppercase
    if "MOTORISTA" in df.columns:
        s = df["MOTORISTA"]
        df["MOTORISTA"] = (
            s.where(s.notna())
            .astype("string")
            .str.strip()
            .str.upper()
            .str.split()
            .str[0]
        )

    # Filter
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
        "TEMPO_ENTRADA_ATE_AGORA",
        "DATA_PREVISTA_SAIDA",
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
        "TEMPO_ENTRADA_ATE_AGORA": "TEMPO PATIO",
        "DATA_PREVISTA_SAIDA": "PREVISÃO SAÍDA",
        "TEMPO_FORMATADO": "TEMPO P/ SAÍDA",
        "PRIORIDADE": "PRIORIDADE",
        "MOTORISTA": "MOTORISTA",
        "REFERENCIA": "REFERÊNCIA ATUAL",
    }

    df_exibir = df_filtrado[colunas_exibir].rename(columns=nomes_alterados).copy()

    # Format dates for display
    df_exibir["ENTRADA"] = pd.to_datetime(df_exibir["ENTRADA"], errors="coerce").dt.strftime(
        "%d/%m/%y %H:%M"
    )
    df_exibir["PREVISÃO SAÍDA"] = pd.to_datetime(
        df_exibir["PREVISÃO SAÍDA"], errors="coerce"
    ).dt.strftime("%d/%m/%y %H:%M")

    # Sort by PREVISÃO SAÍDA safely
    sort_key = pd.to_datetime(df_exibir["PREVISÃO SAÍDA"], format="%d/%m/%y %H:%M", errors="coerce")
    df_exibir["_sort"] = sort_key
    df_exibir = df_exibir.sort_values("_sort").drop(columns="_sort")

    df_exibir = df_exibir.fillna("").replace("None", "")

    qtd_placas = df_filtrado["PLACA"].nunique()

    return df_exibir, qtd_placas


def top_bar(last_update):
    # last_update can be None / Timestamp / datetime
    if last_update is None or (isinstance(last_update, float) and np.isnan(last_update)):
        last_update_str = "-"
    else:
        if hasattr(last_update, "to_pydatetime"):
            last_update = last_update.to_pydatetime()
        last_update_str = last_update.strftime("%d/%m/%Y %H:%M:%S")

    st.markdown(
        f"""
    <div style='
        background-color: #f0f2f6;
        padding: 2px 8px;
        border-radius: 10px;
        position: relative;
        display: flex;
        align-items: center;
        justify-content: space-between;
    '>
        <div style='flex: 1;'>
            <img src="https://letsara.com/wp-content/uploads/2024/11/Letsara-Aplicacao-principal-horizontal.png" style="width: 180px;">
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
            <strong>ÚLTIMA ATUALIZAÇÃO:</strong> {last_update_str}
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )
def inject_colgroup_widths(html: str, widths_px: list[int]) -> str:
    # Build <colgroup> with fixed widths
    colgroup = "<colgroup>" + "".join([f"<col style='width:{w}px'>" for w in widths_px]) + "</colgroup>"

    # Insert colgroup right after the opening <table ...> tag
    # Find the first occurrence of "<table" and the following ">"
    i = html.find("<table")
    if i == -1:
        return html
    j = html.find(">", i)
    if j == -1:
        return html

    return html[: j + 1] + colgroup + html[j + 1 :]

def estilo_personalizado(df):
    def line_color(row):
        cores = {"CRÍTICA": "#FF0000A0", "URGÊNCIA": "#F87474A9", "ATENÇÃO": "#F6F93FAC"}
        cor = cores.get(row.get("PRIORIDADE", ""), "white")
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
                "color": "#434343",
            },
        )

    styled = styled.set_properties(
        subset=[ultima_coluna],
        **{"text-align": "left", "vertical-align": "middle", "font-weight": "bold","color": "#434343"},
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
    styled = styled.set_table_styles(
    [
        # Header cells (column names)
        {
            "selector": "thead th",
            "props": [
                ("color", "#434343"),
                ("background-color", "#f0f2f6"),
                ("text-align", "center"),
                ("font-weight", "bold"),
            ],
        },
    ],
    overwrite=False,
    )

    styled = styled.hide(axis="index")
    return styled


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
.tabela-custom th {
    color: #434343 !important;
    background-color: #f0f2f6 !important;
}

.tabela-custom td {
    color: #434343 !important;
}

/* Let the table be as wide as needed and scroll horizontally */
.tabela-custom table {
    border-collapse: collapse;
    table-layout: fixed !important;
    width: max-content !important;
    font-family: Arial, sans-serif;
    font-size: 22px;
    text-align: center;
}

.tabela-custom th, .tabela-custom td {
    padding: 4px;
    border: 1px solid #ddd;
    font-weight: bold;
    color: #434343;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    text-align: center;
}

@media (forced-colors: active) {
  .tabela-custom th, .tabela-custom td {
    forced-color-adjust: none;
    color: #434343 !important;
    background-color: white !important;
  }
}
</style>
"""

# =========================
# Streamlit page setup
# =========================
st.set_page_config(page_title="Monitoramento Pátio", layout="wide")

st.markdown(
    """
    <style>
        header, footer { visibility: hidden; }
        .main > div:first-child { padding-top: 0rem; }

        .block-container {
            padding-top: 0rem;
            padding-bottom: 0rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }

        .stApp { background-color: white; }
        .block-container { background-color: white; }
    </style>
""",
    unsafe_allow_html=True,
)

# Refresh page every 15 minutes (forces rerun)
st_autorefresh(interval=AUTOREFRESH_INTERVAL_MS, key="auto-refresh")

# Placeholders (render cached immediately, then overwrite if DB refresh completes)
ph_top = st.empty()
ph_kpi = st.empty()
ph_table = st.empty()
ph_status = st.empty()


def render_screen(df_exibir, last_update, qtd_placas):
    with ph_top:
        top_bar(last_update)

    with ph_kpi:
        st.markdown("<div style='margin: 10px 0;'></div>", unsafe_allow_html=True)
        st.markdown(
            f"""
            <div style='
                background-color:#f0f2f6;
                padding:6px;
                border-radius:10px;
                text-align: center;
                color: #434343;
            '>
                <h4>Total de Saída Previstas: {qtd_placas}</h4>
            </div>
        """,
            unsafe_allow_html=True,
        )
        st.markdown("<div style='margin: 5px 0;'></div>", unsafe_allow_html=True)

    with ph_table:
        st.markdown(scroll_style, unsafe_allow_html=True)
        styled_df = estilo_personalizado(df_exibir)
        html = styled_df.to_html(index=False, escape=False)

        # Column widths must match the current column order in df_exibir
        widths = [110, 110, 110, 200, 90, 150,150, 170, 150, 140, 160, 350]
        html = inject_colgroup_widths(html, widths)

        st.markdown(f"<div class='tabela-custom'>{html}</div>", unsafe_allow_html=True)


# =========================
# 1) Show persistent cache immediately (if present)
# =========================
cached = read_persistent_cache()

if cached and cached["df"] is not None and not cached["df"].empty:
    render_screen(cached["df"], cached["last_update"], cached["qtd_placas"])

    saved_at = cached.get("saved_at")
    if saved_at is not None and hasattr(saved_at, "to_pydatetime"):
        saved_at_dt = saved_at.to_pydatetime()
    else:
        saved_at_dt = saved_at

    with ph_status:
        st.caption(
            f"Exibindo cache persistente (JSON) salvo em: "
            f"{saved_at_dt.strftime('%d/%m/%Y %H:%M:%S') if saved_at_dt else '-'}"
        )
else:
    with ph_status:
        st.caption("Sem cache persistente ainda. Carregando do banco...")

# Decide if we should refresh from DB (based on cache age)
should_refresh = True
if cached and cached.get("saved_at") is not None:
    saved_at = cached["saved_at"]
    if hasattr(saved_at, "to_pydatetime"):
        saved_at = saved_at.to_pydatetime()

    try:
        # If saved_at is naive, treat as Sao Paulo
        if getattr(saved_at, "tzinfo", None) is None:
            saved_at = timezone.localize(saved_at)
    except Exception:
        pass

    should_refresh = (datetime.now(timezone) - saved_at) >= REFRESH_EVERY

# =========================
# 2) If stale or missing, refresh from DB and overwrite JSON
# =========================
if should_refresh:
    try:
        with ph_status:
            st.caption("Atualizando do banco de dados e salvando cache persistente...")

        df_raw = load_data()  # can be slow
        df_exibir, qtd_placas = build_view_from_raw(df_raw)
        last_update = get_last_update()

        # Render fresh data
        render_screen(df_exibir, last_update, qtd_placas)

        # Persist to disk
        write_persistent_cache(df_exibir, last_update, qtd_placas)

        with ph_status:
            st.caption(
                f"Atualizado e salvo em disco em: "
                f"{datetime.now(timezone).strftime('%d/%m/%Y %H:%M:%S')} "
                f"({str(CACHE_FILE)})"
            )

    except Exception as e:
        with ph_status:
            st.error("Falha ao atualizar do banco. Veja o erro abaixo:")
        st.exception(e) 
        # Uncomment for debugging:
        # st.exception(e)
