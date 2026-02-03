# DashHedge_rev.py 
# -----------------------------------------------------------------------------
from typing import List
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path               
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go  
import re
import io
from typing import Dict, List, Tuple
from plotnine import (
    ggplot, aes, geom_col, labs, theme, element_text, element_rect,
    scale_fill_manual, geom_text, geom_line, geom_point,
    scale_color_identity, geom_label
)

import io
from os import PathLike

import datetime as dt
import os

POS_FILE = "Dados/hist_posicoes_fundos.csv"
CARTEIRA_DIR = Path("Dados_Carteira")  # ← pasta que você criou
RECENT_PARQ = CARTEIRA_DIR / "carteira_recente.parquet"
TODAY_PARQ = CARTEIRA_DIR / "carteira_hoje.parquet"

# Preciso desenvolver
def _hoje() -> str:
    return dt.date.today().strftime("%Y-%m-%d")


def carregar_hist() -> pd.DataFrame:
    if os.path.exists(POS_FILE):
        return pd.read_csv(POS_FILE)
    # estrutura: Fundo • Ativo • lag • curr
    return pd.DataFrame(columns=["Fundo", "Ativo", "lag", "curr"])


def salvar_posicao(df_fundo: pd.DataFrame) -> None:
    """
    Recebe DataFrame já filtrado para 1 fundo (colunas Fundo, Ativo, Quantidade).
    Atualiza arquivo mantendo lag/curr.
    """
    hist = carregar_hist()
    fundo = df_fundo["Fundo"].iloc[0]
    hoje = _hoje()

    base = df_fundo[["Fundo", "Ativo", "Quantidade"]].copy()
    base.rename(columns={"Quantidade": "curr"}, inplace=True)
    base["lag"] = None          # placeholder – já será preenchido do hist

    # extrai linha antiga (dia anterior)
    antigo = hist[hist["Fundo"] == fundo].copy()

    # junta / atualiza lag
    if not antigo.empty:
        base = base.merge(antigo[["Fundo", "Ativo", "curr"]]
                          .rename(columns={"curr": "lag"}),
                          on=["Fundo", "Ativo"], how="left")

    # remove registros antigos desse fundo e inclui os novos
    hist = hist[hist["Fundo"] != fundo]
    hist = pd.concat([hist, base], ignore_index=True)
    hist.to_csv(POS_FILE, index=False)


def comparar_posicoes(fundo: str, ativos_atual: list[str]) -> dict[str, list[str]]:
    """
    Devolve {'faltando':[...], 'novos':[...]} comparando curr vs lag.
    """
    hist = carregar_hist()
    ant = hist[hist["Fundo"] == fundo]["Ativo"].tolist()
    falt = sorted(list(set(ant) - set(ativos_atual)))
    novos = sorted(list(set(ativos_atual) - set(ant)))
    return {"faltando": falt, "novos": novos}


def make_div1_lookup() -> pd.DataFrame:
    """

    Lê as duas tabelas de debêntures *estáticas* (deb_table_completa2 / 3),
    devolve um lookup     Ativo | DV01_UNIT
    (é barato: ~60 ms, mas cacheamos 4 h nos session_state).

    """
    if "dv01_lookup" in st.session_state:
        return st.session_state["dv01_lookup"]

    bases = []
    for csv in ["Dados/deb_table_completa2.csv",
                "Dados/deb_table_completa3.csv"]:
        df = pd.read_csv(csv)

        # nomes fixos
        df.columns = ["Evento", "Data_pgto", "Prazo_útil", "Dias_btwn",
                      "Exp_pct", "Juros_proj", "Amort", "Fluxo_desc",
                      "Ativo"]
        df["Fluxo_desc"] = (df["Fluxo_desc"].astype(str)
                            .str.replace(".", "", regex=False)
                            .str.replace(",", ".")
                            .astype(float))
        df["Prazo_útil"] = pd.to_numeric(df["Prazo_útil"], errors="coerce")

        bases.append(df[["Ativo", "Fluxo_desc", "Prazo_útil"]])

    base = (pd.concat(bases, ignore_index=True)
              .drop_duplicates("Ativo")            # 1 linha por ativo
              .assign(Ativo=lambda d: d["Ativo"].str.upper().str.strip()))

    base["DV01_UNIT"] = (
        base["Fluxo_desc"] * 0.0001 * (base["Prazo_útil"] / 252)
    )
    st.session_state["dv01_lookup"] = base[["Ativo", "DV01_UNIT"]]
    return st.session_state["dv01_lookup"]


# ───────────────────────── FUNÇÕES DE CARGA ─────────────────────────────
def load_carteira_hoje() -> pd.DataFrame:
    """Lê carteira_hoje.parquet e devolve colunas: Data, Fundo, Ativo,
       Estratégia, Quantidade (capitalização certa)."""
    
    # df = (pd.read_parquet(TODAY_PARQ)
    #        .rename(columns={"data":"Data",
    #                         "fundo":"Fundo",
    #                         "ativo":"Ativo",
    #                         "estrategia":"Estratégia",
    #                         "quantidade":"Quantidade"}))

    df = pd.read_excel('Dados/Relatório de Posição 2026-01-26.xlsx')

    # se precisar de 'Valor' em algum ponto mais à frente:
    if "Valor" not in df.columns:
        df["Valor"] = 0.0
    
    return df


def load_carteira_recent() -> pd.DataFrame:

    """
    Últimos 3 meses.  Agora já volta *com* DIV1_ATIVO diário.
    """

    df = pd.read_parquet(RECENT_PARQ).rename(columns={
        "data": "Data", "fundo": "Fundo", "ativo": "Ativo",
        "estrategia": "Estratégia", "quantidade": "Quantidade"
    })

    # ▸ transforma datas / limpa fins-de-semana
    df["Data"] = pd.to_datetime(df["Data"])
    df = df[df["Data"].dt.dayofweek < 5]           # 0-4 = dias úteis

    # ▸ DV01 unitário por ativo (lookup está em cache 4 h)
    lk = make_div1_lookup()
    df = (df.assign(Ativo=lambda d: d["Ativo"].str.upper().str.strip())
            .merge(lk, on="Ativo", how="left"))

    # ▸ se não achar o ativo na tabela, DV01_UNIT→0 (peso 0)
    df["DV01_UNIT"].fillna(0, inplace=True)

    # ▸ finalmente o peso diário
    df["DIV1_ATIVO"] = df["Quantidade"] * df["DV01_UNIT"]

    # campo Valor às vezes faz falta nas telas antigas
    if "Valor" not in df.columns:
        df["Valor"] = 0.0

    return df[["Data", "Fundo", "Ativo", "Estratégia",
               "Quantidade", "Valor", "DIV1_ATIVO"]]


###############################################################################
# CONFIGURAÇÃO GERAL
###############################################################################
st.set_page_config(
    page_title="DashHedge – Dashboard de Análise de Hedge",
    layout="wide",
    initial_sidebar_state="expanded",
)

################################################################################
# FUNÇÕES AUXILIARES – NOVAS
################################################################################


def _normalizar_ticker_dap(tk: str) -> str:
    """Converte algo como 'DAPAGO30'  →  'DAP30'. Mantém 'DAP25' intacto."""
    if not isinstance(tk, str) or not tk.startswith("DAP"):
        return None
    m = re.search(r"DAP.*?(\d{2})$", tk)
    return f"DAP{m.group(1)}" if m else None


def process_dap_counts(df_posicao_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # ---------------------------------------------------------------------
    # 1) Seleciona apenas DAPs que já são Hedge‐IPCA
    # ---------------------------------------------------------------------
    mask_dap = df_posicao_raw["Ativo"].astype(
        str).str.startswith("DAP", na=False)
    mask_estr = df_posicao_raw["Estratégia"].astype(
        str).str.contains("Hedge IPCA", na=False)
    df_dap = df_posicao_raw[mask_dap & mask_estr].copy()

    # ---------------------------------------------------------------------
    # 2) ► NOVO – extrai a quantidade que está dentro da string “Hedge IPCA (…)”
    # ---------------------------------------------------------------------
    mask_qtd = df_dap["Estratégia"].str.contains(r"Hedge IPCA\s*\(", na=False)
    df_dap.loc[mask_qtd, "Quantidade"] = (
        df_dap.loc[mask_qtd, "Estratégia"]
        # pega o número entre parênteses
        .str.extract(r"Hedge IPCA\s*\(\s*([-+]?\d+)\s*\)")[0]
        .astype(int)
    )

    # ---------------------------------------------------------------------
    # 3) Continua igual: normaliza ticker, garante numérico, faz os groupbys
    # ---------------------------------------------------------------------
    df_dap["DAP"] = df_dap["Ativo"].apply(_normalizar_ticker_dap)
    df_dap.dropna(subset=["DAP"], inplace=True)

    # caso tenha linhas cujo “Quantidade” ainda esteja vazio, zera-as
    df_dap["Quantidade"] = pd.to_numeric(
        df_dap["Quantidade"], errors="coerce").fillna(0)

    by_fundo = df_dap.groupby(["Fundo", "DAP"], as_index=False)[
        "Quantidade"].sum()
    total = df_dap.groupby("DAP",          as_index=False)["Quantidade"].sum()
    return by_fundo, total


################################################################################
# FUNÇÕES DE CARGA / PROCESSAMENTO (bases originais + DAP extra)
################################################################################

def _prep_spread_df2(path: str | PathLike) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lê o Excel de spreads (Planilha2) e devolve:
        • df_melt – longo, p/ gráfico em linha (Spread × Data)
        • df_vert – wide -> longo p/ gráfico Vertice × Spread (um dia)
    """
    dados = pd.read_excel(path, sheet_name="Planilha6")

    # renomeia & filtra zeros
    cols = ['DATA', 'DAP25', 'DAP26', 'DAP27', 'DAP28', 'DAP30', 'DAP32', 'DAP35', 'DAP40',
            'NTNB25', 'NTNB26', 'NTNB27', 'NTNB28', 'NTNB30', 'NTNB32', 'NTNB33', 'NTNB35', 'NTNB40', 'NTNB45', 'NTNB50', 'NTNB60']
    dados.columns = cols
    for c in cols[1:]:
        dados = dados[dados[c] != 0]

    # calcula spreads
    for y in ["25", "26", "27", "28", "30", "32", "33", "35", "40", "45", "50", "60"]:
        dados[f"Spread B{y}/DAP{y}"] = dados[f"NTNB{y}"] - dados[f"DAP{y}"]
    dados_diff = dados.filter(regex=r"^DATA$|^Spread").copy()

    # -------- DF para gráfico em linha -------- #
    df_melt = (
        dados_diff
        .melt(id_vars="DATA", var_name="Tipo", value_name="Spread")
        .assign(Data_Str=lambda x: x["DATA"].dt.strftime("%Y-%m-%d"))
    )

    cores = {
        'Spread B25/DAP25': '#003366',
        'Spread B26/DAP26': '#1f77b4',
        'Spread B27/DAP27': '#2ca02c',
        'Spread B28/DAP28': '#d62728',
        'Spread B30/DAP30': '#9467bd',
        'Spread B32/DAP32': '#8c564b',
        'Spread B35/DAP35': '#e377c2',
        'Spread B40/DAP40': '#7f7f7f',
        'Spread B45/DAP45': '#bcbd22',
        'Spread B50/DAP50': '#17becf',
        'Spread B60/DAP60': '#ff7f0e',
        'Spread B33/DAP33': '#ffbb78',
    }

    df_melt["Color"] = df_melt["Tipo"].map(cores)

    # -------- DF para gráfico Spread (Y) × Vértice (X) -------- #
    df_vert = (
        dados_diff.set_index("DATA")
        .pipe(lambda df_: df_.assign(**{c.split()[1]: df_[c] for c in df_.columns}))
        .drop(columns=dados_diff.columns[1:])              # só as novas cols
        .reset_index()
        .melt(id_vars="DATA", var_name="Vertice", value_name="Spread")
        .assign(Vertice=lambda x: x["Vertice"].str[-2:].astype(int))  # 25,26,…
    )
    return df_melt, df_vert


def _prep_spread_df(path: str | PathLike) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lê o Excel de spreads (Planilha2) e devolve:
        • df_melt – longo, p/ gráfico em linha (Spread × Data)
        • df_vert – wide -> longo p/ gráfico Vertice × Spread (um dia)
    """
    dados = pd.read_excel(path, sheet_name="Planilha2")

    # renomeia & filtra zeros
    cols = ['DATA', 'DAP26', 'DAP27', 'DAP28', 'DAP30', 'DAP32', 'DAP35', 'DAP40',
            'NTNB26', 'NTNB27', 'NTNB28', 'NTNB30', 'NTNB32', 'NTNB35', 'NTNB40']
    dados.columns = cols
    for c in cols[1:]:
        dados = dados[dados[c] != 0]

    # calcula spreads
    for y in ["26", "27", "28", "30", "32", "35", "40"]:
        dados[f"Spread B{y}/DAP{y}"] = dados[f"NTNB{y}"] - dados[f"DAP{y}"]
    dados_diff = dados.filter(regex=r"^DATA$|^Spread").copy()

    # -------- DF para gráfico em linha -------- #
    df_melt = (
        dados_diff
        .melt(id_vars="DATA", var_name="Tipo", value_name="Spread")
        .assign(Data_Str=lambda x: x["DATA"].dt.strftime("%Y-%m-%d"))
    )

    cores = {
        # 'Spread B25/DAP25': '#003366',
        'Spread B26/DAP26': '#1f77b4',
        'Spread B27/DAP27': '#2ca02c',
        'Spread B28/DAP28': '#d62728',
        'Spread B30/DAP30': '#9467bd',
        'Spread B32/DAP32': '#8c564b',
        'Spread B35/DAP35': '#e377c2',
        'Spread B40/DAP40': '#7f7f7f',
    }
    df_melt["Color"] = df_melt["Tipo"].map(cores)

    # -------- DF para gráfico Spread (Y) × Vértice (X) -------- #
    df_vert = (
        dados_diff.set_index("DATA")
        .pipe(lambda df_: df_.assign(**{c.split()[1]: df_[c] for c in df_.columns}))
        .drop(columns=dados_diff.columns[1:])              # só as novas cols
        .reset_index()
        .melt(id_vars="DATA", var_name="Vertice", value_name="Spread")
        .assign(Vertice=lambda x: x["Vertice"].str[-2:].astype(int))  # 25,26,…
    )
    return df_melt, df_vert


def _prep_ntnb_df(path: str | PathLike) -> pd.DataFrame:
    """
    DATA | B_REF | NTNB_YIELD   (DATA em datetime64[ns])
    """
    dados = pd.read_excel(path, sheet_name="Planilha6")

    nt_cols = ['NTNB25', 'NTNB26', 'NTNB27', 'NTNB28', 'NTNB29',
               'NTNB30', 'NTNB32', 'NTNB33', 'NTNB35', 'NTNB40', 'NTNB45',
               'NTNB50', 'NTNB55', 'NTNB60']
    dados = dados[['DATA'] + nt_cols].copy()
    dados.columns = ['DATA'] + nt_cols

    dados["DATA"] = pd.to_datetime(dados["DATA"])     # ← conversão explícita

    nt_long = (dados
               .melt(id_vars='DATA', var_name='NTNB', value_name='NTNB_YIELD')
               .dropna()
               .assign(B_REF=lambda x: x['NTNB'].str.replace('NTNB', 'B'))
               .drop(columns='NTNB')
               .sort_values(['B_REF', 'DATA'])
               )
    # Substitui zeros por NaN e faz forward fill por grupo de B_REF
    nt_long['NTNB_YIELD'] = (
        nt_long
        .groupby('B_REF')['NTNB_YIELD']
        .transform(lambda x: x.replace(0, np.nan).ffill())
    )
    return nt_long


if "df_ntnb_long" not in st.session_state:
    st.session_state["df_ntnb_long"] = _prep_ntnb_df("Dados/BBG - ECO DASH_te.xlsx")


def process_df() -> pd.DataFrame:
    """Carrega posição + debêntures e devolve df_posicao_juros já agregado.
    Também armazena em session_state:
        • df_extras            – ativos IPCA fora da carteira
        • dap_counts_by_fundo  – contratos DAP hedge já existentes por fundo
        • dap_counts_total     – contratos DAP hedge já existentes consolidados
    """
    # --- Posição completa (sem filtros) --------------------------------------
    df_posicao_raw = load_carteira_hoje()
    st.session_state["df_posicao_raw"] = df_posicao_raw.copy()

    # ================================================================
    # 1. Processa DAPs já existentes (antes de qualquer filtro)
    # ================================================================
    counts_by_fundo, counts_total = process_dap_counts(df_posicao_raw)
    st.session_state["dap_counts_by_fundo"] = counts_by_fundo
    st.session_state["dap_counts_total"] = counts_total

    # ----------------------------------------------------------------
    # 2. Fluxos de debêntures conforme versão anterior
    # ----------------------------------------------------------------
    df_debentures = pd.read_csv("Dados/deb_table_completa2.csv")

    for col in ["Juros projetados", "Fluxo descontado (R$)", "Amortizações"]:
        df_debentures[col] = (
            df_debentures[col]
            .str.replace("-", "0")
            .str.replace(".", "", regex=False)
            .str.replace(",", ".")
            .astype(float)
        )

    ativos = obter_lista_ativos_original() + obter_lista_outros_original()
    dap_dict = obter_dap_dict_original()

    # ----------------------------------------------------------------
    # 2.1 Filtra df_posicao para exposição das debêntures (mantém counts DAP
    #      intocados em session_state)
    # ----------------------------------------------------------------
    df_posicao = df_posicao_raw[df_posicao_raw["Ativo"].isin(ativos)].copy()

    # --- restante da rotina original (formatação + merge) --------------------
    df_debentures.columns = [
        "Dados do evento", "Data de pagamento", "Prazos (dias úteis)",
        "Dias entre pagamentos", "Expectativa de juros (%)", "Juros projetados",
        "Amortizações", "Fluxo descontado (R$)", "Ativo",
    ]
    df_debentures = df_debentures[df_debentures["Ativo"].isin(
        df_posicao["Ativo"])]

    df_debentures["Data de pagamento"] = pd.to_datetime(
        df_debentures["Data de pagamento"])
    df_debentures["Data"] = df_debentures["Data de pagamento"].dt.strftime(
        "%Y-%m")
    df_debentures["Ano"] = df_debentures["Data de pagamento"].dt.year
    df_debentures["Semestre"] = (
        df_debentures["Data de pagamento"].dt.quarter.replace(
            {1: "1º Semestre", 2: "1º Semestre", 3: "2º Semestre", 4: "2º Semestre"})
    )
    df_quant = (df_posicao
                .groupby(["Fundo", "Ativo"], as_index=False)
                .agg({"Quantidade": "sum", "Valor": "sum"}))
    df_quant["Valor"] = (
        df_quant["Valor"].astype(str).str.replace(",", ".").astype(float)
    )

    df_debentures.drop(columns=["Data de pagamento"], inplace=True)

    df_merged = pd.merge(df_debentures, df_quant, on="Ativo", how="left")
    df_merged["Juros projetados"] = df_merged["Fluxo descontado (R$)"] * \
        df_merged["Quantidade"]
    df_merged["Amortizações"] = df_merged["Amortizações"] * \
        df_merged["Quantidade"]
    df_merged["DIV1_ATIVO"] = df_merged["Juros projetados"] * \
        0.0001 * (df_merged["Prazos (dias úteis)"] / 252)
    df_merged["DAP"] = df_merged["Ano"].map(dap_dict)

    # ----------------------------------------------------------------
    # 3. Ativos extras (df_debentures2) – permanece igual
    # ----------------------------------------------------------------
    df_debentures2 = pd.read_csv("Dados/deb_table_completa3.csv")
    for col in ["Juros projetados", "Fluxo descontado (R$)", "Amortizações"]:
        df_debentures2[col] = (
            df_debentures2[col]
            .str.replace("-", "0")
            .str.replace(".", "", regex=False)
            .str.replace(",", ".")
            .astype(float)
        )
    df_debentures2.columns = [
        "Dados do evento", "Data de pagamento", "Prazos (dias úteis)",
        "Dias entre pagamentos", "Expectativa de juros (%)", "Juros projetados",
        "Amortizações", "Fluxo descontado (R$)", "Ativo",
    ]
    df_debentures2["Data de pagamento"] = pd.to_datetime(
        df_debentures2["Data de pagamento"])
    df_debentures2["Data"] = df_debentures2["Data de pagamento"].dt.strftime(
        "%Y-%m")
    df_debentures2["Ano"] = df_debentures2["Data de pagamento"].dt.year
    df_debentures2["Semestre"] = (
        df_debentures2["Data de pagamento"].dt.quarter.replace(
            {1: "1º Semestre", 2: "1º Semestre", 3: "2º Semestre", 4: "2º Semestre"})
    )
    df_debentures2 = df_debentures2[~df_debentures2["Ativo"].isin(
        df_debentures["Ativo"].unique())]

    df_debentures2["Quantidade"] = 100
    df_debentures2["Fundo"] = "SEM FUNDO"
    df_debentures2["Juros projetados"] = df_debentures2[
        "Fluxo descontado (R$)"] * df_debentures2["Quantidade"]
    df_debentures2["Amortizações"] = df_debentures2["Amortizações"] * \
        df_debentures2["Quantidade"]
    df_debentures2["DIV1_ATIVO"] = df_debentures2["Juros projetados"] * \
        0.0001 * (df_debentures2["Prazos (dias úteis)"] / 252)
    df_debentures2["DAP"] = df_debentures2["Ano"].map(dap_dict)

    st.session_state["df_extras"] = df_debentures2.copy()

    # saída final da função
    return df_merged.rename(columns={"Data": "Data de pagamento"})


def process_div01() -> pd.DataFrame:
    df_div1 = pd.read_excel("Dados/AF_Trading.xlsm",
                            sheet_name="Base IPCA", skiprows=16)
    df_div1 = df_div1.iloc[:, :13].dropna()[["DAP", "DV01"]]
    df_div1["DAP"] = df_div1["DAP"].apply(
        lambda x: x[:3] + x[-2:] if isinstance(x, str) and len(x) >= 5 else x)
    return df_div1


def obter_lista_ativos_original() -> List[str]:
    return ['RALM21', 'ACRC21', 'AEAB11', 'AEGP23', 'AESL17', 'AESLA5', 'AESLA7', 'AESLA9', 'AESLB7', 'AESOA1', 'AGVF12', 'AHGD13', 'ALGA27', 'ALGAB1', 'ALGAC2', 'ALGE16', 'ALGTA4', 'ALIG12', 'ALIG13', 'ALIG15', 'ALUP18', 'ANET11', 'ANET12', 'APFD19', 'APPSA2', 'APRB18', 'ARTR19', 'ASAB11', 'ASCP13', 'ASCP23', 'ASER12', 'ASSR21', 'ATHT11', 'ATII12', 'AURE12', 'BARU11', 'BCPSA5', 'BHSA11', 'BLMN12', 'BRFS31', 'BRKP28', 'BRST11', 'CAEC12', 'CAEC21', 'CAJS11', 'CAJS12', 'CART13', 'CASN23', 'CBAN12', 'CBAN32', 'CBAN52', 'CBAN72', 'CCLS11', 'CCROA5', 'CCROB4', 'CCROB6', 'CDES11', 'CEAD11', 'CEAP12', 'CEAP14', 'CEAP17', 'CEAR26', 'CEEBA1', 'CEEBB6', 'CEEBB7', 'CEEBC3', 'CEEBC4', 'CEED12', 'CEED13', 'CEED15', 'CEED17', 'CEED21', 'CEMT19', 'CEPE19', 'CEPEB3', 'CEPEC1', 'CEPEC2', 'CESE32', 'CESPA2', 'CESPA3', 'CGASA1', 'CGASA2', 'CGASB1', 'CGMG18', 'CGOS13', 'CGOS16', 'CGOS24', 'CGOS28', 'CGOS34', 'CHSF13', 'CJEN13', 'CLAG13', 'CLCD26', 'CLCD27', 'CLNG11', 'CLTM14', 'CMGD27', 'CMGD28', 'CMGDB0', 'CMGDB1', 'CMIN11', 'CMIN12', 'CMIN21', 'CMIN22', 'CMTR29', 'CNRD11', 'COCE18', 'COMR14', 'COMR15', 'CONF11', 'CONX12', 'CPFGA2', 'CPFPA0', 'CPFPA5', 'CPFPA7', 'CPFPB7', 'CPGT15', 'CPGT26', 'CPGT27', 'CPGT28', 'CPLD15', 'CPLD26', 'CPLD29', 'CPLD37', 'CPTM15', 'CPXB22', 'CRMG15', 'CRTR12', 'CSAN33', 'CSMGA2', 'CSMGA6', 'CSMGB4', 'CSMGB8', 'CSMGB9', 'CSMGC3', 'CSNAA4', 'CSNAA5', 'CSNAA6', 'CSNAB4', 'CSNAB5', 'CSNAB6', 'CSNAC4', 'CSNP12', 'CSRN19', 'CSRN29', 'CSRNA1', 'CSRNB2', 'CSRNC0', 'CTEE17', 'CTEE18', 'CTEE1B', 'CTEE29', 'CTEE2B', 'CTGE11', 'CTGE13', 'CTGE15', 'CTNS14', 'CTRR11', 'CUTI11', 'CXER12', 'DESK17', 'EBAS13', 'EBENA8', 'ECER12', 'ECHP11', 'ECHP12', 'ECHP22', 'ECOV16', 'ECPN11', 'EDFT11', 'EDPA11', 'EDPT11', 'EDTE12', 'EDVP14', 'EDVP17', 'EEELA0', 'EEELA1', 'EEELB1', 'EGIE17', 'EGIE19', 'EGIE27', 'EGIE29', 'EGIE39', 'EGIE49', 'EGIEA0', 'EGIEA1', 'EGIEB1', 'EGIEB2', 'EGVG11', 'EGVG21', 'EKTRB3', 'EKTRC0', 'EKTRC1', 'EKTT11', 'ELEK37', 'ELET14', 'ELET16', 'ELET23', 'ELET42', 'ELPLA5', 'ELPLA7', 'ELPLB4', 'ELTN15', 'ENAT11', 'ENAT12', 'ENAT13', 'ENAT14', 'ENAT24', 'ENAT33', 'ENERA1', 'ENERB4', 'ENEV13', 'ENEV15', 'ENEV16', 'ENEV18', 'ENEV19', 'ENEV26', 'ENEV28', 'ENEV29', 'ENEV32', 'ENEV39', 'ENEVA0', 'ENEVB0', 'ENGI39', 'ENGIA1', 'ENGIA4', 'ENGIA5', 'ENGIA6', 'ENGIA9', 'ENGIB0', 'ENGIB2', 'ENGIB4', 'ENGIB6', 'ENGIB9', 'ENGIC0', 'ENJG21', 'ENMI21', 'ENMTA3', 'ENMTA4', 'ENMTA5', 'ENMTA7', 'ENMTB3', 'ENMTB5', 'ENSEA1', 'ENTV12', 'EQMAA0', 'EQMAA2', 'EQPA18', 'EQSP11', 'EQSP21', 'EQTC11', 'EQTN11', 'EQTR11', 'EQTR21', 'EQTS11', 'EQUA11', 'ERDV17', 'ERDV38', 'ERDVA4', 'ERDVB4', 'ERDVC3', 'ERDVC4', 'ESAM14', 'ESULA1', 'ESULA6', 'ETAP22', 'ETBA12', 'ETEN11', 'ETEN12', 'ETEN21', 'ETEN22', 'ETEN31', 'ETSP12', 'EUBE11', 'EXTZ11', 'FBRI13', 'FGEN13', 'FLCLA0', 'FRAG14', 'FURN21', 'GASC15', 'GASC16', 'GASC17', 'GASC22', 'GASC23', 'GASC25', 'GASC26', 'GASC27', 'GASP19', 'GASP29', 'GASP34', 'GBSP11', 'GEPA28', 'GRRB24', 'GSTS14', 'GSTS24', 'HARG11', 'HBSA11', 'HBSA21', 'HGLB23', 'HVSP11', 'HZTC14', 'IBPB11', 'IGSN15', 'IRJS14', 'IRJS15', 'ITPO14', 'IVIAA0', 'JALL11', 'JALL13', 'JALL14', 'JALL15', 'JALL21', 'JALL24', 'JSMLB5', 'JTEE11', 'JTEE12', 'KLBNA5', 'LCAMD1', 'LCAMD3', 'LGEN11', 'LORTA7', 'LSVE39', 'LTTE15', 'MEZ511', 'MGSP12', 'MNAU13', 'MOVI18', 'MOVI37', 'MRSAA1', 'MRSAA2', 'MRSAB1', 'MRSAB2', 'MRSAC1', 'MRSAC2', 'MSGT12', 'MSGT13', 'MSGT23', 'MSGT33', 'MTRJ19', 'MVLV16', 'NEOE16', 'NEOE26', 'NMCH11', 'NRTB11', 'NRTB21', 'NTEN11', 'ODTR11', 'ODYA11', 'OMGE12', 'OMGE22', 'OMGE31', 'OMGE41', 'OMNG12', 'ORIG11', 'PALF38', 'PALFA3', 'PALFB3', 'PASN12', 'PEJA11', 'PEJA22', 'PEJA23', 'PETR16', 'PETR17', 'PETR26', 'PETR27', 'PLSB1A', 'POTE11', 'POTE12', 'PPTE11', 'PRAS11', 'PRPO12', 'PRTE12', 'PTAZ11', 'QUAT13', 'RAHD11', 'RAIZ13', 'RAIZ23', 'RATL11', 'RDOE18', 'RDVE11', 'RECV11', 'RESA14', 'RESA15', 'RESA17', 'RESA27', 'RIGEA3', 'RIPR22', 'RIS412', 'RIS414', 'RIS422', 'RIS424', 'RISP12', 'RISP14', 'RISP22', 'RISP24', 'RMSA12', 'RRRP13', 'RSAN16', 'RSAN26', 'RSAN34', 'RSAN44', 'RUMOA2', 'RUMOA3', 'RUMOA4', 'RUMOA5', 'RUMOA6', 'RUMOA7', 'RUMOB1', 'RUMOB3', 'RUMOB5', 'RUMOB6', 'RUMOB7', 'SABP12', 'SAELA1', 'SAELA3', 'SAELB3', 'SAPR10', 'SAPRA2', 'SAPRA3', 'SAPRB3', 'SAVI13', 'SBSPB6', 'SBSPC4', 'SBSPC6', 'SBSPD4', 'SBSPE3', 'SBSPE9', 'SBSPF3', 'SBSPF9', 'SERI11', 'SMTO14', 'SMTO24', 'SNRA13', 'SPRZ11', 'SRTI11', 'STBP35', 'STBP45', 'STRZ11', 'SUMI17', 'SUMI18', 'SUMI37', 'SUZB19', 'SUZB29', 'SUZBA0', 'SUZBC1', 'TAEB15', 'TAEE17', 'TAEE18', 'TAEE26', 'TAEEA2', 'TAEEA4', 'TAEEA7', 'TAEEB2', 'TAEEB4', 'TAEEC2', 'TAEEC4', 'TAEED2', 'TAES15', 'TBEG11', 'TBLE26', 'TCII11', 'TEPA12', 'TIET18', 'TIET29', 'TIET39', 'TIMS12', 'TNHL11', 'TOME12', 'TPEN11', 'TPNO12', 'TPNO13', 'TRCC11', 'TRGO11', 'TRPLA4', 'TRPLA7', 'TRPLB4', 'TRPLB7', 'TSSG21', 'TVVH11', 'UHSM12', 'UNEG11', 'UNTE11', 'USAS11', 'UTPS11', 'UTPS12', 'UTPS21', 'UTPS22', 'VALE38', 'VALE48', 'VALEA0', 'VALEB0', 'VALEC0', 'VAMO33', 'VAMO34', 'VBRR11', 'VDBF12', 'VDEN12', 'VERO12', 'VERO13', 'VERO24', 'VERO44', 'VLIM13', 'VLIM14', 'VLIM15', 'VLIM16', 'VPLT12', 'VRDN12', 'WDPR11', 'XNGU17']


def obter_lista_outros_original() -> List[str]:
    return [
        "BRFS31", "CRA Ferroeste 2ª Série", "CRI Bem Brasil", "NTN-B26", "NTN-B28",
        "NTN-B30", "NTN-B32", "NTN-B50", "CRI Vic Engenharia 1ª Emissão", "TBCR18",
        "CRTA12", "CERT11", "CRI PERNAMBUCO 35ª (23J1753853)", "CRI Vic Engenharia 2ª Emissão", "VALEB1"
    ]


def obter_dap_dict_original() -> Dict[int, str]:
    return {
        2025: "DAP26", 2026: "DAP26", 2027: "DAP27", 2028: "DAP28", 2029: "DAP29",
        2030: "DAP30", 2031: "DAP30", 2032: "DAP32", 2033: "DAP32", 2034: "DAP35",
        2035: "DAP35", 2036: "DAP35", 2037: "DAP35", 2038: "DAP40", 2039: "DAP40",
        2040: "DAP40", 2041: "DAP40", 2042: "DAP40", 2043: "DAP40", 2044: "DAP40", 2045: "DAP40", 2046: "DAP40", 2047: "DAP40", 2048: "DAP40", 2049: "DAP40", 2050: "DAP40", 2051: "DAP40", 2052: "DAP40", 2053: "DAP40", 2054: "DAP40", 2055: "DAP40", 2056: "DAP40", 2057: "DAP40", 2058: "DAP40", 2059: "DAP40", 2060: "DAP40"
    }


###############################################################################
# UTILITÁRIOS DE UI (sem alterações exceto import CSS)
###############################################################################

_B_MAP = {   # = mesmo critério que você usa p/ DAPs
    2025: 'B25', 2026: 'B26', 2027: 'B27', 2028: 'B28', 2029: 'B29',
    2030: 'B30', 2031: 'B30', 2032: 'B32', 2033: 'B32', 2034: 'B35',
    2035: 'B35', 2036: 'B35', 2037: 'B35', 2038: 'B40', 2039: 'B40',
    2040: 'B40', 2041: 'B40', 2042: 'B40', 2043: 'B40', 2044: 'B40',
}


def _to_B_ref(s: str | float | pd.Timestamp) -> str | None:
    """
    ▸ Converte qualquer coisa que apareça na col. **B referência** para
      formato canónico 'B25', 'B35', …  
    ▸ Aceita:
        • já estar como 'B35'
        • string-data '15/05/2040'
        • datetime / Timestamp
    """
    if pd.isna(s):
        return None
    txt = str(s).strip().upper()

    m = re.fullmatch(r'B(\d{2})', txt)
    if m:                        # já no formato correcto
        return f'B{m.group(1)}'

    # tenta parsear como data (dd/mm/yyyy, yyyy-mm-dd…)
    data = pd.to_datetime(txt, dayfirst=True, errors='coerce')
    if pd.notna(data):
        ano = data.year
        return _B_MAP.get(ano)
    
    return None                  # não conseguiu limpar


def load_spreads_afinvest(path="Dados/spreads_afinvest.csv") -> pd.DataFrame:
    df = pd.read_csv(path, sep=";").rename(columns=str.strip)
    df["Ativo"] = df["Ativo"].str.upper().str.strip()
    df["B_REF"] = df["B referência"].str.upper().str.strip()
    return df[["Ativo", "B_REF"]]


def _div1_por_fundo(df_posicao: pd.DataFrame,
                    df_lookup:  pd.DataFrame) -> pd.DataFrame:
    """
    ▸ junta posição (quantidade) + DIV_ONE e devolve:
       Fundo | B_REF | DIV1_QTD
       onde  DIV1_QTD = quantidade * DIV_ONE
    """
    base = (df_posicao.merge(df_lookup, on='Ativo', how='left')
                      .dropna(subset=['B_REF']))          # fora o que não casar

    base['DIV1_QTD'] = base['Quantidade'] * base['DIV_ONE']
    return (base.groupby(['Fundo', 'B_REF'], as_index=False)
            ['DIV1_QTD'].sum())


def _ultimo_spread_por_B(df_melt: pd.DataFrame) -> pd.Series:
    """
    devolve Series  •index=B_REF  •value=spread mais recente
    """
    ult_dia = df_melt['DATA'].max()
    ult = df_melt[df_melt['DATA'] == ult_dia].copy()

    ult['B_REF'] = ult['Tipo'].str.extract(r'B(\d{2})').radd('B')
    return ult.set_index('B_REF')['Spread']


def _spread_medio_por_fundo(df_posicao: pd.DataFrame,
                            df_spreads: pd.DataFrame) -> pd.DataFrame:
    """
    df_posicao precisa conter:
        • Fundo, Ativo, DIV1_ATIVO  (já calculado no seu df_merged)

    Retorna:
        Fundo | SPREAD_MEDIO_BPS
    """
    base = (df_posicao[["Fundo", "Ativo", "DIV1_ATIVO"]]
            .merge(df_spreads, on="Ativo", how="inner"))

    # remove itens sem DV01 (caso zero / NaN)
    base = base[base["DIV1_ATIVO"] != 0]

    res = (base.groupby("Fundo")
           .apply(lambda g: np.average(g["SPREAD_BPS"],
                                       weights=g["DIV1_ATIVO"]))
           .reset_index(name="SPREAD_MEDIO_BPS"))

    return res.sort_values("SPREAD_MEDIO_BPS")


def check_duplicates(df: pd.DataFrame, label: str):
    dups = df.duplicated(subset=["Ativo", "Data de pagamento"]).sum()
    if dups:
        st.warning(
            f"{label}: Encontradas {dups} linhas duplicadas de (Ativo, Data de pagamento). Revise filtros.")
        # Colocar quais são os ativos duplicados
        ativos_duplicados = df[df.duplicated(
            subset=["Ativo", "Data de pagamento"], keep=False)]
        ativos_duplicados = ativos_duplicados[[
            "Ativo", "Data de pagamento"]].drop_duplicates()
        if st.checkbox("Mostrar ativos duplicados?", key="chk_show_dups"):
            st.write("Ativos duplicados encontrados:")
            st.dataframe(ativos_duplicados)


def filtro_generico(df: pd.DataFrame) -> pd.DataFrame:
    """Permite filtrar por qualquer coluna via sidebar."""
    if st.sidebar.checkbox("Filtrar por coluna", key="chk_filter"):
        col = st.sidebar.selectbox(
            "Coluna para filtrar", df.columns.tolist(), key="sel_col")
        valores = sorted(df[col].unique().tolist())
        escolhidos = st.sidebar.multiselect(
            "Valores:", valores, default=valores, key="vals")
        df = df[df[col].isin(escolhidos)]
    return df

###############################################################################
# VISUALIZAÇÕES (iguais)
###############################################################################

def plot_relacao_juros(df):
    df_plot = (
        df.groupby(["Ano", "Semestre"], as_index=False)[
            "Juros projetados"].sum()
        .assign(**{"Juros projetados (R$ mil)": lambda x: x["Juros projetados"] / 1_000})
    )
    # Lista de anos únicos no eixo x
    anos = sorted(df_plot["Ano"].unique())
    semestres = df_plot["Semestre"].unique()

    fig = go.Figure()

    for semestre in semestres:
        df_sem = df_plot[df_plot["Semestre"] == semestre]
        fig.add_trace(go.Bar(
            x=df_sem["Ano"],
            y=df_sem["Juros projetados (R$ mil)"],
            name=semestre,
            marker_color="#1F4E79" if semestre == "1º Semestre" else "#A5C8E1"
        ))

    fig.update_layout(
        barmode="group",  # Equivalente ao position="dodge"
        title="Juros + Amortização Projetados por Ano/Semestre",
        xaxis_title="Ano",
        yaxis_title="Juros (R$ mil)",
        height=400,
        plot_bgcolor="white",
        xaxis=dict(tickangle=45)
    )

    st.plotly_chart(fig, use_container_width=True)


def plot_div1_layout(df: pd.DataFrame, df_div1: pd.DataFrame, carteira: pd.DataFrame | None = None):
    """Exibe gráfico + tabela DIV1/DAP. Se *carteira* fornecido, cruza com
    quantidade já existente (coluna 'CARTEIRA')."""

    df_sum = (
        df.groupby("DAP", as_index=False)["DIV1_ATIVO"].sum()
        .merge(df_div1, on="DAP", how="left")
        .rename(columns={"DV01": "DV01_DAP"})
    )
    
    df_sum["CONTRATOS"] = df_sum["DIV1_ATIVO"] / df_sum["DV01_DAP"]

    # -----------------------------------------------------------
    # ▼ Integra contratos já na carteira
    # -----------------------------------------------------------
    if carteira is not None and not carteira.empty:
        cartera_norm = carteira.copy()
        cartera_norm["DAP"] = cartera_norm["DAP"].astype(str)
        df_sum = df_sum.merge(cartera_norm, on="DAP", how="left")
        df_sum.rename(columns={"CARTEIRA": "CARTEIRA"}, inplace=True)
        df_sum["CARTEIRA"].fillna(0, inplace=True)
        df_sum["FALTAM"] = df_sum["CONTRATOS"] + df_sum["CARTEIRA"]

    # -----------------------------------------------------------
    # ► Gráfico – DIV1 por DAP (mantido)
    # -----------------------------------------------------------
    col1, col2, col3 = st.columns([4.9, 0.2, 4.9])
    with col1:
        p1 = (
            ggplot(df_sum, aes(x="DAP", y="DIV1_ATIVO"))
            + geom_col(fill="#1F4E79")
            + geom_text(aes(label=df_sum["DIV1_ATIVO"].apply(
                lambda x: f"{x:,.0f}")), va="bottom", size=8)
            + labs(title="DIV1 vs DAP", x="DAP", y="DIV1 (R$)")
            + theme(figure_size=(6, 4), axis_text_x=element_text(rotation=45, ha="right"),
                    panel_background=element_rect(fill="white"))
        )
        st.pyplot(p1.draw(), use_container_width=True)

    with col2:
        st.html(
            "<div style='border-left:2px solid rgba(49,51,63,0.2);height:60vh;margin:auto'></div>")

    with col3:
        #df_fmt = df_sum.copy()
        ## Arredonda CONTRATOS
        #df_fmt["CONTRATOS"] = df_fmt["CONTRATOS"].round().astype(int)
        ## if "CARTEIRA" in df_fmt.columns:
        ##    df_fmt["CARTEIRA"] = df_fmt["CARTEIRA"].round().astype(int)
        ##    df_fmt["FALTAM"] = df_fmt["FALTAM"].round().astype(int)
        #df_fmt.set_index("DAP", inplace=True)
#
        ## Totais
        #totais = df_fmt.sum(numeric_only=True)
        #df_fmt.loc["Total"] = totais
#
        ## Formatação
        #sty = df_fmt.style.format("{:,.0f}")
        #sty = sty.set_table_styles([
        #    {"selector": "th", "props": [
        #        ("font-weight", "bold"), ("text-align", "center")]},
        #    {"selector": "td", "props": [("text-align", "right")]},
        #    {"selector": "caption", "props": [
        #        ("font-size", "16px"), ("font-weight", "bold")]},
        #])
        #st.table(sty.set_caption("Tabela de DAPs: Necessário × Carteira"))
        df_fmt = df_sum.copy()
        df_fmt.set_index("DAP", inplace=True)

        # Totais (sem arredondar)
        totais = df_fmt.sum(numeric_only=True)
        df_fmt.loc["Total"] = totais

        # ===== TABELA =====
        sty = (
            df_fmt.style
            .format({
                "DIV1_ATIVO": "{:,.4f}",
                "DV01_DAP": "{:,.6f}",
                "CONTRATOS": "{:,.4f}",
                "CARTEIRA": "{:,.4f}",
                "FALTAM": "{:,.4f}",
            })
            .set_table_styles([
                {"selector": "th", "props": [("font-weight", "bold"), ("text-align", "center")]},
                {"selector": "td", "props": [("text-align", "right")]},
            ])
        )

        st.table(sty)

        # ===== EXPORTAÇÃO EXCEL =====
        def _to_excel_bytes(df: pd.DataFrame) -> bytes:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="DAP_DIV1", merge_cells=False)
            return buf.getvalue()

        st.download_button(
            label="📥 Baixar tabela em Excel",
            data=_to_excel_bytes(df_fmt),
            file_name="tabela_dap_div1.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Retorna apenas DAP/CONTRATOS (p/ rotina de salvar posição)
    return df_sum[["DAP", "CONTRATOS"]].set_index("DAP")


def atualizar_session_state_contratos(fundo: str, df_contr: pd.DataFrame):
    if "df_total" not in st.session_state:
        st.session_state.df_total = pd.DataFrame()

    st.subheader("Pré-visualização dos contratos")
    st.table(df_contr.style.format("{:,.0f}"))

    overwrite = st.checkbox("Sobrescrever se já existir", key=f"ovw_{fundo}")

    def _salvar():
        if fundo in st.session_state.df_total.columns and not overwrite:
            st.warning(
                "Coluna já existente. Marque 'Sobrescrever' para substituir.")
            return
        if fundo in st.session_state.df_total.columns and overwrite:
            st.session_state.df_total.drop(columns=[fundo], inplace=True)
        st.session_state.df_total = st.session_state.df_total.add(
            df_contr.rename(columns={"CONTRATOS": fundo}), fill_value=0)
        st.success("Posição salva!")

    st.button("Salvar posição", on_click=_salvar, key=f"btn_save_{fundo}")

    # Exclusão de coluna específica
    if not st.session_state.df_total.empty:
        st.subheader("Posições acumuladas")
        df_plot = st.session_state.df_total.copy()
        df_plot = df_plot.replace([float("inf"), float("-inf")], 0).fillna(0)
        # Transformar todas as colunas em INT (arredondando antes)
        df_plot = df_plot.round().astype(int)

        # Criar colunas de compra e venda
        df_plot["Compra"] = df_plot[df_plot > 0].sum(axis=1)
        df_plot["Venda"] = df_plot[df_plot < 0].sum(axis=1)

        # Aplicar formatação para exibição
        st.table(df_plot.style.format("{:.0f}"))
        col_to_del = st.selectbox(
            "Coluna para apagar", st.session_state.df_total.columns, key="col_del")
        if st.button("Apagar coluna", key="btn_del_col"):
            st.session_state.df_total.drop(columns=[col_to_del], inplace=True)
            st.success(f"Coluna '{col_to_del}' removida.")

        def to_excel_bytes(df):
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Posições")
            return buf.getvalue()
        st.download_button("Baixar Excel", data=to_excel_bytes(df_plot),
                           file_name="posicoes_por_fundo.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


###############################################################################
# FUNÇÕES DE PÁGINA – AJUSTES PARA PASSAR CONTAGEM CARTEIRA
###############################################################################

def analisar_ativo(df: pd.DataFrame, df_div1: pd.DataFrame):
    st.header("Analisar Ativo")

    # NOVO ➌ – junta a base principal com os ativos SEM FUNDO
    df_extras = st.session_state.get("df_extras", pd.DataFrame())
    df_full = pd.concat([df, df_extras], ignore_index=True).copy()

    # seleção de ativo agora usa a lista ampliada
    ativo_sel = st.sidebar.selectbox(
        "Escolha o ativo:",
        sorted(df_full["Ativo"].unique())
    )

    # a partir daqui troque `df`  →  `df_full`
    fundos_disponiveis = df_full[df_full["Ativo"]
                                 == ativo_sel]["Fundo"].unique().tolist()
    if len(fundos_disponiveis) > 1:
        usar_escolha = st.sidebar.checkbox("Escolher fundo-origem?")
        if usar_escolha:
            fundo_origem = st.sidebar.selectbox(
                "Fundo de origem:", fundos_disponiveis)
        else:
            fundo_origem = fundos_disponiveis[0]
        df_asset = df_full[(df_full["Ativo"] == ativo_sel) & (
            df_full["Fundo"] == fundo_origem)].copy()
    else:
        df_asset = df_full[df_full["Ativo"] == ativo_sel].copy()

    # Simulador de quantidade --------------------------------------------------
    qtd_atual = int(df_asset["Quantidade"].iloc[0])
    nova_qtd = st.sidebar.number_input(
        f"Nova quantidade (atual {qtd_atual})", value=qtd_atual, min_value=-100_000)
    if st.sidebar.button("Simular Quantidades Diferentes"):
        df_asset["Quantidade"] = nova_qtd
        df_asset["Juros projetados"] = df_asset["Fluxo descontado (R$)"] * \
            df_asset["Quantidade"]
        df_asset["DIV1_ATIVO"] = df_asset["Juros projetados"] * \
            0.0001 * (df_asset["Prazos (dias úteis)"] / 252)
        st.success("Nova quantidade aplicada!")

    # Filtro genérico opcional -------------------------------------------------
    df_asset = filtro_generico(df_asset)

    # Verifica duplicidades (não deve haver) -----------------------------------
    check_duplicates(df_asset, "Analisar Ativo")

    # Visualizações ------------------------------------------------------------
    plot_relacao_juros(df_asset)
    plot_div1_layout(df_asset, df_div1)

    if st.sidebar.checkbox("Mostrar base do ativo"):
        st.dataframe(df_asset)
        if "SEM FUNDO" in df_asset["Fundo"].unique():
            st.warning(
                "Ativos fora da carteira foram atualizados com as taxas do dia 25 de abril.")
        # Colocar checkbox para exportar base filtrada, mas trocando os pontos por vírgulas
        if st.checkbox("Exportar base filtrada", key="chk_export"):
            df_asset_export = df_asset.copy()
            for col in ["Juros projetados", "Fluxo descontado (R$)", "Amortizações", "DIV1_ATIVO"]:
                df_asset_export[col] = df_asset_export[col].astype(float).apply(
                    lambda x: f"{x:,.2f}".replace(",", "").replace(".", ","))

            # Cria buffer de memória
            output = io.BytesIO()

            # Salva o DataFrame no buffer como Excel usando openpyxl
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_asset_export.to_excel(
                    writer, index=False, sheet_name='Base')

            # Move o ponteiro para o início do arquivo
            output.seek(0)

            # Botão de download
            st.download_button(
                label="Baixar base filtrada",
                data=output,
                file_name=f"{ativo_sel}_base_filtrada.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


def analisar_fundo(df: pd.DataFrame, df_div1: pd.DataFrame):

    st.header("Analisar Fundo")
    lista = sorted(df["Fundo"].unique())
    #Tirar o fundo "AF DEB INCENTIVADAS"
    #lista.remove('AF DEB INCENTIVADAS')

    fundo_sel = st.sidebar.selectbox(
        "Selecione o fundo:", lista)
    df_fundo = df[df["Fundo"] == fundo_sel].copy()

    # ————————————————— AVISO DE DIFERENÇAS ————————————————— #
    # diffs = comparar_posicoes(fundo_sel, df_fundo["Ativo"].unique().tolist())
    # if diffs["faltando"]:
    #    st.warning(f"❌ Ativos presentes ontem mas AUSENTES hoje: {', '.join(diffs['faltando'])}")
    # if diffs["novos"]:
    #    st.info   (f"➕ Ativos NOVOS hoje (não constavam ontem): {', '.join(diffs['novos'])}")

    # Filtro de ativos internos ------------------------------------------------
    ativos_fundo = sorted(df_fundo["Ativo"].unique())
    ativos_filt = st.sidebar.multiselect(
        "Filtrar ativos do fundo:", ativos_fundo, default=None)
    if not ativos_filt:
        ativos_filt2 = ativos_fundo
    else:
        ativos_filt2 = ativos_filt
    df_fundo = df_fundo[df_fundo["Ativo"].isin(ativos_filt2)]

    # Adicionar novos ativos ---------------------------------------------------
    novos_ativos = st.sidebar.checkbox("Adicionar novos ativos (temporário)")

    if novos_ativos:
        df_extras = st.session_state.get("df_extras", pd.DataFrame())
        base_cands = set(df["Ativo"])               # já existentes nos fundos
        extra_cands = set(df_extras["Ativo"])        # só no debentures2
        candidatos = sorted(
            list((base_cands | extra_cands) - set(df_fundo["Ativo"])))

        novos = st.sidebar.multiselect("Ativos a adicionar:", candidatos)

        for atv in novos:
            if atv in base_cands:                                   # lógica antiga
                fundos_src = df[df["Ativo"] == atv]["Fundo"].unique().tolist()
                f_src = st.sidebar.selectbox(
                    f"Fundo origem {atv}", fundos_src, key=f"src_{atv}")
                dados = df[(df["Ativo"] == atv) & (
                    df["Fundo"] == f_src)].copy()

            else:                                                   # veio do df_extras
                dados = df_extras[df_extras["Ativo"] == atv].copy()
                # mantém default 100, mas o usuário pode mudar depois
                dados["Juros projetados"] = dados["Fluxo descontado (R$)"] * \
                    dados["Quantidade"]
                dados["DIV1_ATIVO"] = (
                    dados["Juros projetados"] * 0.0001 *
                    (dados["Prazos (dias úteis)"] / 252)
                )

            # dados["Fundo"] = fundo_sel                    # destino final
            df_fundo = pd.concat([df_fundo, dados], ignore_index=True)

    # Simulador de quantidades -------------------------------------------------
    if ativos_filt:
        st.sidebar.markdown("### Simular novas quantidades")
        novas_qtds = {}
        for atv in sorted(df_fundo["Ativo"].unique()):
            q_atual = int(
                df_fundo.loc[df_fundo["Ativo"] == atv, "Quantidade"].iloc[0])
            novas_qtds[atv] = st.sidebar.number_input(
                atv, value=q_atual, key=f"num_{atv}")
        if st.sidebar.button("Aplicar quantidades", key="btn_qtd_fundo"):
            for a, q in novas_qtds.items():
                df_fundo.loc[df_fundo["Ativo"] == a, "Quantidade"] = q
            df_fundo["Juros projetados"] = df_fundo["Fluxo descontado (R$)"] * \
                df_fundo["Quantidade"]
            df_fundo["DIV1_ATIVO"] = df_fundo["Juros projetados"] * \
                0.0001 * (df_fundo["Prazos (dias úteis)"] / 252)
            st.success("Novas quantidades aplicadas!")

    else:
        if novos_ativos:
            st.sidebar.markdown("### Simular novas quantidades")
            novas_qtds = {}
            for atv in sorted(novos):
                q_atual = int(
                    df_fundo.loc[df_fundo["Ativo"] == atv, "Quantidade"].iloc[0])
                novas_qtds[atv] = st.sidebar.number_input(
                    atv, value=q_atual, key=f"num_{atv}")
            if st.sidebar.button("Aplicar quantidades", key="btn_qtd_fundo"):
                for a, q in novas_qtds.items():
                    df_fundo.loc[df_fundo["Ativo"] == a, "Quantidade"] = q
                df_fundo["Juros projetados"] = df_fundo["Fluxo descontado (R$)"] * \
                    df_fundo["Quantidade"]
                df_fundo["DIV1_ATIVO"] = df_fundo["Juros projetados"] * \
                    0.0001 * (df_fundo["Prazos (dias úteis)"] / 252)
                st.success("Novas quantidades aplicadas!")

    # Filtro genérico opcional -------------------------------------------------
    df_fundo = filtro_generico(df_fundo)

    # Verifica duplicidades ----------------------------------------------------
    check_duplicates(df_fundo, "Analisar Fundo")

    # ► Obtém quantidade de contratos já na carteira para esse fundo
    dap_by_fundo = st.session_state.get("dap_counts_by_fundo", pd.DataFrame())
    carteira_fundo = (
        dap_by_fundo[dap_by_fundo["Fundo"] == fundo_sel][["DAP", "Quantidade"]]
        .rename(columns={"Quantidade": "CARTEIRA"})
    )

    # Visualizações & contratos
    plot_relacao_juros(df_fundo)
    df_contr = plot_div1_layout(df_fundo, df_div1, carteira_fundo)
    atualizar_session_state_contratos(fundo_sel, df_contr)



def analisar_geral(df: pd.DataFrame, df_div1: pd.DataFrame):
    st.header("Análise Geral – Consolidado de Fundos")

    # --- Seleção múltipla de fundos (igual)
    todos_fundos = sorted(df["Fundo"].unique())
    fundos_sel = st.sidebar.multiselect(
        "Escolha os fundos:", todos_fundos, default=None)
    if not fundos_sel:
        fundos_sel = todos_fundos

    df_sel = df[df["Fundo"].isin(fundos_sel)].copy()

    # --- Seleção opcional de ativos dentro desses fundos ----------------------
    ativos_disp = sorted(df_sel["Ativo"].unique())
    ativos_filtr = st.sidebar.multiselect(
        "Filtrar ativos dentro dos fundos selecionados:", ativos_disp, default=None)
    if not ativos_filtr:
        ativos_filtr = ativos_disp
    df_sel = df_sel[df_sel["Ativo"].isin(ativos_filtr)]

    # Filtro genérico adicional (qualquer coluna) ------------------------------
    df_sel = filtro_generico(df_sel)

    # Checa duplicidades -------------------------------------------------------
    # check_duplicates(df_sel, "Análise Geral")

    # … (restante dos filtros)

    # ► Quantos contratos já existem nestes fundos?
    dap_by_fundo = st.session_state.get("dap_counts_by_fundo", pd.DataFrame())
    carteira_geral = (
        dap_by_fundo[dap_by_fundo["Fundo"].isin(fundos_sel)]
        .groupby("DAP", as_index=False)["Quantidade"].sum()
        .rename(columns={"Quantidade": "CARTEIRA"})
    )

    # Visualizações
    plot_relacao_juros(df_sel)
    plot_div1_layout(df_sel, df_div1, carteira_geral)

    if st.sidebar.checkbox("Mostrar base consolidada"):
        st.dataframe(df_sel)
        # Colocar um aviso de os ativos fora da cartera foram atualizados com as taxas do dia 27 de novembro
        if "SEM FUNDO" in df_sel["Fundo"].unique():
            st.warning(
                "Ativos fora da carteira foram atualizados com as taxas do dia 27 de novembro.")


def analisar_spreads() -> None:
    """
    Parte de spreads NTNB × DAP – filtros, linha Plotly, barras Plotly.
    """
    st.subheader("Análise – Spreads NTNB × DAP")

    # ▸ carrega uma vez -----------------------------------------------------
    if "df_spread_melt" not in st.session_state:
        df_melt_tmp, df_vert_tmp = _prep_spread_df("Dados/BBG - ECO DASH_te.xlsx")
        st.session_state["df_spread_melt"] = df_melt_tmp
        st.session_state["df_spread_vert"] = df_vert_tmp

    df_melt_full = st.session_state["df_spread_melt"]
    df_vert_full = st.session_state["df_spread_vert"]

    # ▸ sidebar – filtros comuns ------------------------------------------
    with st.sidebar:
        st.write('---')
        st.markdown("## Filtros – Spreads")
        st.markdown("#### Spread vs. Data")

        min_date = df_melt_full["DATA"].min().date()
        max_date = df_melt_full["DATA"].max().date()

        col_a, col_b = st.columns(2)
        start_date = col_a.date_input("Início", min_value=min_date,
                                      max_value=max_date, value=min_date,
                                      key="hs_start")
        end_date = col_b.date_input("Fim",    min_value=min_date,
                                    max_value=max_date, value=max_date,
                                    key="hs_end")

        freq_map = {"Diária": "D", "Semanal": "W", "Mensal": "M"}
        freq = st.selectbox("Frequência", list(freq_map), 0)

    if start_date > end_date:
        st.warning("A data inicial é posterior à final – ajuste o intervalo.")
        return

    # ▸ aplica período & reamostra -----------------------------------------
    mask = df_melt_full["DATA"].dt.date.between(start_date, end_date)
    df_melt = df_melt_full[mask].copy()
    df_vert = df_vert_full[df_vert_full["DATA"].dt.date
                           .between(start_date, end_date)].copy()

    if freq != "Diária":
        rule = freq_map[freq]                       # "W" ou "M"
        # – df_melt
        wide = (df_melt.pivot(index="DATA", columns="Tipo", values="Spread")
                .resample(rule, label="right", closed="right")
                .median()
                .dropna(how="all"))
        df_melt = (wide.stack().rename("Spread").reset_index())
        df_melt["Data_Str"] = df_melt["DATA"].dt.strftime("%Y-%m-%d")
        color_map = (df_melt_full.drop_duplicates("Tipo")
                     .set_index("Tipo")["Color"])
        df_melt["Color"] = df_melt["Tipo"].map(color_map)
        # – df_vert
        wide_v = (df_vert.pivot(index="DATA", columns="Vertice", values="Spread")
                  .resample(rule, label="right", closed="right")
                  .median()
                  .dropna(how="all"))
        df_vert = (wide_v.stack().rename("Spread").reset_index()
                   .assign(Vertice=lambda x: x["Vertice"].astype(int)))

    # ═════ 1) LINHA (Plotly) ══════════════════════════════════════════════
    pares = sorted(df_melt["Tipo"].unique())
    media_lbl = "Média dos Spreads"
    opcoes = [media_lbl] + pares

    selecionados = st.sidebar.multiselect(
        "Vértices para a linha", opcoes, default=[media_lbl])

    # monta DataFrame com médias + pares escolhidos
    dfs = []
    if media_lbl in selecionados:
        df_media = (df_melt[df_melt["Tipo"] != "Spread B25/DAP25"]
                    .groupby("DATA", as_index=False)["Spread"].mean()
                    .assign(Tipo=media_lbl, Color="#000000"))
        df_media["Data_Str"] = df_media["DATA"].dt.strftime("%Y-%m-%d")
        dfs.append(df_media)

    for par in selecionados:
        if par != media_lbl:
            dfs.append(df_melt[df_melt["Tipo"] == par])

    df_plot = pd.concat(dfs, ignore_index=True)

    fig_line = go.Figure()
    for tipo, grp in df_plot.groupby("Tipo"):
        pontos = len(grp)
        text = ['' if i % 5 else f'{v:.2f}' for i, v in enumerate(grp["Spread"])] \
            if len(selecionados) == 1 else None
        modo = "lines+markers+text" if text else "lines+markers"

        fig_line.add_trace(go.Scatter(
            x=grp["Data_Str"], y=grp["Spread"],
            mode=modo, text=text, textposition="top center",
            marker=dict(color=grp["Color"].iloc[0], size=6),
            name=tipo))

    fig_line.update_layout(
        title="Spreads NTNB × DAP – evolução temporal",
        xaxis_title="Data", yaxis_title="Spread (p.p.)",
        plot_bgcolor="white", height=450, legend_title_text="")

    st.plotly_chart(fig_line, use_container_width=True)

    # ═════ 2) BARRAS (Plotly) ═════════════════════════════════════════════
    with st.sidebar:
        st.write('---')
        st.markdown("#### Spread × Vértice")

        agg_opt = st.radio(
            "Base de cálculo",
            ["Diário", "Mediana Semanal", "Mediana Mensal"],
            0, key="hs_bar_agg")

    ordem = ["25", "26", "27", "28", "30", "32", "35", "40"]
    df_base = df_vert_full.copy()

    if agg_opt != "Diário":
        rule = "W" if agg_opt == "Mediana Semanal" else "M"
        df_base = (df_base.pivot(index="DATA", columns="Vertice", values="Spread")
                   .resample(rule, label="right", closed="right")
                   .median()
                   .stack()
                   .rename("Spread")
                   .reset_index())

    datas_disp = sorted(df_base["DATA"].dt.date.unique())

    with st.sidebar:
        if agg_opt == "Diário":
            data_ref = st.date_input("Data de referência",
                                     value=datas_disp[-1],
                                     min_value=datas_disp[0],
                                     max_value=datas_disp[-1],
                                     key="hs_bar_refdate")
        else:
            rotulo = "Semana" if agg_opt == "Mediana Semanal" else "Mês"
            data_ref = st.selectbox(f"{rotulo} de referência",
                                    datas_disp,
                                    index=len(datas_disp) - 1,
                                    key="hs_bar_refsel")

    df_sel = df_base[df_base["DATA"].dt.date == data_ref]
    spread_map = dict(zip(df_sel["Vertice"].astype(str), df_sel["Spread"]))
    y_vals = [spread_map.get(v, None) for v in ordem]
    text_vals = [f"{v:.2f}" if v is not None else "" for v in y_vals]

    fig_bar = go.Figure(go.Bar(
        x=ordem, y=y_vals,
        text=text_vals, textposition="outside",
        marker_color="#1F4E79"))

    fig_bar.update_layout(
        title=f"Spreads por Vértice – {data_ref:%d/%m/%Y}",
        xaxis=dict(title="Vértice (anos)", type="category"),
        yaxis=dict(title="Spread (p.p.)"),
        plot_bgcolor="white", height=550, showlegend=False)

    st.plotly_chart(fig_bar, use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────


def analisar_spreads_deb_b(df_posicao: pd.DataFrame) -> None:
    """
    • Gráfico 1 – Spread por ativo (linha)                    – opcional
    • Gráfico 2 – Spread médio ponderado por vértice/fundo    – opcional
    • Gráfico 3 – Série temporal do spread médio ponderado    – 1‑N fundos
    """
    st.header("Spreads Debêntures × NTNB‑B")

    # ═════ 0. CARREGAS BÁSICAS ═══════════════════════════════════════════
    tx_hist = (
        pd.read_csv("Dados/dados_ativos (01_01_2025-29_04_2025).csv")
          .rename(columns={"position_date": "DATA",
                           "ativo": "Ativo",
                           "tax": "TAX_INDIC"})
    )
    tx_hist["Ativo"] = tx_hist["Ativo"].str.upper().str.strip()
    tx_hist["DATA"] = pd.to_datetime(tx_hist["DATA"], errors="coerce")
    tx_hist["TAX_INDIC"] = pd.to_numeric(tx_hist["TAX_INDIC"], errors="coerce")
    tx_hist.dropna(subset=["DATA", "TAX_INDIC"], inplace=True)

    if "df_spreads_af" not in st.session_state:
        st.session_state["df_spreads_af"] = load_spreads_afinvest()
    lk = st.session_state["df_spreads_af"]

    if "df_ntnb_long" not in st.session_state:
        st.session_state["df_ntnb_long"] = _prep_ntnb_df(
            "BBG - ECO DASH_te.xlsx")

    # st.write(st.session_state["df_ntnb_long"])

    nt_long = st.session_state["df_ntnb_long"].copy()
    nt_long["DATA"] = pd.to_datetime(nt_long["DATA"], errors="coerce")
    nt_long["NTNB_YIELD"] = pd.to_numeric(
        nt_long["NTNB_YIELD"], errors="coerce")
    nt_long.dropna(subset=["DATA", "NTNB_YIELD"], inplace=True)

    vertices_disp = sorted(nt_long["B_REF"].unique())

    # ═════ 1. BASE GERAL COM SPREAD (para reutilizar) ════════════════════
    base0 = (
        df_posicao[["Ativo", "Fundo", "DIV1_ATIVO"]]
        .merge(lk, on="Ativo", how="left")
        .merge(tx_hist, on="Ativo", how="left")
        .dropna(subset=["B_REF", "DATA", "TAX_INDIC"])
    )

    # merge_asof por vértice
    frames = []
    for b, left in base0.groupby("B_REF", sort=True):
        right = nt_long[nt_long["B_REF"] == b][[
            "DATA", "NTNB_YIELD"]].sort_values("DATA")
        joined = pd.merge_asof(left.sort_values("DATA"), right,
                               on="DATA", direction="backward")
        frames.append(joined)
    base = (pd.concat(frames, ignore_index=True)
              .dropna(subset=["NTNB_YIELD"]))

    base["SPREAD_PP"] = ((1+(base["TAX_INDIC"]/100)) /
                         (1+(base["NTNB_YIELD"]/100))) - 1
    base["SPREAD_PP"] = base["SPREAD_PP"] * 100
    base["DATA_DATE"] = base["DATA"].dt.date  # col. auxiliar p/ comparações

# ───────────────────────── helpers_spreads.py ──────────────────────────


@st.cache_data(show_spinner=False, ttl=4*3600)   # 4 h
def get_df_spread_ready(
        df_posicao: pd.DataFrame,
        df_extras:  pd.DataFrame,
        df_tx_hist: pd.DataFrame,
        df_ntnb:    pd.DataFrame,
        df_lookup:  pd.DataFrame
) -> pd.DataFrame:
    
    """
    Constrói a base completa para todos os gráficos de *Spreads Deb-B*.

    Retorna um DataFrame já pronto, contendo **apenas dias-úteis** e com as
    colunas-chave:

        DATA (datetime) • DATA_DATE (date) • Fundo • B_REF  
        TAX_INDIC • NTNB_YIELD • SPREAD_PP • DIV1_ATIVO

    Premissas novas
    --------------- 
    1. `df_posicao` **já** traz DIV1_ATIVO diário (cálculo feito na carga do
       parquet; ver `load_carteira_recent` revisto).  
    2. `df_tx_hist` tem as curvas dos ativos  
       (Ativo • DATA • TAX_INDIC).  
    3. `df_ntnb` é a curva longa das NTNBs  
       (DATA • B_REF • NTNB_YIELD).  
    4. `df_lookup` faz o mapeamento *Ativo → B_REF*.
    """

    # ─────────────────────────────────────────────────────────────────────
    # 1) posição + extras  → normaliza & garante datetime
    # --------------------------------------------------------------------
    # ── 1) posição + extras ────────────────────────────────────────────────
    df_pos = pd.concat([df_posicao, df_extras], ignore_index=True).copy()
    df_pos.rename(columns={"Data": "DATA_POS"},
                  inplace=True)          # ← rename
    df_pos["DATA_POS"] = pd.to_datetime(df_pos["DATA_POS"], errors="coerce")
    df_pos["Ativo_up"] = df_pos["Ativo"].str.upper().str.strip()

    # ── 2) lookup B_REF ────────────────────────────────────────────────────
    lk = df_lookup.copy()
    lk["Ativo_up"] = lk["Ativo"].str.upper().str.strip()

    # ── 3) histórico de taxas ─────────────────────────────────────────────
    tx = df_tx_hist.copy()
    tx.rename(columns={"DATA": "DATA_HIST"},
              inplace=True)             # ← rename
    tx["DATA_HIST"] = pd.to_datetime(tx["DATA_HIST"], errors="coerce")

    # ── 4) junta tudo (posição × lookup × taxas) ─────────────────────────
    base0 = (
        df_pos[["Ativo_up", "Fundo", "DIV1_ATIVO", "DATA_POS"]]
        .merge(lk[["Ativo_up", "B_REF"]], on="Ativo_up", how="left")
        .merge(tx[["Ativo", "DATA_HIST", "TAX_INDIC"]],
               left_on="Ativo_up", right_on="Ativo", how="left")
        .dropna(subset=["B_REF", "DATA_HIST", "TAX_INDIC"])
    )

    # ─────────────────────────────────────────────────────────────────────
    # 4) para cada vértice faz merge_asof com o yield da NTNB correspondente
    #    (tolerância ±30 dias, pega o pregão mais próximo <= DATA)
    # --------------------------------------------------------------------
    # ── 5) merge_asof com NTNB ────────────────────────────────────────────
    bases = []
    for b_ref, grp in base0.groupby("B_REF", sort=True):
        ntb_ref = df_ntnb.loc[df_ntnb["B_REF"]
                              == b_ref, ["DATA", "NTNB_YIELD"]]
        joined = pd.merge_asof(
            grp.sort_values("DATA_HIST"),
            ntb_ref.sort_values("DATA"),
            left_on="DATA_HIST", right_on="DATA",
            direction="nearest",
            tolerance=pd.Timedelta(days=30)
        )
        bases.append(joined)

    base = pd.concat(bases, ignore_index=True).dropna(subset=["NTNB_YIELD"])

    # ─────────────────────────────────────────────────────────────────────
    # 5) calcula spread, auxiliares e filtra fins-de-semana
    # --------------------------------------------------------------------
    base["SPREAD_PP"] = ((1 + (base["TAX_INDIC"]/100)) /
                         (1 + (base["NTNB_YIELD"]/100))) - 1
    base["SPREAD_PP"] = base["SPREAD_PP"] * 100
    base["DATA_DATE"] = base["DATA_HIST"].dt.date
    base = base[base["DATA_HIST"].dt.dayofweek < 5]     # 0=seg … 4=sex

    # ─────────────────────────────────────────────────────────────────────
    # 6) ordena / devolve
    # --------------------------------------------------------------------
    cols_core = [
        "DATA", "DATA_DATE", "Fundo", "Ativo_up", "B_REF",
        "TAX_INDIC", "NTNB_YIELD", "SPREAD_PP", "DIV1_ATIVO"
    ]
    outras = [c for c in base.columns if c not in cols_core]
    return base[cols_core + outras]

# ───────────────────── páginas / análise Spreads Deb-B ─────────────────────
def analisar_spreads_deb_b2(_: pd.DataFrame) -> None:
    """Usa df pronto em cache (não importa parâmetro enviado)."""

    # 1. dataframe pré-processado --------------------------------------------
    with st.spinner("Carregando dados..."):
        df_ready = get_df_spread_ready(
            st.session_state["df_carteira_recent"],
            st.session_state.get("df_extras", pd.DataFrame()),
            _load_tx_hist(),              # funções rápidas de leitura CSV
            _load_ntnb_long(),
            _load_lookup_spreads()
        )

    # 2. mini-gráficos -------------------------------------------------------
    def grafico_ativos(df: pd.DataFrame) -> None:
        st.subheader("Gráfico 1 – Spread por Ativo")
        ativos_disp = sorted(df["Ativo_up"].unique())
        sel = st.sidebar.multiselect(
            "Ativos a exibir:", ativos_disp, ativos_disp[:1])
        if not sel:
            st.info("Selecione ao menos um ativo.")
            return

        dfp = df.query("Ativo_up in @sel").copy()
        fig = go.Figure()
        for atv, g in dfp.groupby("Ativo_up"):
            fig.add_trace(go.Scatter(x=g["DATA"], y=g["SPREAD_PP"],
                                     mode="lines+markers",
                                     name=f"{atv} (B {g['B_REF'].iloc[0]})"))
        fig.update_layout(height=460, plot_bgcolor="white",
                          xaxis_title="Data", yaxis_title="Spread (p.p.)",
                          title="Evolução diária do spread (Ativo × B-ref)")
        st.plotly_chart(fig, use_container_width=True)

        # ▼ tabela opcional ----------------------------------------------------
        if st.checkbox("Mostrar dados do gráfico (Ativos)"):
            cols_show = ["DATA", "Ativo_up", "B_REF",
                         "TAX_INDIC", "NTNB_YIELD", "SPREAD_PP"]

            df_tbl = (
                dfp[cols_show]
                .drop_duplicates(subset=["Ativo_up", "DATA", "B_REF"])
                .rename(columns={"Ativo_up": "Ativo"})
                .assign(DATA=lambda d: d["DATA"].dt.strftime("%Y-%m-%d"))
                # índice único (MultiIndex)
                .set_index(["DATA", "Ativo"])
                .sort_index()
            )
            sty = (df_tbl.style
                   .format({                       # casas decimais
                       "SPREAD_PP": "{:.2f}",
                       "TAX_INDIC": "{:.4f}",
                       "NTNB_YIELD": "{:.4f}"
                   })
                   # ― visual ―
                   .set_table_styles([
                       {"selector": "thead th",
                        "props": [("background", "#1F4E79"),
                                  ("color", "white"),
                                  ("font-weight", "bold")]},
                       {"selector": "tbody tr:nth-child(even)",
                        "props": [("background", "#F2F2F2")]},
                       {"selector": "tbody tr:hover",
                        "props": [("background", "#D6EAF8")]}
                   ])
                   .set_properties(**{
                       "border":       "1px solid #DDD",
                       "text-align":   "right",
                       "white-space":  "nowrap"})
                   )

            st.table(sty)      # usa Styler ⇒ cabeçalhos fixos + formatação
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df_tbl.to_excel(writer, sheet_name="Spreads_Ativos")

            st.download_button(
                label="📥 Baixar tabela em Excel",
                data=buffer.getvalue(),
                file_name="spreads_ativos.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    

    def grafico_vertices_fundo(df: pd.DataFrame) -> None:
        st.subheader("Gráfico 2 – Spread do Fundo por Vértice")

        fundos = sorted(df["Fundo"].unique())
        st.sidebar.write('---')
        fundo = st.sidebar.selectbox("Fundo:", fundos)

        # datas disponíveis para o fundo
        datas_fundo = sorted(df.query("Fundo == @fundo")["DATA_DATE"].unique())
        if not datas_fundo:
            st.info("Fundo sem dados.")
            return

        # controles de data-ref
        col1, col2 = st.sidebar.columns(2)
        data_ref = col1.date_input("Data-ref 1:", value=datas_fundo[-1],
                                   min_value=datas_fundo[0], max_value=datas_fundo[-1])
        # comparar = col1.checkbox("Comparar com outra data?", value=True)
        comparar = True
        if not comparar:
            data_ref2 = None
        else:
            data_ref2 = col2.date_input("Data-ref 2:", value=datas_fundo[0],
                                        min_value=datas_fundo[0], max_value=datas_fundo[-1])
        spread_tipo = st.sidebar.radio("Tipo de média:",
                                       ("Ponderada", "Simples"), horizontal=True)

        # ----------  função que monta gráfico + tabela p/ uma data  -------------
        def _build_vert(dt):
            df_ref = df.query("Fundo == @fundo & DATA_DATE == @dt").copy()
            if df_ref.empty:
                return None, None

            if spread_tipo == "Ponderada":
                medias = (df_ref.groupby("B_REF")
                          .apply(lambda g: np.average(g["TAX_INDIC"],
                                                      weights=g["DIV1_ATIVO"]))
                          .reset_index(name="TAX_MED"))
            else:  # média simples
                medias = (df_ref.groupby("B_REF", as_index=False)["TAX_INDIC"]
                          .mean().rename(columns={"TAX_INDIC": "TAX_MED"}))

            ntb = (_load_ntnb_for_date(dt)
                   .rename(columns={"NTNB_YIELD": "NTNB"}))
            df_vert = medias.merge(ntb, on="B_REF", how="left")
            df_vert["SPREAD_PP"] = df_vert["TAX_MED"] - df_vert["NTNB"]

            # gráfico
            tit = "ponderado" if spread_tipo == "Ponderada" else "médio simples"
            fig = go.Figure(go.Bar(
                x=df_vert["B_REF"], y=df_vert["SPREAD_PP"],
                text=[f"{v:.2f}" if pd.notna(
                    v) else "" for v in df_vert["SPREAD_PP"]],
                textposition="outside", marker_color="#1F4E79"))
            fig.update_layout(height=520, plot_bgcolor="white",
                              title=f"{fundo} – Spread {tit} ({dt})",
                              xaxis_title="Vértice (B)",
                              yaxis_title="Spread (p.p.)")

            # tabela formatada
            tbl = (df_vert.set_index("B_REF").drop(columns=["DATA"], axis=1)
                          .style.format({"TAX_MED": "{:.4f}",
                                         "NTNB": "{:.4f}",
                                         "SPREAD_PP": "{:.2f}"}))
            return fig, tbl

        # ----------  render  -----------------------------------------------------
        if comparar and data_ref2:
            col_esq, col_meio, col_dir = st.columns([4.9, 0.2, 4.9])

            fig1, tbl1 = _build_vert(data_ref)
            fig2, tbl2 = _build_vert(data_ref2)

            with col_esq:
                if fig1:
                    st.plotly_chart(fig1, use_container_width=True)
                st.table(tbl1)

            with col_meio:
                st.markdown("<div style='border-left:2px solid #ccc;height:80vh;'></div>",
                            unsafe_allow_html=True)

            with col_dir:
                if fig2:
                    st.plotly_chart(fig2, use_container_width=True)
                st.table(tbl2)

        else:
            fig, tbl = _build_vert(data_ref)
            st.plotly_chart(fig, use_container_width=True)
            if tbl and st.checkbox("Mostrar tabela", key="tbl_single"):
                st.table(tbl)

    # ───────────────── mini-gráfico 3 – série temporal ──────────────────
    def grafico_serie_fundos(df: pd.DataFrame) -> None:
        st.subheader("Gráfico 3 – Série temporal do Spread médio (ponderado)")
        fundos = sorted(df["Fundo"].unique())
        st.sidebar.write('---')
        sel = st.sidebar.multiselect("Fundos:", fundos, fundos[:2])
        if not sel:
            st.info("Selecione ao menos um fundo.")
            return

        fig = go.Figure()
        for f in sel:
            def _m_pond(g):
                w = g["DIV1_ATIVO"].values
                return (np.average(g["SPREAD_PP"], weights=w)
                        if w.sum() else g["SPREAD_PP"].mean())

            serie = (df.query("Fundo == @f")
                       .groupby("DATA_DATE")
                       .apply(_m_pond)
                       .rename("SPREAD")
                       .reset_index())

            fig.add_trace(go.Scatter(x=serie["DATA_DATE"], y=serie["SPREAD"],
                                     mode="lines+markers", name=f))

        fig.update_layout(height=460, plot_bgcolor="white",
                          xaxis_title="Data", yaxis_title="Spread (p.p.)",
                          title="Spread médio ponderado – evolução por Fundo")
        st.plotly_chart(fig, use_container_width=True)

    # 3. layout em tabs -------------------------------------------------------
    tab1, tab2, tab3 = st.tabs(["Ativos", "Vértice/Fundo", "Série Fundos"])
    with tab1:
        grafico_ativos(df_ready)
    with tab2:
        grafico_vertices_fundo(df_ready)
    with tab3:
        grafico_serie_fundos(df_ready)
    if st.checkbox("DEBUG – ver DIV1 zeros"):
        z = st.session_state["df_carteira_recent"]
        st.write(z[z["DIV1_ATIVO"] == 0].head())


def _load_tx_hist():
    return (pd.read_csv("Dados/dados_ativos (01_01_2025-29_04_2025).csv")
              .rename(columns={"position_date": "DATA", "ativo": "Ativo",
                               "tax": "TAX_INDIC"})
              .assign(Ativo=lambda d: d["Ativo"].str.upper().str.strip(),
                      DATA=lambda d: pd.to_datetime(
                          d["DATA"], errors="coerce"),
                      TAX_INDIC=lambda d:
                          pd.to_numeric(d["TAX_INDIC"], errors="coerce"))
              .dropna(subset=["DATA", "TAX_INDIC"]))


@st.cache_data
def _load_ntnb_long():
    return _prep_ntnb_df("Dados/BBG - ECO DASH_te.xlsx")


def _load_ntnb_for_date(d):
    nt = _load_ntnb_long()
    return (nt[nt["DATA"].dt.date <= d]
            .sort_values(["B_REF", "DATA"])
            .groupby("B_REF").tail(1))


@st.cache_data
def _load_lookup_spreads():
    lk = load_spreads_afinvest()
    lk["Ativo"] = lk["Ativo"].str.upper().str.strip()
    return lk


# ═════ 2. GRÁFICO 1 – ATIVOS ═════════════════════════════════════════
# ──────────────────────────────────────────────────────────────────────────
def analisar_spreads_deb_b(df_posicao: pd.DataFrame) -> None:
    """
    • Gráfico 1 – Spread por ativo (linha)                    – opcional
    • Gráfico 2 – Spread médio ponderado por vértice/fundo    – opcional
    • Gráfico 3 – Série temporal do spread médio ponderado    – 1‑N fundos
    """
    st.header("Spreads Debêntures × NTNB‑B")
    # NOVO ➌ – junta a base principal com os ativos SEM FUNDO
    df_extras = st.session_state.get("df_extras", pd.DataFrame())
    df_posicao = pd.concat([df_posicao, df_extras], ignore_index=True).copy()

    def _spread_fundo_dia(df_dia: pd.DataFrame) -> float:
        # ① média ponderada da TAX_INDIC em cada vértice
        verts = (df_dia.groupby("B_REF")
                       .apply(lambda g: pd.Series({
                           "DIV1_SUM":  g["DIV1_ATIVO"].sum(),
                           "TAX_POND":  np.average(g["TAX_INDIC"],
                                                   weights=g["DIV1_ATIVO"])
                       }))
                 .reset_index())

        # ② NTNB mais recente ≤ data‑ref
        data_ref = df_dia["DATA_DATE"].iloc[0]
        ntb_ref = (nt_long[nt_long["DATA"].dt.date <= data_ref]
                   .sort_values(["B_REF", "DATA"])
                   .groupby("B_REF").tail(1)[["B_REF", "NTNB_YIELD"]])
        verts = verts.merge(ntb_ref, on="B_REF", how="left")
        verts["SPREAD_VERT"] = verts["TAX_POND"] - verts["NTNB_YIELD"]

        # ③ resultado ponderado pelos DIV1_SUM de cada vértice
        num = (verts["SPREAD_VERT"] * verts["DIV1_SUM"]).sum()
        den = verts["DIV1_SUM"].sum()
        return num / den if den else np.nan
    # ----------------------------------------------------------------

    # ═════ 0. CARREGAS BÁSICAS ═══════════════════════════════════════════
    tx_hist = (
        pd.read_csv("Dados/dados_ativos (01_01_2025-29_04_2025).csv")
          .rename(columns={"position_date": "DATA",
                           "ativo": "Ativo",
                           "tax": "TAX_INDIC"})
    )
    tx_hist["Ativo"] = tx_hist["Ativo"].str.upper().str.strip()
    tx_hist["DATA"] = pd.to_datetime(tx_hist["DATA"], errors="coerce")
    tx_hist["TAX_INDIC"] = pd.to_numeric(tx_hist["TAX_INDIC"], errors="coerce")
    tx_hist.dropna(subset=["DATA", "TAX_INDIC"], inplace=True)

    if "df_spreads_af" not in st.session_state:
        st.session_state["df_spreads_af"] = load_spreads_afinvest()
    lk = st.session_state["df_spreads_af"]

    if "df_ntnb_long" not in st.session_state:
        st.session_state["df_ntnb_long"] = _prep_ntnb_df(
            "BBG - ECO DASH_te.xlsx")

    # st.write(st.session_state["df_ntnb_long"])

    nt_long = st.session_state["df_ntnb_long"].copy()
    nt_long["DATA"] = pd.to_datetime(nt_long["DATA"], errors="coerce")
    nt_long["NTNB_YIELD"] = pd.to_numeric(
        nt_long["NTNB_YIELD"], errors="coerce")
    nt_long.dropna(subset=["DATA", "NTNB_YIELD"], inplace=True)

    vertices_disp = sorted(nt_long["B_REF"].unique())

    # ═════ 1. BASE GERAL COM SPREAD (para reutilizar) ════════════════════
    # ################################## PRECISO ADICIONAR UMA FORMA DE ANALISAR DUPLICIDADES COMO É FEITO NAS OUTRAS TELAS

    base0 = (
        df_posicao[["Ativo", "Fundo", "DIV1_ATIVO"]]
        .merge(lk, on="Ativo", how="left")
        .merge(tx_hist, on="Ativo", how="left")
        .dropna(subset=["B_REF", "DATA", "TAX_INDIC"])
    )

    # merge_asof por vértice
    frames = []
    for b, left in base0.groupby("B_REF", sort=True):
        right = nt_long[nt_long["B_REF"] == b][[
            "DATA", "NTNB_YIELD"]].sort_values("DATA")
        joined = pd.merge_asof(left.sort_values("DATA"), right,
                               on="DATA", direction="backward")
        frames.append(joined)
    base = (pd.concat(frames, ignore_index=True)
              .dropna(subset=["NTNB_YIELD"]))

    base["SPREAD_PP"] = base["TAX_INDIC"] - base["NTNB_YIELD"]
    base["DATA_DATE"] = base["DATA"].dt.date  # col. auxiliar p/ comparações

    # ======================================================================
    # CONTROLES GERAIS DE VISUALIZAÇÃO
    # ======================================================================
    st.sidebar.markdown("### O que exibir?")
    show_g1 = st.sidebar.checkbox("Gráfico 1 – Ativos", value=True)
    show_g2 = st.sidebar.checkbox("Gráfico 2 – Vértices do fundo", value=True)
    show_g3 = st.sidebar.checkbox("Gráfico 3 – Série por fundo(s)", value=True)

    # Lista de fundos disponível – usada nos dois últimos gráficos
    fundos_disp = sorted(base["Fundo"].unique())

    # ───────────────── 1. GRÁFICO 1 – SPREAD POR ATIVO ─────────────────────
    if show_g1:
        st.subheader("Gráfico 1 – Spread por Ativo")
        ativos_disp = sorted(base["Ativo"].unique())
        ativos_sel = st.sidebar.multiselect("Ativos:", ativos_disp,
                                            default=ativos_disp[:1])

        df_plot = (base[base["Ativo"].isin(ativos_sel)]
                   .assign(Data_Str=lambda x: x["DATA"].dt.strftime("%Y‑%m‑%d")))

        # col_g1, col_tbl1 = st.columns([3, 2])
        # with col_g1:
        fig = go.Figure()
        for atv, grp in df_plot.groupby("Ativo"):
            grp = grp.sort_values("DATA")
            fig.add_trace(go.Scatter(
                x=grp["Data_Str"], y=grp["SPREAD_PP"],
                mode="lines+markers",
                name=f"{atv} (B {grp['B_REF'].iloc[0]})"))
        fig.update_layout(height=520, plot_bgcolor="white",
                          xaxis_title="Data", yaxis_title="Spread (p.p.)")
        st.plotly_chart(fig, use_container_width=True)

        # with col_tbl1:
        #    st.dataframe(df_plot[["DATA","Ativo","B_REF","SPREAD_PP"]]
        #                 .sort_values("DATA")
        #                 .style.format({"SPREAD_PP":"{:.2f}"}))

        # Checkbox para mostrar tabela com os dados do gráfico
        if st.checkbox("Mostrar tabela com dados do gráfico", key="chk_g1_data"):
            st.dataframe(df_plot)
            st.dataframe(
                df_plot[["DATA", "Ativo", "B_REF", "SPREAD_PP"]]
                .drop_duplicates()
                .sort_values("DATA")
                .style.format({"SPREAD_PP": "{:.2f}"})   # ← corrigido
            )

    # ───────────────── 2. GRÁFICO 2 – VÉRTICE / FUNDO ──────────────────────
    if show_g2:
        st.subheader("Gráfico 2 – Spread médio ponderado por vértice")

        fundo_sel = st.sidebar.selectbox("Fundo (para o Gráfico 2):",
                                         fundos_disp)
        datas_fundo = sorted(
            base[base["Fundo"] == fundo_sel]["DATA_DATE"].unique())
        data_ref = st.sidebar.date_input("Data‑ref:",
                                         value=datas_fundo[-1],
                                         min_value=datas_fundo[0],
                                         max_value=datas_fundo[-1])

        df_ref = base[(base["Fundo"] == fundo_sel) &
                      (base["DATA_DATE"] == data_ref)].copy()
        if df_ref.empty:
            st.info("Fundo sem ativos nessa data.")
        else:
            medias = (df_ref.groupby("B_REF")
                      .apply(lambda g: np.average(g["TAX_INDIC"],
                                                  weights=g["DIV1_ATIVO"]))
                      .reset_index(name="TAX_POND"))
            pesos = df_ref.groupby("B_REF")["DIV1_ATIVO"].sum().reset_index()
            ntb_lookup = (nt_long[nt_long["DATA"].dt.date <= data_ref]
                          .sort_values(["B_REF", "DATA"])
                          .groupby("B_REF").tail(1)
                          .loc[:, ["B_REF", "NTNB_YIELD"]])

            df_vert = (medias.merge(ntb_lookup, on="B_REF", how="left")
                       .merge(pesos, on="B_REF", how="left"))
            df_vert["SPREAD_PP"] = df_vert["TAX_POND"] - df_vert["NTNB_YIELD"]

            col_g2, col_tbl2 = st.columns([5, 5])
            with col_g2:
                fig2 = go.Figure(go.Bar(
                    x=df_vert["B_REF"], y=df_vert["SPREAD_PP"],
                    text=[f"{v:.2f}" if pd.notna(
                        v) else "" for v in df_vert["SPREAD_PP"]],
                    textposition="outside", marker_color="#1F4E79"))
                fig2.update_layout(
                    title=f"{fundo_sel} – Spread ponderado ({data_ref})",
                    xaxis_title="Vértice (B)", yaxis_title="Spread (p.p.)",
                    height=520, plot_bgcolor="white")
                st.plotly_chart(fig2, use_container_width=True)
                st.table(
                    df_vert.set_index('B_REF').rename(
                        columns={"DIV1_ATIVO": "Σ DIV1"})
                    .style.format({"TAX_POND": "{:.4f}",
                                   "NTNB_YIELD": "{:.4f}",
                                   "SPREAD_PP": "{:.2f}",
                                   "Σ DIV1": "{:.0f}"}))

            # DEIXAR DUAS COLUNAS OPCIONAL. SE O USUÁRIOS ESCOLHER TER A VISÃO DO SPREAD MÉDIO SEM PONDERAÇÃO MOSTRAR O GRÁFICO ABAIXO E A TABELA, SENÃO MANTER O GRÁFICO E A TABELA ACIMA, MAS COMO DUAS COLUNAS EM VEZ DE UMA
            with col_tbl2:
                df_ref = base[(base["Fundo"] == fundo_sel) &
                              (base["DATA_DATE"] == data_ref)]

                # st.write(df_ref)

                if df_ref.empty:
                    st.info("Fundo sem ativos nessa data.")
                    return

                medias = (df_ref.groupby("B_REF", as_index=False)["TAX_INDIC"].mean()
                                .rename(columns={"TAX_INDIC": "TAX_MEAN"}))

                # Junta NTNB (último pregão <= data_ref)
                # ── médias por vértice (dict para evitar Series vazios) ─────────────
                tax_dict = (df_ref.groupby("B_REF")["TAX_INDIC"]
                            .mean()
                            .to_dict())                 # ex.: {'B25': 5.23, ...}

                # Somar os Div1_ATIVO por B_REF para cada fundo
                pesos_dict = df_ref.groupby(
                    "B_REF")["DIV1_ATIVO"].sum().reset_index()
                # st.write(pesos_dict)

                rows = []
                for b in vertices_disp:                         # garante todos os vértices
                    taxa_mean = tax_dict.get(b, None)

                    ntb = nt_long[(nt_long["B_REF"] == b) &
                                  (nt_long["DATA"].dt.date <= data_ref)]
                    ntb_y = ntb.iloc[-1]["NTNB_YIELD"] if not ntb.empty else None

                    spread = (taxa_mean - ntb_y
                              if pd.notna(taxa_mean) and pd.notna(ntb_y) else None)

                    rows.append({"B_REF": b,
                                "TAX_MEAN": taxa_mean,
                                 "NTNB_YIELD": ntb_y,
                                 "SPREAD_PP": spread})

                df_vert = pd.DataFrame(rows)
                # Dropar Nones in Taxa_mean
                df_vert = df_vert.dropna(subset=["TAX_MEAN"])

                # gráfico de barras
                fig2 = go.Figure(go.Bar(
                    x=df_vert["B_REF"],
                    y=df_vert["SPREAD_PP"],
                    text=[f"{v:.2f}" if pd.notna(
                        v) else "" for v in df_vert["SPREAD_PP"]],
                    textposition="outside",
                    marker_color="#1F4E79"))
                fig2.update_layout(
                    title=f"{fundo_sel} – Spread Médio por Vértice ({data_ref})",
                    xaxis_title="Vértice (B)",
                    yaxis_title="Spread (p.p.)",
                    height=520, plot_bgcolor="white")
                st.plotly_chart(fig2, use_container_width=True)

                # tabela
                st.table(
                    df_vert.set_index('B_REF').style.format({"TAX_MEAN": "{:.4f}",
                                                             "NTNB_YIELD": "{:.4f}",
                                                             "SPREAD_PP": "{:.2f}"})
                )

    # ───────────────── 3. GRÁFICO 3 – SÉRIE 1‑N FUNDOS ────────────────────
    if show_g3:
        st.subheader("Gráfico 3 – Evolução do spread médio ponderado")

        fundos_sel = st.sidebar.multiselect("Fundos para o Gráfico 3:",
                                            fundos_disp,
                                            default=[fundos_disp[0]])
        if not fundos_sel:
            st.info("Selecione pelo menos um fundo para o Gráfico 3.")
            return

        fig3 = go.Figure()
        for fundo in fundos_sel:
            df_fund = base[base["Fundo"] == fundo].copy()

            # ▼ NOVA série usando _spread_fundo_dia ▼
            serie = (df_fund.groupby("DATA_DATE", group_keys=False)
                     .apply(_spread_fundo_dia)
                     .reset_index(name="SPREAD_POND"))

            fig3.add_trace(go.Scatter(
                x=serie["DATA_DATE"], y=serie["SPREAD_POND"],
                mode="lines+markers", name=fundo))

        fig3.update_layout(
            height=480, plot_bgcolor="white",
            xaxis_title="Data", yaxis_title="Spread (p.p.)")
        st.plotly_chart(fig3, use_container_width=True)

        # (opção de mostrar tabela resumida)
        if st.checkbox("Mostrar série numérica (todos os fundos)"):
            big_tbl = None
            for fundo in fundos_sel:
                df_fund = base[base["Fundo"] == fundo].copy()
                serie = (df_fund.groupby("DATA_DATE", group_keys=False)
                         .apply(_spread_fundo_dia)
                         .rename(fundo))
                big_tbl = pd.concat([big_tbl, serie], axis=1)
            st.dataframe(big_tbl.style.format("{:.2f}"))


def analisar_spreads_por_fundo(df_posicao_juros: pd.DataFrame):
    """
    • Calcula pesos =  Σ DIV1_ATIVO por B_REF dentro do(s) fundo(s)
    • Aplica esses pesos aos spreads B/DAP (df_spread_melt e df_spread_vert)
    • Reproduz a mesma UI de 'analisar_spreads', mas já ponderada
    """
    st.subheader("Análise – Spreads ponderados por Fundo")

    # ══════════════ 0) ESCOLHA DE FUNDOS ═══════════════════════════════════
    fundos_disp = sorted(df_posicao_juros["Fundo"].unique())
    fundos_sel = st.sidebar.multiselect(
        "Fundos:", fundos_disp, default=fundos_disp)
    if not fundos_sel:
        st.warning("Selecione ao menos um fundo.")
        return

    # ══════════════ 1) PESOS DIV1 POR B_REF ════════════════════════════════
    # precisa do lookup (Ativo → B_REF)
    if "df_spreads_af" not in st.session_state:
        st.session_state["df_spreads_af"] = load_spreads_afinvest()
    df_lookup = st.session_state["df_spreads_af"][["Ativo", "B_REF"]]

    pesos = (df_posicao_juros[df_posicao_juros["Fundo"].isin(fundos_sel)]
             .merge(df_lookup, on="Ativo", how="inner")
             .groupby("B_REF", as_index=False)["DIV1_ATIVO"].sum())
    st.write(df_posicao_juros[df_posicao_juros["Fundo"].isin(fundos_sel)])

    st.write(df_lookup, pesos)
    if pesos.empty:
        st.write("Nenhum ativo dos fundos escolhidos existe na planilha de spreads.")
        return

    pesos["peso"] = pesos["DIV1_ATIVO"] / pesos["DIV1_ATIVO"].sum()
    # dicionário  { 'B25':0.17 , 'B35':0.22 , ... }

    w = dict(zip(pesos["B_REF"], pesos["peso"]))

    # ══════════════ 2) OBTÉM SPREADS ORIGINAIS (já no session_state) ═══════
    if "df_spread_melt" not in st.session_state:
        dfm, dfv = _prep_spread_df("Dados/BBG - ECO DASH_te.xlsx")
        st.session_state["df_spread_melt"] = dfm
        st.session_state["df_spread_vert"] = dfv

    df_melt_full = st.session_state["df_spread_melt"].copy()
    df_vert_full = st.session_state["df_spread_vert"].copy()

    # Apenas vértices que têm peso
    padrao = "|".join(sorted(w))                       # ex.: 'B25|B30|B35'
    df_melt_full = df_melt_full[
        df_melt_full["Tipo"].str.contains(padrao)
    ]
    df_vert_full = df_vert_full[
        df_vert_full["Vertice"].astype(str).isin([b[1:] for b in w])
    ]

    # ══════════════ 3) SIDEBAR (igual à função original) ═══════════════════
    with st.sidebar:
        st.write("---")
        st.markdown("## Filtros – Spreads ponderados")
        # ── se não sobrou nenhum spread depois do filtro ‘padrao’
        if df_melt_full.empty:
            st.warning(
                "Não há spreads para os vértices presentes nos fundos selecionados.")
            return

        min_date = df_melt_full["DATA"].min().date()
        max_date = df_melt_full["DATA"].max().date()

        col1, col2 = st.columns(2)
        start_date = col1.date_input("Início",  min_value=min_date,
                                     max_value=max_date, value=min_date)
        end_date = col2.date_input("Fim",     min_value=min_date,
                                   max_value=max_date, value=max_date)

        freq_map = {"Diária": "D", "Semanal": "W", "Mensal": "M"}
        freq = st.selectbox("Frequência", list(freq_map), 0)

    if start_date > end_date:
        st.warning("A data inicial é posterior à final – ajuste o intervalo.")
        return

    # ══════════════ 4) FILTRO DE PERÍODO & REAMOSTRAGEM ════════════════════
    mask = df_melt_full["DATA"].dt.date.between(start_date, end_date)
    df_melt = df_melt_full[mask].copy()

    if freq != "Diária":
        rule = freq_map[freq]
        wide = (df_melt.pivot(index="DATA", columns="Tipo", values="Spread")
                .resample(rule, label="right", closed="right")
                .median()
                .dropna(how="all"))
        df_melt = (wide.stack().rename("Spread").reset_index())
        df_melt["Data_Str"] = df_melt["DATA"].dt.strftime("%Y-%m-%d")

    # ══════════════ 5)  CÁLCULO DO SPREAD PONDERADO ════════════════════════
    #   5.1  extrai label 'B25' do campo 'Tipo' (Spread B25/DAP25)
    df_melt["B_REF"] = df_melt["Tipo"].str.extract(r"(B\d{2})")
    #   5.2  aplica peso
    df_melt["PESO"] = df_melt["B_REF"].map(w)
    df_melt["Spread_Pond"] = df_melt["Spread"] * df_melt["PESO"]

    #   5.3  série agregada
    serie = (df_melt.groupby("DATA", as_index=False)["Spread_Pond"].sum()
             .assign(Data_Str=lambda x: x["DATA"].dt.strftime("%Y-%m-%d")))

    # ══════════════ 6) GRÁFICO DE LINHA (unica curva) ══════════════════════
    fig_line = go.Figure(go.Scatter(
        x=serie["Data_Str"], y=serie["Spread_Pond"],
        mode="lines+markers+text",
        text=[f"{v:.2f}" if i % 5 == 0 else "" for i,
              v in enumerate(serie["Spread_Pond"])],
        textposition="top center",
        marker=dict(color="#1F4E79", size=6),
        name="Spread Médio Ponderado"))
    fig_line.update_layout(
        title="Evolução do Spread Médio Ponderado",
        xaxis_title="Data", yaxis_title="Spread (p.p.)",
        plot_bgcolor="white", height=450)
    st.plotly_chart(fig_line, use_container_width=True)

    # ══════════════ 7) BARRA POR VÉRTICE  (mesma data-ref da função original)═
    # Reutiliza df_vert_full já filtrado no intervalo
    st.write(df_vert_full)
    df_vert = df_vert_full[
        df_vert_full["DATA"].dt.date.between(start_date, end_date)
    ].copy()

    if freq != "Diária":
        rule = freq_map[freq]
        wide_v = (df_vert.pivot(index="DATA", columns="Vertice", values="Spread")
                  .resample(rule, label="right", closed="right")
                  .median()
                  .stack()
                  .rename("Spread")
                  .reset_index())
        df_vert = wide_v.assign(Vertice=lambda x: x["Vertice"].astype(int))

    # Sidebar – escolha do dia/semana/mês de referência
    # Ver se esse gráfico faz sentido então
    st.write('Ver se esse gráfico faz sentido')
    ordem = ["25", "26", "27", "28", "30", "32", "35", "40"]
    df_base = df_vert
    datas_disp = sorted(df_base["DATA"].dt.date.unique())

    with st.sidebar:
        if freq == "Diária":
            data_ref = st.date_input("Data de referência",
                                     value=datas_disp[-1],
                                     min_value=datas_disp[0],
                                     max_value=datas_disp[-1])
        else:
            rot = "Semana" if freq == "Semanal" else "Mês"
            data_ref = st.selectbox(f"{rot} de referência",
                                    datas_disp, index=len(datas_disp)-1)

    df_sel = df_base[df_base["DATA"].dt.date == data_ref]
    spread_map = dict(zip(df_sel["Vertice"].astype(str), df_sel["Spread"]))

    # aplica peso de cada vértice
    y_vals = []
    for v in ordem:
        b = f"B{v}"
        if b in w and v in spread_map:
            y_vals.append(spread_map[v] * w[b])   # já ponderado
        else:
            y_vals.append(None)

    fig_bar = go.Figure(go.Bar(
        x=ordem, y=y_vals,
        text=[f"{x:.2f}" if x is not None else "" for x in y_vals],
        textposition="outside",
        marker_color="#1F4E79"))
    fig_bar.update_layout(
        title=f"Contribuição por Vértice – {data_ref:%d/%m/%Y}",
        xaxis=dict(title="Vértice (anos)", type="category"),
        yaxis=dict(title="Spread ponderado (p.p.)"),
        plot_bgcolor="white", height=550, showlegend=False)
    st.plotly_chart(fig_bar, use_container_width=True)

    # ══════════════ 8) MOSTRA TABELA DE PESOS ════════════════════════════════
    st.write("Pesos DIV1 utilizados:")
    st.dataframe(
        pesos[["B_REF", "peso", "DIV1_ATIVO"]]
        .rename(columns={"B_REF": "B", "peso": "Peso", "DIV1_ATIVO": "Σ DIV1"})
        .style.format({"Peso": "{:.1%}", "Σ DIV1": "{:.0f}"}))


###############################################################################
# (Demais funções utilitárias, CSS e main() seguem SEM alterações relevantes,
#  apenas atualizando assinaturas onde plot_div1_layout foi chamado.)
###############################################################################

def add_custom_css():
    # CSS personalizado
    st.markdown(
        """
        <style>
        div[class="st-al st-bt st-bp st-bu st-bv st-bw st-bx st-by st-bz st-ak st-c0 st-c1 st-c2 st-bn st-c3 st-c4 st-c5 st-c6 st-c7 st-c8 st-c9"] input {
            color: black !important;  /* Altera a cor do texto */
        }
        div[data-baseweb="select"] div[class*="st-cy"] {
        color: black !important;
        }
        /* Altera o texto dentro da opção selecionada do primeiro dropdown */
        div[data-baseweb="select"] div[class*="st-cy"] {
            color: black !important;
        }

        /* Altera o texto do input do segundo dropdown */
        div[data-baseweb="select"] input[role="combobox"] {
            color: black !important;
        }
        div[role="listbox"] div {
        color: black !important;
        }
        div[class*="st-cc"] {
            color: black !important;
        }
        div.st-cc.st-bn.st-ar.st-cd.st-ce.st-cf {
            color: black !important;
        }
        /* Altera a cor de texto dos itens de opção (como "ACRC21") */
        div[data-baseweb="select"] div[role="option"] {
            color: black !important;
        }

         /* Alterar a cor de todo o texto na barra lateral */
        section[data-testid="stSidebar"] * {
            color: White; /* Cor padrão para textos na barra lateral */
        }

        div[class="stDateInput"] div[class="st-b8"] input {
            color: white;
        }
        div[role="presentation"] div {
            color: white;
        }

        div[data-baseweb="calendar"] button  {
            color:white;
        }
            
        /* Alterar a cor do texto no campo de entrada do st.number_input */
        input[data-testid="stNumberInput-Input"] {
            color: black !important; /* Define a cor do texto como preto */
        }

        input[data-testid="stNumberInputField"] {
            color: black !important; /* Define a cor do texto como preto */
        }

        /* Estiliza os botões de incremento e decremento */
        button[data-testid="stNumberInputStepDown"], 
        button[data-testid="stNumberInputStepUp"] {
            color: black !important; /* Define a cor do ícone ou texto como preto */
            fill: black !important;  /* Caso o ícone SVG precise ser estilizado */
        }

        /* Estiliza o ícone dentro dos botões */
        button[data-testid="stNumberInputStepDown"] svg, 
        button[data-testid="stNumberInputStepUp"] svg {
            fill: black !important;  /* Garante que os ícones sejam pretos */
        }
        
        /* Estiliza o fundo do container do multiselect */
        div[class="st-ak st-al st-bd st-be st-bf st-as st-bg st-bh st-ar st-bi st-bj st-bk st-bl"] {
            background-color: White !important; /* Altera o fundo para branco */
        }

        /* Estiliza o fundo do input dentro do multiselect */
        div[class="st-al st-bm st-bn st-bo st-bp st-bq st-br st-bs st-bt st-ak st-bu st-bv st-bw st-bx st-by st-bi st-bj st-bz st-bl st-c0 st-c1"] input {
            background-color: White !important; /* Altera o fundo do campo de entrada */
        }

        div[class="st-al st-bt st-bp st-bu st-bv st-bw st-bx st-by st-bz st-ak st-c0 st-c1 st-c2 st-bn st-c3 st-c4 st-c5 st-c6 st-c7 st-c8 st-c9"] input {
            color: black !important;  /* Altera a cor do texto */
        }
            /* Altera a cor de texto dos itens de opção (como "ACRC21") */
        div[data-baseweb="select"] div[role="option"] {
            color: black !important;
        }

        /* Altera a cor de texto dos itens já selecionados no multiselect */
        div[data-baseweb="select"] div[class*="st-bm"] {
            color: black !important;
        }

        /* Estiliza o fundo do botão ou elemento de "Escolher uma opção" */
        div[class="st-cc st-bn st-ar st-cd st-ce st-cf"] {
            background-color: White !important; /* Altera o fundo do botão de opção */
        }

        /* Estiliza o ícone dentro do botão de decremento */
        button[data-testid="stNumberInput-StepUp"] svg {
            color: black !important;
            fill: black !important;
        }
        button[data-testid="stNumberInput-StepDown"] svg {
            fill: black !important; /* Garante que o ícone seja preto */
        }

        div[data-testid="stNumberInput"] input {
            color: black; /* Define o texto como preto */
        }
        
        div[data-testid="stDateInput"] input {
            color: black;
        }
        div[class="st-al st-bt st-bp st-bu st-bv st-bw st-bx st-by st-bz st-ak st-c0 st-c1 st-c2 st-bn st-c3 st-c4 st-c5 st-c6 st-c7 st-c8 st-c9"] input {
            color: black !important;  /* Altera a cor do texto */
        }
        /* Altera a cor de texto dos itens de opção (como "ACRC21") */
        div[data-baseweb="select"] div[role="option"] {
            color: black !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # JavaScript para aplicar zoom de 80%
    components.html(
        """
        <script>
            document.body.style.zoom = "60%";
        </script>
        """,
        height=0,
    )


if __name__ == "__main__":
    # CSS pessoal, carregamento de dados e roteamento principal – inalterados
    add_custom_css()
    df_master = process_df()
    df_div1 = process_div01()

    st.sidebar.title("Escolha a visão")
    # modo = st.sidebar.radio(
    #    "Modo de análise",
    #    ["Analisar Ativo", "Analisar Fundo", "Análise Geral",
    #    "Analisar Spreads", "Analisar Spreads por Fundo",
    #    "Analisar Spreads Deb‑B","Analisar Spreads Deb-B2"])          # ← nova linha

    # ─── menu principal ──────────────────────────────────────────────
    modo = st.sidebar.radio(
        "Modo de análise",
        ["Analisar Ativo", "Analisar Fundo", "Análise Geral",
         "Analisar Spreads"]      # ❌ removemos o item “Analisar Spreads Deb-B2”
    )



    if modo == "Analisar Ativo":
        # assinatura original aceita apenas 2 args
        analisar_ativo(df_master, df_div1)
    elif modo == "Analisar Fundo":
        analisar_fundo(df_master, df_div1)
    elif modo == "Análise Geral":
        analisar_geral(df_master, df_div1)
    elif modo == "Analisar Spreads":
        # ── sub-menu interno ─────────────────────────────────────────
        tipo_spread = st.sidebar.radio(
            "Tipo de spread",
            ["NTNB × DAP", "Debêntures × NTNB-B"],
            key="sub_spread"
        )
        if tipo_spread == "NTNB × DAP":
            analisar_spreads()                     # ↩︎ continua igual
        else:                                      # Debêntures × NTNB-B
            if "df_carteira_recent" not in st.session_state:
                st.session_state["df_carteira_recent"] = load_carteira_recent()
            analisar_spreads_deb_b2(
                st.session_state["df_carteira_recent"])  # ← nova linha

    elif modo == "Analisar Spreads por Fundo":
        analisar_spreads_por_fundo(df_master)
    elif modo == "Analisar Spreads Deb‑B":
        analisar_spreads_deb_b(df_master)
    else:
        st.warning("Escolha uma opção válida.")
