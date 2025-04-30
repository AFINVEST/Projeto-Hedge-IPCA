# DashHedge_rev.py – revisão 2025‑04‑29
# -----------------------------------------------------------------------------
# ▸ NOVO: Contagem de contratos DAP já existentes (df_posicao) para cruzar com
#         a quantidade necessária calculada via DV01.
# ▸ Abrange visões "Analisar Fundo" e "Análise Geral".
# ▸ Mantém toda a lógica anterior – adições estão claramente marcadas com
#         «### DAP …» para facilitar busca.
# -----------------------------------------------------------------------------

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go  # reservado para uso futuro
import re
import io
from typing import Dict, List, Tuple
from plotnine import (
    ggplot, aes, geom_col, labs, theme, element_text, element_rect,
    scale_fill_manual, geom_text, geom_line, geom_point,
    scale_color_identity, geom_label
)

from os import PathLike


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
    mask_dap  = df_posicao_raw["Ativo"].astype(str).str.startswith("DAP", na=False)
    mask_estr = df_posicao_raw["Estratégia"].astype(str).str.contains("Hedge IPCA", na=False)
    df_dap = df_posicao_raw[mask_dap & mask_estr].copy()

    # ---------------------------------------------------------------------
    # 2) ► NOVO – extrai a quantidade que está dentro da string “Hedge IPCA (…)”
    # ---------------------------------------------------------------------
    mask_qtd = df_dap["Estratégia"].str.contains(r"Hedge IPCA\s*\(", na=False)
    df_dap.loc[mask_qtd, "Quantidade"] = (
        df_dap.loc[mask_qtd, "Estratégia"]
              .str.extract(r"Hedge IPCA\s*\(\s*([-+]?\d+)\s*\)")[0]  # pega o número entre parênteses
              .astype(int)
    )

    # ---------------------------------------------------------------------
    # 3) Continua igual: normaliza ticker, garante numérico, faz os groupbys
    # ---------------------------------------------------------------------
    df_dap["DAP"] = df_dap["Ativo"].apply(_normalizar_ticker_dap)
    df_dap.dropna(subset=["DAP"], inplace=True)

    # caso tenha linhas cujo “Quantidade” ainda esteja vazio, zera-as
    df_dap["Quantidade"] = pd.to_numeric(df_dap["Quantidade"], errors="coerce").fillna(0)

    by_fundo = df_dap.groupby(["Fundo", "DAP"], as_index=False)["Quantidade"].sum()
    total    = df_dap.groupby("DAP",          as_index=False)["Quantidade"].sum()
    return by_fundo, total


################################################################################
# FUNÇÕES DE CARGA / PROCESSAMENTO (bases originais + DAP extra)
################################################################################


def _prep_spread_df(path: str | PathLike) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lê o Excel de spreads (Planilha2) e devolve:
        • df_melt – longo, p/ gráfico em linha (Spread × Data)
        • df_vert – wide -> longo p/ gráfico Vertice × Spread (um dia)
    """
    dados = pd.read_excel(path, sheet_name="Planilha2")

    # renomeia & filtra zeros
    cols = ['DATA','DAP25','DAP26','DAP27','DAP28','DAP30','DAP32','DAP35','DAP40',
            'NTNB25','NTNB26','NTNB27','NTNB28','NTNB30','NTNB32','NTNB35','NTNB40']
    dados.columns = cols
    for c in cols[1:]:
        dados = dados[dados[c] != 0]

    # calcula spreads
    for y in ["25","26","27","28","30","32","35","40"]:
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


def process_df() -> pd.DataFrame:
    """Carrega posição + debêntures e devolve df_posicao_juros já agregado.
    Também armazena em session_state:
        • df_extras            – ativos IPCA fora da carteira
        • dap_counts_by_fundo  – contratos DAP hedge já existentes por fundo
        • dap_counts_total     – contratos DAP hedge já existentes consolidados
    """
    # --- Posição completa (sem filtros) --------------------------------------
    df_posicao_raw = pd.read_excel(
        "Dados/Relatório de Posição 2025-04-29.xlsx", sheet_name="Worksheet"
    )
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
    #     intocados em session_state)
    # ----------------------------------------------------------------
    df_posicao = df_posicao_raw[df_posicao_raw["Ativo"].isin(ativos)].copy()

    # --- restante da rotina original (formatação + merge) --------------------
    df_debentures.columns = [
        "Dados do evento", "Data de pagamento", "Prazos (dias úteis)",
        "Dias entre pagamentos", "Expectativa de juros (%)", "Juros projetados",
        "Amortizações", "Fluxo descontado (R$)", "Ativo",
    ]
    df_debentures = df_debentures[df_debentures["Ativo"].isin(df_posicao["Ativo"])]

    df_debentures["Data de pagamento"] = pd.to_datetime(df_debentures["Data de pagamento"])
    df_debentures["Data"] = df_debentures["Data de pagamento"].dt.strftime("%Y-%m")
    df_debentures["Ano"] = df_debentures["Data de pagamento"].dt.year
    df_debentures["Semestre"] = (
        df_debentures["Data de pagamento"].dt.quarter.replace({1: "1º Semestre", 2: "1º Semestre", 3: "2º Semestre", 4: "2º Semestre"})
    )

    df_quant = (
        df_posicao.groupby(["Fundo", "Ativo"]).sum()[["Quantidade", "Valor"]].reset_index()
    )
    df_quant["Valor"] = (
        df_quant["Valor"].astype(str).str.replace(",", ".").astype(float)
    )

    df_debentures.drop(columns=["Data de pagamento"], inplace=True)

    df_merged = pd.merge(df_debentures, df_quant, on="Ativo", how="left")
    df_merged["Juros projetados"] = df_merged["Fluxo descontado (R$)"] * df_merged["Quantidade"]
    df_merged["Amortizações"] = df_merged["Amortizações"] * df_merged["Quantidade"]
    df_merged["DIV1_ATIVO"] = df_merged["Juros projetados"] * 0.0001 * (df_merged["Prazos (dias úteis)"] / 252)
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
    df_debentures2["Data de pagamento"] = pd.to_datetime(df_debentures2["Data de pagamento"])
    df_debentures2["Data"] = df_debentures2["Data de pagamento"].dt.strftime("%Y-%m")
    df_debentures2["Ano"] = df_debentures2["Data de pagamento"].dt.year
    df_debentures2["Semestre"] = (
        df_debentures2["Data de pagamento"].dt.quarter.replace({1: "1º Semestre", 2: "1º Semestre", 3: "2º Semestre", 4: "2º Semestre"})
    )
    df_debentures2 = df_debentures2[~df_debentures2["Ativo"].isin(df_debentures["Ativo"].unique())]

    df_debentures2["Quantidade"] = 100
    df_debentures2["Fundo"] = "SEM FUNDO"
    df_debentures2["Juros projetados"] = df_debentures2["Fluxo descontado (R$)"] * df_debentures2["Quantidade"]
    df_debentures2["Amortizações"] = df_debentures2["Amortizações"] * df_debentures2["Quantidade"]
    df_debentures2["DIV1_ATIVO"] = df_debentures2["Juros projetados"] * 0.0001 * (df_debentures2["Prazos (dias úteis)"] / 252)
    df_debentures2["DAP"] = df_debentures2["Ano"].map(dap_dict)

    st.session_state["df_extras"] = df_debentures2.copy()

    # saída final da função
    return df_merged.rename(columns={"Data": "Data de pagamento"})


def process_div01() -> pd.DataFrame:
    df_div1 = pd.read_excel("Dados/AF_Trading.xlsm", sheet_name="Base IPCA", skiprows=16)
    df_div1 = df_div1.iloc[:, :13].dropna()[["DAP", "DV01"]]
    df_div1["DAP"] = df_div1["DAP"].apply(lambda x: x[:3] + x[-2:] if isinstance(x, str) and len(x) >= 5 else x)
    return df_div1


def obter_lista_ativos_original() -> List[str]:
    return ['RALM21','ACRC21', 'AEAB11', 'AEGP23', 'AESL17', 'AESLA5', 'AESLA7', 'AESLB7', 'AESOA1', 'AGVF12', 'AHGD13', 'ALGA27', 'ALGAB1', 'ALGAC2', 'ALGE16', 'ALGTA4', 'ALIG12', 'ALIG13', 'ALIG15', 'ALUP18', 'ANET11', 'ANET12', 'APFD19', 'APPSA2', 'APRB18', 'ARTR19', 'ASAB11', 'ASCP13', 'ASCP23', 'ASER12', 'ASSR21', 'ATHT11', 'ATII12', 'AURE12', 'BARU11', 'BCPSA5', 'BHSA11', 'BLMN12', 'BRFS31', 'BRKP28', 'BRST11', 'CAEC12', 'CAEC21', 'CAJS11', 'CAJS12', 'CART13', 'CASN23', 'CBAN12', 'CBAN32', 'CBAN52', 'CBAN72', 'CCLS11', 'CCROA5', 'CCROB4', 'CCROB6', 'CDES11', 'CEAD11', 'CEAP12', 'CEAP14', 'CEAP17', 'CEAR26', 'CEEBA1', 'CEEBB6', 'CEEBB7', 'CEEBC3', 'CEEBC4', 'CEED12', 'CEED13', 'CEED15', 'CEED17', 'CEED21', 'CEMT19', 'CEPE19', 'CEPEB3', 'CEPEC1', 'CEPEC2', 'CESE32', 'CESPA2', 'CESPA3', 'CGASA1', 'CGASA2', 'CGASB1', 'CGMG18', 'CGOS13', 'CGOS16', 'CGOS24', 'CGOS28', 'CGOS34', 'CHSF13', 'CJEN13', 'CLAG13', 'CLCD26', 'CLCD27', 'CLNG11', 'CLTM14', 'CMGD27', 'CMGD28', 'CMGDB0', 'CMGDB1', 'CMIN11', 'CMIN12', 'CMIN21', 'CMIN22', 'CMTR29', 'CNRD11', 'COCE18', 'COMR14', 'COMR15', 'CONF11', 'CONX12', 'CPFGA2', 'CPFPA0', 'CPFPA5', 'CPFPA7', 'CPFPB7', 'CPGT15', 'CPGT26', 'CPGT27', 'CPGT28', 'CPLD15', 'CPLD26', 'CPLD29', 'CPLD37', 'CPTM15', 'CPXB22', 'CRMG15', 'CRTR12', 'CSAN33', 'CSMGA2', 'CSMGA6', 'CSMGB4', 'CSMGB8', 'CSMGB9', 'CSMGC3', 'CSNAA4', 'CSNAA5', 'CSNAA6', 'CSNAB4', 'CSNAB5', 'CSNAB6', 'CSNAC4', 'CSNP12', 'CSRN19', 'CSRN29', 'CSRNA1', 'CSRNB2', 'CSRNC0', 'CTEE17', 'CTEE18', 'CTEE1B', 'CTEE29', 'CTEE2B', 'CTGE11', 'CTGE13', 'CTGE15', 'CTNS14', 'CTRR11', 'CUTI11', 'CXER12', 'DESK17', 'EBAS13', 'EBENA8', 'ECER12', 'ECHP11', 'ECHP12', 'ECHP22', 'ECOV16', 'ECPN11', 'EDFT11', 'EDPA11', 'EDPT11', 'EDTE12', 'EDVP14', 'EDVP17', 'EEELA0', 'EEELA1', 'EEELB1', 'EGIE17', 'EGIE19', 'EGIE27', 'EGIE29', 'EGIE39', 'EGIE49', 'EGIEA0', 'EGIEA1', 'EGIEB1', 'EGIEB2', 'EGVG11', 'EGVG21', 'EKTRB3', 'EKTRC0', 'EKTRC1', 'EKTT11', 'ELEK37', 'ELET14', 'ELET16', 'ELET23', 'ELET42', 'ELPLA5', 'ELPLA7', 'ELPLB4', 'ELTN15', 'ENAT11', 'ENAT12', 'ENAT13', 'ENAT14', 'ENAT24', 'ENAT33', 'ENERA1', 'ENERB4', 'ENEV13', 'ENEV15', 'ENEV16', 'ENEV18', 'ENEV19', 'ENEV26', 'ENEV28', 'ENEV29', 'ENEV32', 'ENEV39', 'ENEVA0', 'ENEVB0', 'ENGI39', 'ENGIA1', 'ENGIA4', 'ENGIA5', 'ENGIA6', 'ENGIA9', 'ENGIB0', 'ENGIB2', 'ENGIB4', 'ENGIB6', 'ENGIB9', 'ENGIC0', 'ENJG21', 'ENMI21', 'ENMTA3', 'ENMTA4', 'ENMTA5', 'ENMTA7', 'ENMTB3', 'ENMTB5', 'ENSEA1', 'ENTV12', 'EQMAA0', 'EQMAA2', 'EQPA18', 'EQSP11', 'EQSP21', 'EQTC11', 'EQTN11', 'EQTR11', 'EQTR21', 'EQTS11', 'EQUA11', 'ERDV17', 'ERDV38', 'ERDVA4', 'ERDVB4', 'ERDVC3', 'ERDVC4', 'ESAM14', 'ESULA1', 'ESULA6', 'ETAP22', 'ETBA12', 'ETEN11', 'ETEN12', 'ETEN21', 'ETEN22', 'ETEN31', 'ETSP12', 'EUBE11', 'EXTZ11', 'FBRI13', 'FGEN13', 'FLCLA0', 'FRAG14', 'FURN21', 'GASC15', 'GASC16', 'GASC17', 'GASC22', 'GASC23', 'GASC25', 'GASC26', 'GASC27', 'GASP19', 'GASP29', 'GASP34', 'GBSP11', 'GEPA28', 'GRRB24', 'GSTS14', 'GSTS24', 'HARG11', 'HBSA11', 'HBSA21', 'HGLB23', 'HVSP11', 'HZTC14', 'IBPB11', 'IGSN15', 'IRJS14', 'IRJS15', 'ITPO14', 'IVIAA0', 'JALL11', 'JALL13', 'JALL14', 'JALL15', 'JALL21', 'JALL24', 'JSMLB5', 'JTEE11', 'JTEE12', 'KLBNA5', 'LCAMD1', 'LCAMD3', 'LGEN11', 'LORTA7', 'LSVE39', 'LTTE15', 'MEZ511', 'MGSP12', 'MNAU13', 'MOVI18', 'MOVI37', 'MRSAA1', 'MRSAA2', 'MRSAB1', 'MRSAB2', 'MRSAC1', 'MRSAC2', 'MSGT12', 'MSGT13', 'MSGT23', 'MSGT33', 'MTRJ19', 'MVLV16', 'NEOE16', 'NEOE26', 'NMCH11', 'NRTB11', 'NRTB21', 'NTEN11', 'ODTR11', 'ODYA11', 'OMGE12', 'OMGE22', 'OMGE31', 'OMGE41', 'OMNG12', 'ORIG11', 'PALF38', 'PALFA3', 'PALFB3', 'PASN12', 'PEJA11', 'PEJA22', 'PEJA23', 'PETR16', 'PETR17', 'PETR26', 'PETR27', 'PLSB1A', 'POTE11', 'POTE12', 'PPTE11', 'PRAS11', 'PRPO12', 'PRTE12', 'PTAZ11', 'QUAT13', 'RAHD11', 'RAIZ13', 'RAIZ23', 'RATL11', 'RDOE18', 'RDVE11', 'RECV11', 'RESA14', 'RESA15', 'RESA17', 'RESA27', 'RIGEA3', 'RIPR22', 'RIS412', 'RIS414', 'RIS422', 'RIS424', 'RISP12', 'RISP14', 'RISP22', 'RISP24', 'RMSA12', 'RRRP13', 'RSAN16', 'RSAN26', 'RSAN34', 'RSAN44', 'RUMOA2', 'RUMOA3', 'RUMOA4', 'RUMOA5', 'RUMOA6', 'RUMOA7', 'RUMOB1', 'RUMOB3', 'RUMOB5', 'RUMOB6', 'RUMOB7', 'SABP12', 'SAELA1', 'SAELA3', 'SAELB3', 'SAPR10', 'SAPRA2', 'SAPRA3', 'SAPRB3', 'SAVI13', 'SBSPB6', 'SBSPC4', 'SBSPC6', 'SBSPD4', 'SBSPE3', 'SBSPE9', 'SBSPF3', 'SBSPF9', 'SERI11', 'SMTO14', 'SMTO24', 'SNRA13', 'SPRZ11', 'SRTI11', 'STBP35', 'STBP45', 'STRZ11', 'SUMI17', 'SUMI18', 'SUMI37', 'SUZB19', 'SUZB29', 'SUZBA0', 'SUZBC1', 'TAEB15', 'TAEE17', 'TAEE18', 'TAEE26', 'TAEEA2', 'TAEEA4', 'TAEEA7', 'TAEEB2', 'TAEEB4', 'TAEEC2', 'TAEEC4', 'TAEED2', 'TAES15', 'TBEG11', 'TBLE26', 'TCII11', 'TEPA12', 'TIET18', 'TIET29', 'TIET39', 'TIMS12', 'TNHL11', 'TOME12', 'TPEN11', 'TPNO12', 'TPNO13', 'TRCC11', 'TRGO11', 'TRPLA4', 'TRPLA7', 'TRPLB4', 'TRPLB7', 'TSSG21', 'TVVH11', 'UHSM12', 'UNEG11', 'UNTE11', 'USAS11', 'UTPS11', 'UTPS12', 'UTPS21', 'UTPS22', 'VALE38', 'VALE48', 'VALEA0', 'VALEB0', 'VALEC0', 'VAMO33', 'VAMO34', 'VBRR11', 'VDBF12', 'VDEN12', 'VERO12', 'VERO13', 'VERO24', 'VERO44', 'VLIM13', 'VLIM14', 'VLIM15', 'VLIM16', 'VPLT12', 'VRDN12', 'WDPR11', 'XNGU17']


def obter_lista_outros_original() -> List[str]:
    return [
        "BRFS31", "CRA Ferroeste 2ª Série", "CRI Bem Brasil", "NTN-B26", "NTN-B28",
        "NTN-B30", "NTN-B32", "NTN-B50", "CRI Vic Engenharia 1ª Emissão", "TBCR18",
        "CRTA12", "CERT11", "CRI PERNAMBUCO 35ª (23J1753853)", "CRI Vic Engenharia 2ª Emissão",
    ]


def obter_dap_dict_original() -> Dict[int, str]:
    return {
        2025: "DAP25", 2026: "DAP26", 2027: "DAP27", 2028: "DAP28", 2029: "DAP29",
        2030: "DAP30", 2031: "DAP30", 2032: "DAP32", 2033: "DAP32", 2034: "DAP35",
        2035: "DAP35", 2036: "DAP35", 2037: "DAP35", 2038: "DAP40", 2039: "DAP40",
        2040: "DAP40", 2041: "DAP40", 2042: "DAP40", 2043: "DAP40", 2044: "DAP40", 2045: "DAP40", 2046 : "DAP40", 2047 : "DAP40", 2048 : "DAP40", 2049 : "DAP40", 2050 : "DAP40", 2051 : "DAP40", 2052 : "DAP40", 2053 : "DAP40", 2054 : "DAP40", 2055 : "DAP40", 2056 : "DAP40", 2057 : "DAP40", 2058 : "DAP40", 2059 : "DAP40", 2060 : "DAP40"
    }


###############################################################################
# UTILITÁRIOS DE UI (sem alterações exceto import CSS)
###############################################################################
# ... (o restante das funções check_duplicates, filtro_generico, etc. permanecem
# inalteradas – suprimidas aqui por brevidade, mas integram o arquivo completo)
###############################################################################
# VISUALIZAÇÕES – ATUALIZAÇÃO PARA TRAZER DAP CARTEIRA
###############################################################################


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
    p = (
        ggplot(df_plot, aes(x="factor(Ano)",
               y="Juros projetados (R$ mil)", fill="Semestre"))
        + geom_col(position="dodge")
        + scale_fill_manual(values=["#1F4E79", "#A5C8E1"])
        + labs(title="Juros + Amortização Projetados por Ano/Semestre",
               x="Ano", y="Juros (R$ mil)")
        + theme(figure_size=(10, 4), axis_text_x=element_text(rotation=45, ha="right"),
                panel_background=element_rect(fill="white"), plot_background=element_rect(fill="white"))
    )
    st.pyplot(p.draw(), use_container_width=True)



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
            + geom_text(aes(label=df_sum["DIV1_ATIVO"].apply(lambda x: f"{x:,.0f}")), va="bottom", size=8)
            + labs(title="DIV1 vs DAP", x="DAP", y="DIV1 (R$)")
            + theme(figure_size=(6, 4), axis_text_x=element_text(rotation=45, ha="right"),
                    panel_background=element_rect(fill="white"))
        )
        st.pyplot(p1.draw(), use_container_width=True)

    with col2:
        st.html("<div style='border-left:2px solid rgba(49,51,63,0.2);height:60vh;margin:auto'></div>")

    with col3:
        df_fmt = df_sum.copy() 
        # Arredonda CONTRATOS
        df_fmt["CONTRATOS"] = df_fmt["CONTRATOS"].round().astype(int)
        if "CARTEIRA" in df_fmt.columns:
            df_fmt["CARTEIRA"] = df_fmt["CARTEIRA"].round().astype(int)
            df_fmt["FALTAM"] = df_fmt["FALTAM"].round().astype(int)
        df_fmt.set_index("DAP", inplace=True)

        # Totais
        totais = df_fmt.sum(numeric_only=True)
        df_fmt.loc["Total"] = totais

        # Formatação
        sty = df_fmt.style.format("{:,.0f}")
        sty = sty.set_table_styles([
            {"selector": "th", "props": [("font-weight", "bold"), ("text-align", "center")]},
            {"selector": "td", "props": [("text-align", "right")]},
            {"selector": "caption", "props": [("font-size", "16px"), ("font-weight", "bold")]},
        ])
        st.table(sty.set_caption("Tabela de DAPs: Necessário × Carteira"))

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
    fundo_sel = st.sidebar.selectbox("Selecione o fundo:", sorted(df["Fundo"].unique()))
    df_fundo = df[df["Fundo"] == fundo_sel].copy()


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

    # … (opção de mostrar base do fundo permanece)


def analisar_geral(df: pd.DataFrame, df_div1: pd.DataFrame):
    st.header("Análise Geral – Consolidado de Fundos")

    # --- Seleção múltipla de fundos (igual)
    todos_fundos = sorted(df["Fundo"].unique())
    fundos_sel = st.sidebar.multiselect("Escolha os fundos:", todos_fundos, default=None)
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
        # Colocar um aviso de os ativos fora da cartera foram atualizados com as taxas do dia 25 de abril
        if "SEM FUNDO" in df_sel["Fundo"].unique():
            st.warning(
                "Ativos fora da carteira foram atualizados com as taxas do dia 25 de abril.")

def analisar_spreads() -> None:
    """
    Parte de spreads NTNB × DAP – filtros, linha Plotly, barras Plotly.
    """
    st.subheader("Análise – Spreads NTNB × DAP")

    # ▸ carrega uma vez -----------------------------------------------------
    if "df_spread_melt" not in st.session_state:
        df_melt_tmp, df_vert_tmp = _prep_spread_df("BBG - ECO DASH_te.xlsx")
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
        end_date   = col_b.date_input("Fim",    min_value=min_date,
                                      max_value=max_date, value=max_date,
                                      key="hs_end")

        freq_map = {"Diária": "D", "Semanal": "W", "Mensal": "M"}
        freq = st.selectbox("Frequência", list(freq_map), 0)

    if start_date > end_date:
        st.warning("A data inicial é posterior à final – ajuste o intervalo.")
        return

    # ▸ aplica período & reamostra -----------------------------------------
    mask     = df_melt_full["DATA"].dt.date.between(start_date, end_date)
    df_melt  = df_melt_full[mask].copy()
    df_vert  = df_vert_full[df_vert_full["DATA"].dt.date
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
    pares     = sorted(df_melt["Tipo"].unique())
    media_lbl = "Média dos Spreads"
    opcoes    = [media_lbl] + pares

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
        text   = ['' if i % 5 else f'{v:.2f}' for i, v in enumerate(grp["Spread"])] \
                 if len(selecionados) == 1 else None
        modo   = "lines+markers+text" if text else "lines+markers"

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

    ordem   = ["25", "26", "27", "28", "30", "32", "35", "40"]
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
            rotulo   = "Semana" if agg_opt == "Mediana Semanal" else "Mês"
            data_ref = st.selectbox(f"{rotulo} de referência",
                                    datas_disp,
                                    index=len(datas_disp) - 1,
                                    key="hs_bar_refsel")

    df_sel     = df_base[df_base["DATA"].dt.date == data_ref]
    spread_map = dict(zip(df_sel["Vertice"].astype(str), df_sel["Spread"]))
    y_vals     = [spread_map.get(v, None) for v in ordem]
    text_vals  = [f"{v:.2f}" if v is not None else "" for v in y_vals]

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
    modo = st.sidebar.radio("Modo de análise", ["Analisar Ativo", "Analisar Fundo", "Análise Geral","Analisar Spreads"], index=0)

    if modo == "Analisar Ativo":
        analisar_ativo(df_master, df_div1)  # assinatura original aceita apenas 2 args
    elif modo == "Analisar Fundo":
        analisar_fundo(df_master, df_div1)
    elif modo == "Análise Geral":
        analisar_geral(df_master, df_div1)
    else:
        analisar_spreads()