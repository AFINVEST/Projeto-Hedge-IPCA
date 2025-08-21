"""
Script de fluxos para debêntures, CRI/CRA e exceções
===================================================

Como usar:
    1. Edite a lista ASSETS.
    2. Rode:  python deb_flow.py
    3. O arquivo deb_table_c_exc.csv é gerado igual antes.

Requisitos:
    pip install pandas selenium undetected-chromedriver pandas_market_calendars bs4
"""

from __future__ import annotations
import time
import io
import random
import sys
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Literal

import pandas as pd
import pandas_market_calendars as mcal
from bs4 import BeautifulSoup

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import selenium.webdriver as webdriver
import locale

# Define o locale temporariamente para pt_BR
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')  # Linux/Mac

# --------------------------------------------------------------------------- #
# 1. CONFIGURAÇÃO: basta mexer aqui                                          #
# --------------------------------------------------------------------------- #

LOGIN_AFINVEST = ("emanuel.cabral@afinvest.com.br", "Afs@2024")
LOGIN_XP = ("bruno.veloso@afinvest.com.br", "Afs@2023")

# Cada ativo precisa:
#   • code_xp  : o código ou ISIN usado no campo da calculadora da XP
#   • tab      : "DEB", "CRI" ou "CRA"  → botão de seleção na XP
#   • label    : como deve aparecer na coluna 'Ativo' (igual ao script antigo)
#   • rate_src : "scrape"  -> busca a taxa na página da AFinvest
#                "manual"  -> usa o valor definido em `rate`
#   • url      : (somente se rate_src=="scrape") URL da página interna AFinvest
#   • rate     : (somente se rate_src=="manual") taxa % a.a.

ASSETS: list[dict] = [

    # --------- EXCEÇÕES FIXAS (buscam taxa no site) ------------------------
    dict(code_xp="BRFS31", tab="DEB",
         label="BRFS31", rate_src="scrape",
         url="https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=6410&codativo=BRFS31"),

    dict(code_xp="CRTA12", tab="DEB",
         label="CRTA12", rate_src="scrape",
         url="https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=9358&codativo=CRTA12"),

    dict(code_xp="TBCR18", tab="DEB",
         label="TBCR18", rate_src="scrape",
         url="https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=3417&codativo=TBCR18"),

    dict(code_xp="CERT11", tab="DEB",
         label="CERT11", rate_src="scrape",
         url="https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=3417&codativo=CERT11"),

    # --------- ATIVOS CASUAIS (taxa informada manualmente) ----------------- (Manter ordem de DEB, CRI, CRA)
    #dict(code_xp="CART13", tab="DEB",
    #     label="CART13", rate_src="manual", rate=7.6077),

    # dict(code_xp="VALEB1", tab="DEB",
    #     label="VALEB1", rate_src="manual", rate=8.0343),

    # dict(code_xp ="ECHP11", tab="DEB",
    #     label="ECHP11", rate_src="manual", rate=8.214500),

    # dict(code_xp="FBRI13", tab="DEB",
    #     label="FBRI13", rate_src="manual", rate=7.9328),

    # dict(code_xp="EQPA18", tab="DEB",
    #     label="EQPA18", rate_src="manual", rate=7.7530),

    # dict(code_xp="CPLD29", tab="DEB",
    #     label="CPLD29", rate_src="manual", rate=7.6122),

    # dict(code_xp="AESOA1", tab="DEB",
    #     label="AESOA1", rate_src="manual", rate=7.7596),

    # --------- EXCEÇÕES FIXAS (buscam taxa no site) ------------------------
    dict(code_xp="21F0189140", tab="CRI",
         label="CRI Vic Engenharia 1ª Emissão", rate_src="scrape",
         url="https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=9081&codativo=CRI%20Vic%20Engenharia%201%C2%AA%20Emiss%C3%A3o"),

    dict(code_xp="21I0605705", tab="CRI",
         label="CRI Bem Brasil", rate_src="scrape",
         url="https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=2795&codativo=CRI%20Bem%20Brasil"),

    dict(code_xp="CRA021000SA", tab="CRA",
         label="CRA Ferroeste 2ª Série", rate_src="scrape",
         url="https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=1459&codativo=CRA%20Ferroeste%202%C2%AA%20S%C3%A9rie"),
]

# Arquivos de entrada/saída
CSV_DEB = Path("Dados/tabela_debentures222.csv")  # já existente
CSV_OUT = Path("Dados/deb_table_c_exc3.csv")

# --------------------------------------------------------------------------- #
# 2. AUXILIARES                                                               #
# --------------------------------------------------------------------------- #


def start_driver() -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1600,1000")
    return uc.Chrome(options=opts)


def wait_click(wd, locator, t=15):
    WebDriverWait(wd, t).until(EC.element_to_be_clickable(locator)).click()


def scrape_rate(driver: uc.Chrome, url: str, max_try: int = 10) -> float:
    """
    Faz scrape da taxa na página interna da AFinvest.

    • Se a taxa aparecer como “--”, recarrega a página e tenta novamente
      (intervalo fixo de 5 s entre tentativas).
    • Levanta ValueError depois de `max_try` falhas consecutivas.

    Parameters
    ----------
    driver   : uc.Chrome
    url      : str
    max_try  : int  | número máximo de tentativas antes de abortar

    Returns
    -------
    float  | taxa anual em formato decimal (ex.: 8.55 → 8.55)
    """
    for attempt in range(1, max_try + 1):
        if attempt < 2:
            driver.get(url)
            # espera o carregamento inicial (mantive sua folga extra)
            time.sleep(5)
        else:
            driver.get(url)
            time.sleep(5)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "tx_mtm_details"))
        )

        rate_txt = driver.find_element(By.ID, "tx_mtm_details").text.strip()

        if rate_txt != "--":                                    # ✓ deu certo
            return float(rate_txt.replace("%", "").replace(",", "."))

        # Caso ainda seja “--”, aguarda 5 s e tenta de novo
        print(
            f"[tentativa {attempt}/{max_try}] taxa ainda '--', recarregando…")
        time.sleep(5)

    raise ValueError(
        f"Não foi possível capturar a taxa em '{url}' após {max_try} tentativas.")


def xp_select_tab(driver: uc.Chrome, tab: str):
    """tab ∈ {'DEB','CRI','CRA'}"""
    tab_map = {"DEB": "DEBENTURE", "CRI": "CRI", "CRA": "CRA"}
    testid = tab_map[tab]
    wait_click(driver, (By.XPATH, f"//button[@data-testid='{testid}']"))


def xp_calculate(driver: uc.Chrome, code: str, rate: float) -> pd.DataFrame:
    input_id = {
        "DEB": "react-select-2-input",
        "CRI": "react-select-3-input",
        "CRA": "react-select-4-input",
    }
    # detect aba atual olhando qual input existe
    time.sleep(2)
    for tab_key, html_id in input_id.items():
        if driver.find_elements(By.ID, html_id):
            active_tab = tab_key
            break
    else:
        raise RuntimeError("Aba da calculadora XP não identificada.")

    campo = driver.find_element(By.ID, input_id[active_tab])
    campo.send_keys(code)
    time.sleep(0.8)
    campo.send_keys(Keys.ENTER)

    campo_taxa = driver.find_element(By.ID, "rate")
    campo_taxa.clear()
    time.sleep(0.3)
    campo_taxa.send_keys(Keys.BACKSPACE * 100 +
                         f"{rate:.2f}".replace(".", ","))

    wait_click(
        driver, (By.XPATH, "//button[@data-testid='calculator_submit_button']"))
    time.sleep(2)

    tabela = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "sc-1vu2ca6-4"))
    )
    linhas = tabela.find_elements(By.TAG_NAME, "tr")
    dados = [
        [td.text for td in ln.find_elements(By.TAG_NAME, "td")]
        for ln in linhas if ln.find_elements(By.TAG_NAME, "td")
    ]
    print(
        f"Capturando {len(dados)} linhas da tabela de {active_tab} - {code} - ({rate:.2f}%)")
    print(f"  {dados[0]}")
    print(f"  {dados[-1]}")
    return pd.DataFrame(dados, columns=["Data", "Tipo", "Valor Futuro"])

# --------------------------------------------------------------------------- #
# 3. LOGINs                                                                   #
# --------------------------------------------------------------------------- #
driver = webdriver.Chrome()


def login_afinvest():
    user, pwd = LOGIN_AFINVEST
    driver.get("https://afinvest.com.br/login/interno")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "atributo"))).send_keys(user)
    driver.find_element(By.ID, "passwordLogin").send_keys(pwd)
    driver.find_element(By.ID, "loginInterno").click()


def login_xp():
    user, pwd = LOGIN_XP
    driver.get("https://login-corporate.xpi.com.br/u/login?state=hKFo2SAxSUpBcktrb1FKRGZoVVRmYTg3ckMwSW1vbEZUbFI4dqFur3VuaXZlcnNhbC1sb2dpbqN0aWTZIDhDc2ZwRmN4US1WYnZsaDB5cGR5SWlDRkE1RDRXLTJjo2NpZNkgRTlORlFjUlJkdHdYOVJkOGg2alFZNTVBbldMSjBTRkQ")
    wait = WebDriverWait(driver, 20)
    wait.until(EC.presence_of_element_located(
        (By.ID, "username"))).send_keys(user)
    wait.until(EC.presence_of_element_located(
        (By.ID, "password"))).send_keys(pwd)
    wait_click(
        driver, (By.XPATH, "//button[@type='submit' and @name='action']"))
    time.sleep(3)
    # primeiro "Continuar"
    wait_click(driver, (By.XPATH, "//button[@data-testid='next-button']"))
    driver.set_window_size(400, 1300)
    driver.get(
        "https://www.xpinstitucional.com.br/plataforma/renda-fixa/#/credito-privado/calculadora")

# --------------------------------------------------------------------------- #
# 4. EXECUÇÃO                                                                 #
# --------------------------------------------------------------------------- #


try:
    driver.maximize_window()
    login_afinvest()
    # Captura todas as taxas
    for a in ASSETS:
        if a["rate_src"] == "scrape":
            a["rate"] = scrape_rate(driver, a["url"])
        # manual permanece
    login_xp()

    dfs = []
    current_tab = 'DEB'
    for a in ASSETS:
        if a["tab"] != current_tab:
            xp_select_tab(driver, a["tab"])
            current_tab = a["tab"]
        # Minimizar a janela para evitar problemas de renderização
        # driver.set_window_size(400, 1300)
        df_tmp = xp_calculate(driver, a["code_xp"], a["rate"])
        df_tmp["Ativo"] = a["label"]
        dfs.append(df_tmp)

    # Concatenar (mantém mesmo formato de antes)
    df_excecoes = pd.concat(dfs, ignore_index=True)

    # -------- tratamento igual ao script original -------------------------
    df_excecoes["Valor Futuro"] = (
        df_excecoes["Valor Futuro"]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    juros = df_excecoes[df_excecoes["Tipo"] == "Juros"]
    amort = df_excecoes[df_excecoes["Tipo"] != "Juros"]

    df_ja = pd.merge(
        juros[["Data", "Ativo", "Valor Futuro"]],
        amort[["Data", "Ativo", "Valor Futuro"]],
        on=["Data", "Ativo"], how="outer",
        suffixes=("_Juros", "_Amortizacao")
    )
    df_ja["Valor Futuro"] = df_ja["Valor Futuro_Juros"].fillna(
        0) + df_ja["Valor Futuro_Amortizacao"].fillna(0)
    df_ja["Tipo"] = "Juros|Amortização"

    df_final = (
        pd.concat([df_excecoes, df_ja[["Data", "Tipo", "Valor Futuro", "Ativo"]]])
          .drop_duplicates(subset=["Data", "Tipo", "Ativo"])
    )

    meses_pt_en = {"Jan": "Jan", "Fev": "Feb", "Mar": "Mar", "Abr": "Apr", "Mai": "May", "Jun": "Jun",
                   "Jul": "Jul", "Ago": "Aug", "Set": "Sep", "Out": "Oct", "Nov": "Nov", "Dez": "Dec"}
    for pt, en in meses_pt_en.items():
        df_final["Data"] = df_final["Data"].str.replace(pt, en)
    df_final["Data"] = pd.to_datetime(
        df_final["Data"], format="%d-%b-%Y", dayfirst=True)

    b3 = mcal.get_calendar("B3")
    today = datetime.now()
    df_final["Dias Úteis"] = df_final["Data"].apply(
        lambda x: len(b3.valid_days(start_date=today, end_date=x)) - 1
    )
    df_final["Dias"] = (df_final["Data"] - today).dt.days

    # tabela de taxas anuais
    taxas_anuais = {a["label"]: a["rate"]/100 if a["rate"]
                    > 1 else a["rate"] for a in ASSETS}
    df_final["Taxa"] = df_final["Ativo"].map(taxas_anuais)

    df_final = df_final.query("Tipo=='Juros|Amortização' and Dias>0")
    df_final["VP"] = df_final["Valor Futuro"] / \
        (1 + df_final["Taxa"]) ** (df_final["Dias Úteis"] / 252)

    # saída IDENTICA ao original
    deb = pd.read_csv(CSV_DEB)
    resultado = (df_final
                 .assign(**{
                     "Data": lambda d: d["Data"].dt.strftime("%d/%m/%Y"),
                     "Expectativa de juros (%)": "-",
                     "Dias entre pagamentos": "-",
                     "Juros projetados": "0",
                     "Amortizações": "0",
                     "Fluxo descontado (R$)": lambda d: d["VP"].map(lambda x: str(round(x, 2)).replace(".", ",")),
                     "Prazos (dias úteis)": df_final["Dias Úteis"],
                     "Dados do evento": df_final["Tipo"],
                     "Data de pagamento": df_final["Data"],
                 })
                 .drop(columns=["Valor Futuro", "Dias", "Taxa", "VP", "Tipo", "Dias Úteis"])
                 )
    # Renomear Data para Data de pagamento
    resultado.drop(columns=["Data"], inplace=True)
    # Transformar Data de pagamento em %d/%m/%Y
    resultado["Data de pagamento"] = pd.to_datetime(
        resultado["Data de pagamento"], format="%d-%b-%Y", dayfirst=True)
    resultado["Data de pagamento"] = resultado["Data de pagamento"].dt.strftime(
        "%d/%m/%Y")
    resultado = pd.concat([deb, resultado], ignore_index=True)
    resultado.to_csv(CSV_OUT, index=False)
    print(f"Arquivo '{CSV_OUT}' salvo com sucesso!")

finally:
    driver.quit()
    ntnb = pd.read_csv('Dados/ntnb.csv')
    ntnb['Valor presente'] = ntnb['Valor presente'].str.replace(
        '.', '').str.replace(',', '.').astype(float)
    # Passo 1: Traduzir 'J' e 'V' para texto completo
    ntnb["Tipo"] = ntnb["Tipo"].map(
        {"J": "Juros", "V": "Amortização", "A": "Amortização"})
    # Passo 2: Agrupar por Data e Ativo, agregando as colunas
    df_agrupado = ntnb.groupby(["Data", "Ativo"], as_index=False).agg({
        # Ordena para Juros|Amortização
        "Tipo": lambda x: "|".join(sorted(x.unique(), reverse=True)),
        "Dias úteis": "first",
        "Taxa": "first",
        "Valor futuro": "first",
        "Valor presente": "sum"
    })

    # Passo 3: Reordenar as colunas para manter a estrutura original
    colunas_originais = ["Data", "Tipo", "Dias úteis",
                         "Taxa", "Valor futuro", "Valor presente", "Ativo"]
    df_agrupado = df_agrupado[colunas_originais]
    # Ordenar por Data e Ativo
    df_agrupado = df_agrupado.sort_values(["Data"])
    df_agrupado.rename(columns={'Valor presente': 'Fluxo descontado (R$)', 'Data': 'Data de pagamento',
                       'Dias úteis': 'Prazos (dias úteis)', 'Tipo': 'Dados do evento'}, inplace=True)
    df_agrupado['Expectativa de juros (%)'] = '-'
    df_agrupado['Juros projetados'] = '0'
    df_agrupado['Amortizações'] = '0'
    df_agrupado['Dias entre pagamentos'] = '0'
    df_agrupado.drop(columns=['Taxa', 'Valor futuro'], inplace=True)
    df_agrupado['Fluxo descontado (R$)'] = df_agrupado['Fluxo descontado (R$)'].apply(
        lambda x: str(x).replace('.', ','))
    resultado = pd.concat([resultado, df_agrupado], ignore_index=True)
    resultado.to_csv('Dados/deb_table_completa2.csv', index=False)
    print(f"Arquivo 'deb_table_completa2.csv' salvo com sucesso!")
