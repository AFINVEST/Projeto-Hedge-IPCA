import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
from bs4 import BeautifulSoup  # futuras extensões

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support   import expected_conditions as EC

# ---------------------------------------------------------------------------
# 1. CONFIGURAÇÕES BÁSICAS                                                   |
# ---------------------------------------------------------------------------

LOGIN_AFINVEST   = ("emanuel.cabral@afinvest.com.br", "Afs@2024")
LOGIN_XP         = ("bruno.veloso@afinvest.com.br", "Afs@2023")


#: Ativos a adicionar  –  **edite aqui** ====================================
ADD_ASSETS: list[dict] = [
    # Exemplo de teste solicitado
    dict(code_xp="VERO13", tab="DEB", label="VERO13", 
         rate_src="manual", rate=9.3292),

]

# Caminhos de arquivos
CSV_BASE_DEB = Path("Dados/tabela_debentures222.csv")   # usado p/ colunas‑modelo
CSV_FINAL    = Path("Dados/deb_table_completa2.csv")    # sofrerá append

# ---------------------------------------------------------------------------
# 2. FUNÇÕES UTILITÁRIAS (copiadas/adaptadas do script original)             |
# ---------------------------------------------------------------------------

def start_driver() -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1600,1000")
    return uc.Chrome(options=opts)

def wait_click(wd, locator, t=15):
    WebDriverWait(wd, t).until(EC.element_to_be_clickable(locator)).click()

def scrape_rate(driver: uc.Chrome, url: str, max_try: int = 8) -> float:
    """Captura taxa % na página interna da AFinvest."""
    for attempt in range(1, max_try + 1):
        driver.get(url)
        time.sleep(3 if attempt == 1 else 5)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "tx_mtm_details"))
        )
        txt = driver.find_element(By.ID, "tx_mtm_details").text.strip()
        if txt != "--":
            return float(txt.replace("%", "").replace(",", "."))
        print(f"[tentativa {attempt}/{max_try}] taxa '--', tentando de novo…")
    raise ValueError("Não foi possível capturar a taxa na AFinvest.")

def xp_select_tab(driver: uc.Chrome, tab: str):
    tab_map = {"DEB": "DEBENTURE", "CRI": "CRI", "CRA": "CRA"}
    wait_click(driver, (By.XPATH, f"//button[@data-testid='{tab_map[tab]}']"))

def xp_calculate(driver: uc.Chrome, code: str, rate: float) -> pd.DataFrame:
    input_id = {"DEB": "react-select-2-input", "CRI": "react-select-3-input", "CRA": "react-select-4-input"}
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
    campo_taxa.send_keys(Keys.BACKSPACE * 50 + f"{rate:.2f}".replace(".", ","))

    wait_click(driver, (By.XPATH, "//button[@data-testid='calculator_submit_button']"))
    time.sleep(2)

    tabela = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "sc-1vu2ca6-4"))
    )
    linhas = tabela.find_elements(By.TAG_NAME, "tr")
    dados = [
        [td.text for td in ln.find_elements(By.TAG_NAME, "td")]
        for ln in linhas if ln.find_elements(By.TAG_NAME, "td")
    ]
    return pd.DataFrame(dados, columns=["Data", "Tipo", "Valor Futuro"])

# ---------------------------------------------------------------------------
# 3. LOGINs                                                                  |
# ---------------------------------------------------------------------------

def login_afinvest(driver):
    user, pwd = LOGIN_AFINVEST
    driver.get("https://afinvest.com.br/login/interno")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "atributo"))
    ).send_keys(user)
    driver.find_element(By.ID, "passwordLogin").send_keys(pwd)
    driver.find_element(By.ID, "loginInterno").click()


def login_xp(driver):
    user, pwd = LOGIN_XP
    driver.get("https://login-corporate.xpi.com.br/u/login?state=hKFo2SAxSUpBcktrb1FKRGZoVVRmYTg3ckMwSW1vbEZUbFI4dqFur3VuaXZlcnNhbC1sb2dpbqN0aWTZIDhDc2ZwRmN4US1WYnZsaDB5cGR5SWlDRkE1RDRXLTJjo2NpZNkgRTlORlFjUlJkdHdYOVJkOGg2alFZNTVBbldMSjBTRkQ")
    w = WebDriverWait(driver, 20)
    w.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(user)
    w.until(EC.presence_of_element_located((By.ID, "password"))).send_keys(pwd)
    wait_click(driver, (By.XPATH, "//button[@type='submit' and @name='action']"))
    time.sleep(3)
    wait_click(driver, (By.XPATH, "//button[@data-testid='next-button']"))
    driver.set_window_size(400, 1300)
    driver.get("https://www.xpinstitucional.com.br/plataforma/renda-fixa/#/credito-privado/calculadora")

# ---------------------------------------------------------------------------
# 4. EXECUÇÃO                                                                |
# ---------------------------------------------------------------------------

def main():
    if not ADD_ASSETS:
        print("Lista ADD_ASSETS vazia – nada a fazer.")
        return

    driver = start_driver()
    try:
        # 1) Captura de taxas (scrape ou manual)
        precisa_scrape = any(a["rate_src"] == "scrape" for a in ADD_ASSETS)
        if precisa_scrape:
            print("Logando na AFinvest…")
            login_afinvest(driver)
            for a in ADD_ASSETS:
                if a["rate_src"] == "scrape":
                    a["rate"] = scrape_rate(driver, a["url"])
                    print(f"  ✓ {a['label']} – taxa capturada: {a['rate']:.4f} %")

        # 2) XP
        print("Logando na XP…")
        login_xp(driver)
        current_tab = "DEB"
        dfs = []
        for a in ADD_ASSETS:
            if a["tab"] != current_tab:
                xp_select_tab(driver, a["tab"]); current_tab = a["tab"]
            df_tmp = xp_calculate(driver, a["code_xp"], a["rate"])
            df_tmp["Ativo"] = a["label"]
            dfs.append(df_tmp)
    finally:
        driver.quit()

    df_ex = pd.concat(dfs, ignore_index=True)

    # ---------------------------------------------------------------------
    # 5. Tratamentos (iguais ao script original)                            |
    # ---------------------------------------------------------------------
    df_ex["Valor Futuro"] = (df_ex["Valor Futuro"].str.replace(".", "", regex=False)
                                              .str.replace(",", ".", regex=False)
                                              .astype(float))

    juros  = df_ex[df_ex["Tipo"] == "Juros"]
    amort  = df_ex[df_ex["Tipo"] != "Juros"]
    df_ja  = pd.merge(
        juros[["Data","Ativo","Valor Futuro"]],
        amort[["Data","Ativo","Valor Futuro"]],
        on=["Data","Ativo"], how="outer", suffixes=("_J","_A"))
    df_ja["Valor Futuro"] = df_ja["Valor Futuro_J"].fillna(0) + df_ja["Valor Futuro_A"].fillna(0)
    df_ja["Tipo"] = "Juros|Amortização"

    df_fin = (pd.concat([df_ex, df_ja[["Data","Tipo","Valor Futuro","Ativo"]]])
                 .drop_duplicates(subset=["Data","Tipo","Ativo"]))

    meses = {"Jan":"Jan","Fev":"Feb","Mar":"Mar","Abr":"Apr","Mai":"May","Jun":"Jun",
             "Jul":"Jul","Ago":"Aug","Set":"Sep","Out":"Oct","Nov":"Nov","Dez":"Dec"}
    for pt,en in meses.items():
        df_fin["Data"] = df_fin["Data"].str.replace(pt, en)
    df_fin["Data"] = pd.to_datetime(df_fin["Data"], format="%d-%b-%Y", dayfirst=True)

    b3 = mcal.get_calendar("B3")
    hoje = datetime.now()
    df_fin["Dias Úteis"] = df_fin["Data"].apply(lambda x: len(b3.valid_days(start_date=hoje, end_date=x)) - 1)
    df_fin["Dias"] = (df_fin["Data"] - hoje).dt.days

    taxas = {a["label"]: a["rate"] / 100 for a in ADD_ASSETS}
    df_fin["Taxa"] = df_fin["Ativo"].map(taxas)
    df_fin = df_fin.query("Tipo=='Juros|Amortização' and Dias>0")
    df_fin["VP"] = df_fin["Valor Futuro"] / (1 + df_fin["Taxa"]) ** (df_fin["Dias Úteis"] / 252)

    # ------------------------------------------------------------------
    # 6. Formatações de saída exatamente como deb_table_completa2.csv    |
    # ------------------------------------------------------------------
    col_ref = pd.read_csv(CSV_FINAL, nrows=2).columns.tolist()

    saida = (df_fin.assign(**{
        "Data": lambda d: d["Data"].dt.strftime("%d/%m/%Y"),
        "Expectativa de juros (%)": "-",
        "Dias entre pagamentos": "-",
        "Juros projetados": "0",
        "Amortizações": "0",
        "Fluxo descontado (R$)": lambda d: d["VP"].map(lambda x: str(round(x, 2)).replace(".", ",")),
        "Prazos (dias úteis)": df_fin["Dias Úteis"],
        "Dados do evento": df_fin["Tipo"],
        "Data de pagamento": df_fin["Data"],
    })
    .drop(columns=["Valor Futuro","Dias","Taxa","VP","Tipo","Dias Úteis"])
    .pipe(lambda d: d.assign(**{
        "Data de pagamento": pd.to_datetime(d["Data de pagamento"], format="%d-%b-%Y", dayfirst=True)
                              .dt.strftime("%d/%m/%Y")
    }))
    )

    # garante a ordem das colunas
    saida = saida[col_ref]

    # ------------------------------------------------------------------
    # 7. APPEND no arquivo final                                         |
    # ------------------------------------------------------------------
    deb_atual = pd.read_csv(CSV_FINAL)
    deb_novo  = pd.concat([deb_atual, saida], ignore_index=True)
    # evita duplicatas exatas (todas as colunas).
    deb_novo.drop_duplicates(inplace=True)
    deb_novo.to_csv(CSV_FINAL, index=False)

    print(f"\n✓ {len(saida)} novas linhas adicionadas a '{CSV_FINAL}'.\n")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
    