import platform
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Optional
import pandas as pd
import requests
from bs4 import BeautifulSoup
import selenium.webdriver as webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import undetected_chromedriver as uc
from selenium.common.exceptions     import StaleElementReferenceException, TimeoutException


# ---------------------------------------------------------------------------
# 1. CONFIGURAÇÕES BÁSICAS
# ---------------------------------------------------------------------------

LOGIN_AFINVEST: Tuple[str, str] = ("emanuel.cabral@afinvest.com.br", "Afs@2024")     # <- preencha aqui
CSV_SAIDA                = Path("spreads_afinvest.csv")
ERROS_CAPTURA: List[str] = []                            # ativos sem dados

# ---------------------------------------------------------------------------
# 2. LISTA DE ATIVOS
# ---------------------------------------------------------------------------

ATIVOS_SEM_DADOS_EXTRA   = {
    'RALM21','AEAB11','AESL17','ALGA27','APPSA2','BHSA11','BLMN12','BRFS31',
    'CBAN32','CCROB4','CEAR26','CEEBA1','CEEBB6','CEPE19','CESE32','CESPA2',
    'CGASB1','CLCD27','CLNG11','CONF11','CONX12','CPGT26','CPGT27','CSMGC3',
    'CSNAA4','CSNAA5','CSRNC0','CTEE17','CTEE29','CTEE2B','CTNS14','CTRR11',
    'CUTI11','ECPN11','EDVP14','EDVP17','EGIE17','EKTRC0','EKTRC1','ENAT24',
    'ENAT33','ENEV39','ENEVA0','ENGIA6','ENGIA9','ENMTB3','ENMTB5','ERDV17',
    'ERDVA4','ERDVB4','ERDVC3','ERDVC4','ESULA1','ETAP22','ETBA12','ETSP12',
    'EUBE11','FGEN13','FLCLA0','GEPA28','GRRB24','IRJS14','IRJS15','JSMLB5',
    'JTEE11','JTEE12','KLBNA5','LSVE39','LTTE15','MEZ511','MOVI37','MSGT23',
    'MSGT33','NEOE26','NMCH11','ODTR11','OMNG12','PALFA3','PALFB3','PEJA22',
    'PEJA23','QUAT13','RAHD11','RESA17','RESA27','RISP22','RISP24','RUMOA5',
    'RUMOA6','RUMOB1','SBSPD4','SBSPE3','SRTI11','STBP35','TAEEA4','TAEEA7',
    'TAEEC2','TAEEC4','TBEG11','TIET39','TIMS12','TNHL11','TOME12','UHSM12',
    'UNEG11','VAMO33','VDBF12','VDEN12'
}

def obter_lista_ativos() -> List[str]:
    """
    Preencha e devolva os tickers desejados, por exemplo:
        return ["AHGD13", "ALUP18", ...]
    """
    #return ['RALM21','ACRC21', 'AEAB11', 'AEGP23', 'AESL17', 'AESLA5', 'AESLA7', 'AESLB7', 'AESOA1', 'AGVF12', 'AHGD13', 'ALGA27', 'ALGAB1', 'ALGAC2', 'ALGE16', 'ALGTA4', 'ALIG12', 'ALIG13', 'ALIG15', 'ALUP18', 'ANET11', 'ANET12', 'APFD19', 'APPSA2', 'APRB18', 'ARTR19', 'ASAB11', 'ASCP13', 'ASCP23', 'ASER12', 'ASSR21', 'ATHT11', 'ATII12', 'AURE12', 'BARU11', 'BCPSA5', 'BHSA11', 'BLMN12', 'BRFS31', 'BRKP28', 'BRST11', 'CAEC12', 'CAEC21', 'CAJS11', 'CAJS12', 'CART13', 'CASN23', 'CBAN12', 'CBAN32', 'CBAN52', 'CBAN72', 'CCLS11', 'CCROA5', 'CCROB4', 'CCROB6', 'CDES11', 'CEAD11', 'CEAP12', 'CEAP14', 'CEAP17', 'CEAR26', 'CEEBA1', 'CEEBB6', 'CEEBB7', 'CEEBC3', 'CEEBC4', 'CEED12', 'CEED13', 'CEED15', 'CEED17', 'CEED21', 'CEMT19', 'CEPE19', 'CEPEB3', 'CEPEC1', 'CEPEC2', 'CESE32', 'CESPA2', 'CESPA3', 'CGASA1', 'CGASA2', 'CGASB1', 'CGMG18', 'CGOS13', 'CGOS16', 'CGOS24', 'CGOS28', 'CGOS34', 'CHSF13', 'CJEN13', 'CLAG13', 'CLCD26', 'CLCD27', 'CLNG11', 'CLTM14', 'CMGD27', 'CMGD28', 'CMGDB0', 'CMGDB1', 'CMIN11', 'CMIN12', 'CMIN21', 'CMIN22', 'CMTR29', 'CNRD11', 'COCE18', 'COMR14', 'COMR15', 'CONF11', 'CONX12', 'CPFGA2', 'CPFPA0', 'CPFPA5', 'CPFPA7', 'CPFPB7', 'CPGT15', 'CPGT26', 'CPGT27', 'CPGT28', 'CPLD15', 'CPLD26', 'CPLD29', 'CPLD37', 'CPTM15', 'CPXB22', 'CRMG15', 'CRTR12', 'CSAN33', 'CSMGA2', 'CSMGA6', 'CSMGB4', 'CSMGB8', 'CSMGB9', 'CSMGC3', 'CSNAA4', 'CSNAA5', 'CSNAA6', 'CSNAB4', 'CSNAB5', 'CSNAB6', 'CSNAC4', 'CSNP12', 'CSRN19', 'CSRN29', 'CSRNA1', 'CSRNB2', 'CSRNC0', 'CTEE17', 'CTEE18', 'CTEE1B', 'CTEE29', 'CTEE2B', 'CTGE11', 'CTGE13', 'CTGE15', 'CTNS14', 'CTRR11', 'CUTI11', 'CXER12', 'DESK17', 'EBAS13', 'EBENA8', 'ECER12', 'ECHP11', 'ECHP12', 'ECHP22', 'ECOV16', 'ECPN11', 'EDFT11', 'EDPA11', 'EDPT11', 'EDTE12', 'EDVP14', 'EDVP17', 'EEELA0', 'EEELA1', 'EEELB1', 'EGIE17', 'EGIE19', 'EGIE27', 'EGIE29', 'EGIE39', 'EGIE49', 'EGIEA0', 'EGIEA1', 'EGIEB1', 'EGIEB2', 'EGVG11', 'EGVG21', 'EKTRB3', 'EKTRC0', 'EKTRC1', 'EKTT11', 'ELEK37', 'ELET14', 'ELET16', 'ELET23', 'ELET42', 'ELPLA5', 'ELPLA7', 'ELPLB4', 'ELTN15', 'ENAT11', 'ENAT12', 'ENAT13', 'ENAT14', 'ENAT24', 'ENAT33', 'ENERA1', 'ENERB4', 'ENEV13', 'ENEV15', 'ENEV16', 'ENEV18', 'ENEV19', 'ENEV26', 'ENEV28', 'ENEV29', 'ENEV32', 'ENEV39', 'ENEVA0', 'ENEVB0', 'ENGI39', 'ENGIA1', 'ENGIA4', 'ENGIA5', 'ENGIA6', 'ENGIA9', 'ENGIB0', 'ENGIB2', 'ENGIB4', 'ENGIB6', 'ENGIB9', 'ENGIC0', 'ENJG21', 'ENMI21', 'ENMTA3', 'ENMTA4', 'ENMTA5', 'ENMTA7', 'ENMTB3', 'ENMTB5', 'ENSEA1', 'ENTV12', 'EQMAA0', 'EQMAA2', 'EQPA18', 'EQSP11', 'EQSP21', 'EQTC11', 'EQTN11', 'EQTR11', 'EQTR21', 'EQTS11', 'EQUA11', 'ERDV17', 'ERDV38', 'ERDVA4', 'ERDVB4', 'ERDVC3', 'ERDVC4', 'ESAM14', 'ESULA1', 'ESULA6', 'ETAP22', 'ETBA12', 'ETEN11', 'ETEN12', 'ETEN21', 'ETEN22', 'ETEN31', 'ETSP12', 'EUBE11', 'EXTZ11', 'FBRI13', 'FGEN13', 'FLCLA0', 'FRAG14', 'FURN21', 'GASC15', 'GASC16', 'GASC17', 'GASC22', 'GASC23', 'GASC25', 'GASC26', 'GASC27', 'GASP19', 'GASP29', 'GASP34', 'GBSP11', 'GEPA28', 'GRRB24', 'GSTS14', 'GSTS24', 'HARG11', 'HBSA11', 'HBSA21', 'HGLB23', 'HVSP11', 'HZTC14', 'IBPB11', 'IGSN15', 'IRJS14', 'IRJS15', 'ITPO14', 'IVIAA0', 'JALL11', 'JALL13', 'JALL14', 'JALL15', 'JALL21', 'JALL24', 'JSMLB5', 'JTEE11', 'JTEE12', 'KLBNA5', 'LCAMD1', 'LCAMD3', 'LGEN11', 'LORTA7', 'LSVE39', 'LTTE15', 'MEZ511', 'MGSP12', 'MNAU13', 'MOVI18', 'MOVI37', 'MRSAA1', 'MRSAA2', 'MRSAB1', 'MRSAB2', 'MRSAC1', 'MRSAC2', 'MSGT12', 'MSGT13', 'MSGT23', 'MSGT33', 'MTRJ19', 'MVLV16', 'NEOE16', 'NEOE26', 'NMCH11', 'NRTB11', 'NRTB21', 'NTEN11', 'ODTR11', 'ODYA11', 'OMGE12', 'OMGE22', 'OMGE31', 'OMGE41', 'OMNG12', 'ORIG11', 'PALF38', 'PALFA3', 'PALFB3', 'PASN12', 'PEJA11', 'PEJA22', 'PEJA23', 'PETR16', 'PETR17', 'PETR26', 'PETR27', 'PLSB1A', 'POTE11', 'POTE12', 'PPTE11', 'PRAS11', 'PRPO12', 'PRTE12', 'PTAZ11', 'QUAT13', 'RAHD11', 'RAIZ13', 'RAIZ23', 'RATL11', 'RDOE18', 'RDVE11', 'RECV11', 'RESA14', 'RESA15', 'RESA17', 'RESA27', 'RIGEA3', 'RIPR22', 'RIS412', 'RIS414', 'RIS422', 'RIS424', 'RISP12', 'RISP14', 'RISP22', 'RISP24', 'RMSA12', 'RRRP13', 'RSAN16', 'RSAN26', 'RSAN34', 'RSAN44', 'RUMOA2', 'RUMOA3', 'RUMOA4', 'RUMOA5', 'RUMOA6', 'RUMOA7', 'RUMOB1', 'RUMOB3', 'RUMOB5', 'RUMOB6', 'RUMOB7', 'SABP12', 'SAELA1', 'SAELA3', 'SAELB3', 'SAPR10', 'SAPRA2', 'SAPRA3', 'SAPRB3', 'SAVI13', 'SBSPB6', 'SBSPC4', 'SBSPC6', 'SBSPD4', 'SBSPE3', 'SBSPE9', 'SBSPF3', 'SBSPF9', 'SERI11', 'SMTO14', 'SMTO24', 'SNRA13', 'SPRZ11', 'SRTI11', 'STBP35', 'STBP45', 'STRZ11', 'SUMI17', 'SUMI18', 'SUMI37', 'SUZB19', 'SUZB29', 'SUZBA0', 'SUZBC1', 'TAEB15', 'TAEE17', 'TAEE18', 'TAEE26', 'TAEEA2', 'TAEEA4', 'TAEEA7', 'TAEEB2', 'TAEEB4', 'TAEEC2', 'TAEEC4', 'TAEED2', 'TAES15', 'TBEG11', 'TBLE26', 'TCII11', 'TEPA12', 'TIET18', 'TIET29', 'TIET39', 'TIMS12', 'TNHL11', 'TOME12', 'TPEN11', 'TPNO12', 'TPNO13', 'TRCC11', 'TRGO11', 'TRPLA4', 'TRPLA7', 'TRPLB4', 'TRPLB7', 'TSSG21', 'TVVH11', 'UHSM12', 'UNEG11', 'UNTE11', 'USAS11', 'UTPS11', 'UTPS12', 'UTPS21', 'UTPS22', 'VALE38', 'VALE48', 'VALEA0', 'VALEB0', 'VALEC0', 'VAMO33', 'VAMO34', 'VBRR11', 'VDBF12', 'VDEN12', 'VERO12', 'VERO13', 'VERO24', 'VERO44', 'VLIM13', 'VLIM14', 'VLIM15', 'VLIM16', 'VPLT12', 'VRDN12', 'WDPR11', 'XNGU17']
    return ['RALM21']

# ---------------------------------------------------------------------------
# 3. FUNÇÕES UTILITÁRIAS
# ---------------------------------------------------------------------------

def start_driver() -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1600,1000")
    return uc.Chrome(options=opts)


def wait_click(wd, locator, t: int = 12) -> None:
    """
    Aguarda até que o elemento esteja clicável e então executa o click.
    `locator` deve ser uma tupla, por exemplo:
        (By.ID, "meu-botao")
    """
    WebDriverWait(wd, t).until(EC.element_to_be_clickable(locator)).click()

def _ticker_selecionado(driver, ticker) -> bool:
    sel = driver.find_element(By.ID,
          "select2-select_asset_details-container").text.strip()
    return sel.upper().startswith(ticker.upper())

def trocar_ativo(driver: uc.Chrome, ticker: str, t: int = 12) -> None:
    """Troca o ativo no Select2 tentando 3 métodos em sequência."""

    # abre o dropdown atual
    wait_click(driver, (By.ID, "select2-select_asset_details-container"), t)

    # localiza o campo de busca
    campo = WebDriverWait(driver, t).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input.select2-search__field"))
    )
    campo.clear()

    # ---------------------------------------------------------------
    # MÉTODO 1 – digita, seta para baixo, Enter
    # ---------------------------------------------------------------
    campo.send_keys(ticker.upper())
    ActionChains(driver).pause(0.3) \
                        .send_keys(Keys.ARROW_DOWN) \
                        .send_keys(Keys.ENTER) \
                        .perform()
    time.sleep(0.3)
    if _ticker_selecionado(driver, ticker):
        wait_click(driver, (By.ID, "btn-reload-asset-details"), t)
        return

    # ---------------------------------------------------------------
    # MÉTODO 2 – encontra o <li> da lista e clica nele
    # ---------------------------------------------------------------
    try:
        opcao = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 "//li[contains(@class,'select2-results__option') and "
                 "starts-with(normalize-space(.), '{}')]".format(ticker.upper()))
        ))
        driver.execute_script("arguments[0].click();", opcao)
        time.sleep(0.2)
        if _ticker_selecionado(driver, ticker):
            wait_click(driver, (By.ID, "btn-reload-asset-details"), t)
            return
    except TimeoutException:
        pass   # pula para o método 3

    # ---------------------------------------------------------------
    # MÉTODO 3 – força via JavaScript no <select> original
    # ---------------------------------------------------------------
    try:
        driver.execute_script("""
            var sel = document.getElementById('select_asset_details');
            if (sel){ sel.value = arguments[0];
                      sel.dispatchEvent(new Event('change',{bubbles:true})); }
        """, ticker.upper())
        time.sleep(0.3)
        if _ticker_selecionado(driver, ticker):
            wait_click(driver, (By.ID, "btn-reload-asset-details"), t)
            return
    except Exception:
        pass

    # se chegou aqui, nenhum método funcionou
    raise RuntimeError(f"Não consegui selecionar o ativo {ticker}")

def scrape_spreads(driver: uc.Chrome, max_try: int = 4) -> Tuple[str, float, float]:
    """
    Retorna (B_referencia, Spread_pct, Spread_bps).
    Lança ValueError se não encontrar dados válidos.
    """
    for attempt in range(1, max_try + 1):
        try:
            par = WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "p.spread_reference_ntnb"))
            )

            # B de referência
            b_ref = par.find_element(By.CSS_SELECTOR, "#spread_reference_ntnb a span").text.strip()

            # Spread Over (%)
            pct_txt = par.find_element(By.CSS_SELECTOR, "#spread_reference_ntnb").text
            pct_val = pct_txt.split(":")[-1].strip().replace("%", "").replace(",", ".")
            spread_pct = float(pct_val)

            # Spread (bps)
            bps_txt = par.find_element(By.ID, "spread_variation_daily").text
            bps_val = bps_txt.strip("()").replace("bps", "").replace(",", ".")
            spread_bps = float(bps_val)

            if b_ref and pct_val not in ("--", ""):
                return b_ref, spread_pct, spread_bps

        except Exception:
            pass

    raise ValueError("Spreads não encontrados ou layout alterado.")

def login_afinvest(driver: uc.Chrome) -> None:
    user, pwd = LOGIN_AFINVEST
    driver.get("https://afinvest.com.br/login/interno")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "atributo"))
    ).send_keys(user)
    driver.find_element(By.ID, "passwordLogin").send_keys(pwd)
    driver.find_element(By.ID, "loginInterno").click()


from typing import Optional

import undetected_chromedriver as uc            # ou selenium.webdriver.chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui  import WebDriverWait
from selenium.webdriver.support     import expected_conditions as EC
from selenium.common.exceptions     import TimeoutException

def obter_b_ref_anbima(ticker: str,
                       driver: Optional[uc.Chrome] = None,
                       t_out: int = 10) -> Optional[str]:
    """
    Acessa https://data.anbima.com.br/debentures/{ticker}/precos
    e devolve o texto do <span id="referenciaNTNB"> … </span>.
    Retorna None se não encontrar no tempo limite.
    """
    url = f"https://data.anbima.com.br/debentures/{ticker}/precos"
    created_here = False
    if driver is None:
        created_here = True
        opts = uc.ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1200,800")
        driver = uc.Chrome(options=opts)

    try:
        driver.get(url)

        def _texto_span(driver):
            try:
                span = driver.find_element(By.ID, "referenciaNTNB")
                txt  = span.text.strip()
                return txt if txt else False   # False → continua esperando
            except StaleElementReferenceException:
                return False                    # DOM reciclou; tenta de novo

        txt = WebDriverWait(driver, t_out).until(_texto_span)
        return txt

    except TimeoutException:
        return None

    finally:
        if created_here:
            try:
                driver.quit()
            except Exception:
                pass
# ---------------------------------------------------------------------------
# 4. EXECUÇÃO
# ---------------------------------------------------------------------------

def main() -> None:
    ativos = obter_lista_ativos()
    if not ativos:
        print("⚠️  Nenhum ativo fornecido em `obter_lista_ativos()`.")
        return

    dados: List[Dict] = []

    driver = webdriver.Chrome()
    #Colocar em tela cheia
    driver.maximize_window()
    try:
        print("Logando na AFinvest…")
        login_afinvest(driver)

        # abre a página de detalhes apenas uma vez
        driver.get("https://afinvest.com.br/interno/relatorios/detalhes-de-ativos?id=9850&codativo=RALM21")
        trocar_ativo(driver, ativos[0])

        for tk in ativos:
            try:
                if tk != ativos[0]:
                    trocar_ativo(driver, tk)

                b_ref, pct, bps = scrape_spreads(driver)
                dados.append(
                    {"Ativo": tk,
                     "B referência": b_ref,
                     "Spread Over (%)": pct,
                     "Spread (bps)": bps}
                )
                print(f"✓ {tk:<8}  {b_ref}  {pct:>6.2f}%  {bps:>7.2f} bps")

            except Exception as e:
                ERROS_CAPTURA.append(tk)
                print(f"✗ {tk} – erro: {e}")
        a_anbima = sorted(set(ERROS_CAPTURA).union(ATIVOS_SEM_DADOS_EXTRA))
        if a_anbima:
            print("\nConsultando B-referência no Data Anbima…")
        for tk in a_anbima:
            b_ref = obter_b_ref_anbima(tk,driver)
            if b_ref:
                dados.append({"Ativo": tk,
                            "B referência": b_ref,
                            "Spread Over (%)": None,
                            "Spread (bps)": None})
                print(f"• {tk:<8}  B_ref={b_ref}")
            else:
                print(f"• {tk:<8}  NÃO encontrado na Anbima")
    finally:
        driver.quit()

    if dados:
        df = pd.DataFrame(dados)
        df.sort_values("Ativo", inplace=True)
        df.to_csv(CSV_SAIDA, index=False, decimal=",", sep=";")
        print(f"\nArquivo salvo em: {CSV_SAIDA.resolve()}")
    else:
        print("Nenhum dado coletado.")



# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()




'''
ATIVOS SEM DADOS
• ALGA2 NÃO encontrado na Anbima
• CTEE1 NÃO encontrado na Anbima
• GEPA2 NÃO encontrado na Anbima
• LSVE3 NÃO encontrado na Anbima
• MEZ51 B_ref=-
• RALM2 NÃO encontrado na Anbima
• RUMOB B_ref=-
'''