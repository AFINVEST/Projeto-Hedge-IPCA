from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
from selenium.webdriver.common.action_chains import ActionChains
# ['CDES11', 'VAMO33', 'VALEC0', 'UHSM12', 'TSSG21', 'TIMS12', 'SUZBC1', 'SUZBA0', 'RISP12', 'JSMLB5', 'GASC23', 'GASC17', 'GASC25', 'RIPR22', 'RDVE11', 'RAIZ23', 'PEJA11', 'PEJA22', 'HZTC14', 'OMGE12', 'NTEN11', 'NEOE26', 'NMCH11', 'MRSAB2', 'MRSAB1', 'MRSAC1', 'CCROB6', 'MSGT33', 'LTTE15', 'LIGH2B', 'KLBNA5', 'CTEE29', 'IGSN15', 'HARG11', 'HBSA11', 'HVSP11', 'SUMI17', 'FBRI13', 'EQTR11', 'CGOS16', 'CGOS34', 'ENTV12', 'EGIEB2', 'EGIEA1', 'ENEV32', 'ENEV29', 'ENEVB0', 'CESE32', 'ENEV18', 'ENGIA5', 'ENGIA6', 'ENGIB2', 'ENGIB9', 'ESULA6', 'ENMTB3', 'ENAT14', 'ESAM14', 'ELPLA7', 'EKTRC1', 'ERDVC3', 'ERDVB4', 'ECHP11', 'DESK17', 'EEELB1', 'CPLD37', 'CPLD29', 'CBAN12', 'CBAN72', 'CBAN52', 'CBAN32', 'CART13', 'IVIAA0', 'RSAN26', 'PALFB3', 'CHSF13', 'COCE18', 'CAEC12', 'CAEC21', 'COMR14', 'BCPSA5', 'SBSPE3', 'CSMGB9', 'CEEBC3', 'EQPA18', 'ELET14', 'CMGDB0', 'CMGDB1', 'CLCD27', 'BRKP28', 'RMSA12', 'TEPA12', 'BARU11', 'BHSA11', 'PLSB1A', 'ACRC21', 'ARTR19', 'ANET11', 'ANET12', 'ALGAB1', 'RIS424', 'RIS422', 'GSTS14', 'GSTS24', 'TIET29', 'AESOA1']

ativos = ['ACRC21', 'AEAB11', 'AEGP23', 'AESL17', 'AESLA5', 'AESLA7', 'AESLB7', 'AESOA1', 'AGVF12', 'AHGD13', 'ALGA27', 'ALGAB1', 'ALGAC2', 'ALGE16', 'ALGTA4', 'ALIG12', 'ALIG13', 'ALIG15', 'ALUP18', 'ANET11', 'ANET12', 'APFD19', 'APPSA2', 'APRB18', 'ARTR19', 'ASAB11', 'ASCP13', 'ASCP23', 'ASER12', 'ASSR21', 'ATHT11', 'ATII12', 'AURE12', 'BARU11', 'BCPSA5', 'BHSA11', 'BLMN12', 'BRKP28', 'BRST11', 'CAEC12', 'CAEC21', 'CAJS11', 'CAJS12', 'CART13', 'CASN23', 'CBAN12', 'CBAN32', 'CBAN52', 'CBAN72', 'CCLS11', 'CCROA5', 'CCROB4', 'CCROB6', 'CDES11', 'CEAD11', 'CEAP12', 'CEAP14', 'CEAP17', 'CEAR26', 'CEEBA1', 'CEEBB6', 'CEEBB7', 'CEEBC3', 'CEEBC4', 'CEED12', 'CEED13', 'CEED15', 'CEED17', 'CEED21', 'CEMT19', 'CEPE19', 'CEPEB3', 'CEPEC1', 'CEPEC2', 'CESE32', 'CESPA2', 'CESPA3', 'CGASA1', 'CGASA2', 'CGASB1', 'CGMG18', 'CGOS13', 'CGOS16', 'CGOS24', 'CGOS28', 'CGOS34', 'CHSF13', 'CJEN13', 'CLAG13', 'CLCD26', 'CLCD27', 'CLNG11', 'CLTM14', 'CMGD27', 'CMGD28', 'CMGDB0', 'CMGDB1', 'CMIN11', 'CMIN12', 'CMIN21', 'CMIN22', 'CMTR29', 'CNRD11', 'COCE18', 'COMR14', 'COMR15', 'CONF11', 'CONX12', 'CPFGA2', 'CPFPA0', 'CPFPA5', 'CPFPA7', 'CPFPB7', 'CPGT15', 'CPGT26', 'CPGT27', 'CPGT28', 'CPLD26', 'CPLD29', 'CPLD37', 'CPTM15', 'CPXB22', 'CRMG15', 'CRTR12', 'CSAN33', 'CSMGA2', 'CSMGA6', 'CSMGB4', 'CSMGB8', 'CSMGB9', 'CSMGC3', 'CSNAA4', 'CSNAA5', 'CSNAA6', 'CSNAB4', 'CSNAB5', 'CSNAB6', 'CSNAC4', 'CSNP12', 'CSRN19', 'CSRN29', 'CSRNA1', 'CSRNB2', 'CSRNC0', 'CTEE17', 'CTEE18', 'CTEE1B', 'CTEE29', 'CTEE2B',  'CTGE13', 'CTGE15', 'CTNS14', 'CTRR11', 'CUTI11', 'CXER12', 'DESK17', 'EBAS13', 'EBENA8', 'ECER12', 'ECHP11', 'ECHP12', 'ECHP22', 'ECOV16', 'ECPN11', 'EDFT11', 'EDPA11', 'EDPT11', 'EDTE12', 'EDVP14', 'EDVP17', 'EEELA0', 'EEELA1', 'EEELB1', 'EGIE17', 'EGIE19', 'EGIE27', 'EGIE29', 'EGIE39', 'EGIE49', 'EGIEA0', 'EGIEA1', 'EGIEB1', 'EGIEB2', 'EGVG11', 'EGVG21', 'EKTRB3', 'EKTRC0', 'EKTRC1', 'EKTT11', 'ELEK37', 'ELET14', 'ELET16', 'ELET23', 'ELET42', 'ELPLA5', 'ELPLA7', 'ELPLB4', 'ELTN15', 'ENAT11', 'ENAT12', 'ENAT13', 'ENAT14', 'ENAT24', 'ENAT33', 'ENERA1', 'ENERB4', 'ENEV13', 'ENEV15', 'ENEV16', 'ENEV18', 'ENEV19', 'ENEV26', 'ENEV28', 'ENEV29', 'ENEV32', 'ENEV39', 'ENEVA0', 'ENEVB0', 'ENGI39', 'ENGIA1', 'ENGIA4', 'ENGIA5', 'ENGIA6', 'ENGIA9', 'ENGIB0', 'ENGIB2', 'ENGIB4', 'ENGIB6', 'ENGIB9', 'ENGIC0', 'ENJG21', 'ENMI21', 'ENMTA3', 'ENMTA4', 'ENMTA5', 'ENMTA7', 'ENMTB3', 'ENMTB5', 'ENSEA1', 'ENTV12', 'EQMAA0', 'EQMAA2', 'EQPA18', 'EQSP11', 'EQSP21', 'EQTC11', 'EQTN11', 'EQTR11', 'EQTR21', 'EQTS11', 'EQUA11', 'ERDV17',
          'ERDV38', 'ERDVA4', 'VALEB1', 'ERDVB4', 'ERDVC3', 'SUZBC1', 'ERDVC4', 'ESAM14', 'ESULA1', 'ESULA6', 'ETAP22', 'ETBA12', 'ETEN11', 'ETEN12', 'ETEN21', 'ETEN22', 'ETEN31', 'ETSP12', 'EUBE11', 'EXTZ11', 'FBRI13', 'FGEN13', 'FLCLA0', 'FRAG14', 'FURN21', 'GASC15', 'GASC16', 'GASC17', 'GASC22', 'GASC23', 'GASC25', 'GASC26', 'GASC27', 'GASP19', 'GASP29', 'GASP34', 'GBSP11', 'GEPA28', 'GRRB24', 'GSTS14', 'GSTS24', 'HARG11', 'HBSA11', 'HBSA21', 'HGLB23', 'HVSP11', 'HZTC14', 'IBPB11', 'IGSN15', 'IRJS14', 'IRJS15', 'ITPO14', 'IVIAA0', 'JALL11', 'JALL13', 'JALL14', 'JALL15', 'JALL21', 'JALL24', 'JSMLB5', 'JTEE11', 'JTEE12', 'KLBNA5', 'LCAMD1', 'LCAMD3', 'LGEN11', 'LIGH2B', 'LIGHA5', 'LORTA7', 'LTTE15', 'MEZ511', 'MGSP12', 'MNAU13', 'MOVI18', 'MOVI37', 'MRSAA1', 'MRSAA2', 'MRSAB1', 'MRSAB2', 'MRSAC1', 'MRSAC2', 'MSGT12', 'MSGT13', 'MSGT23', 'MSGT33', 'MTRJ19', 'MVLV16', 'NEOE16', 'NEOE26', 'NMCH11', 'NRTB11', 'NRTB21', 'NTEN11', 'ODTR11', 'ODYA11', 'OMGE12', 'OMGE22', 'OMGE31', 'OMGE41', 'OMNG12', 'ORIG11', 'PALF38', 'PALFA3', 'PALFB3', 'PASN12', 'PEJA11', 'PEJA22', 'PEJA23', 'PETR16', 'PETR17', 'PETR26', 'PETR27', 'PLSB1A', 'POTE11', 'POTE12', 'PPTE11', 'PRAS11', 'PRPO12', 'PRTE12', 'PTAZ11', 'QUAT13', 'RAHD11', 'RAIZ13', 'RAIZ23', 'RATL11', 'RDOE18', 'RDVE11', 'RECV11', 'RESA14', 'RESA15', 'RESA17', 'RESA27', 'RIGEA3', 'RIPR22', 'RIS412', 'RIS414', 'RIS422', 'RIS424', 'RISP12', 'RISP14', 'RISP22', 'RISP24', 'RMSA12', 'RRRP13', 'RSAN16', 'RSAN26', 'RSAN34', 'RSAN44', 'RUMOA2', 'RUMOA3', 'RUMOA4', 'RUMOA5', 'RUMOA6', 'RUMOA7', 'RUMOB1', 'RUMOB3', 'RUMOB5', 'RUMOB6', 'RUMOB7', 'SABP12', 'SAELA1', 'SAELA3', 'SAELB3', 'SAPR10', 'SAPRA2', 'SAPRA3', 'SAPRB3', 'SAVI13', 'SBSPB6', 'SBSPC4', 'SBSPC6', 'SBSPD4', 'SBSPE3', 'SBSPE9', 'SBSPF3', 'SBSPF9', 'SERI11', 'SMTO14', 'SMTO24', 'SNRA13', 'SPRZ11', 'SRTI11', 'STBP35', 'STBP45', 'STRZ11', 'SUMI17', 'SUMI18', 'SUZB19', 'SUZB29', 'SUZBA0', 'TAEB15', 'TAEE17', 'TAEE18', 'TAEE26', 'TAEEA2', 'TAEEA4', 'TAEEA7', 'TAEEB2', 'TAEEB4', 'TAEEC2', 'TAEEC4', 'TAEED2', 'TAES15', 'TBEG11', 'TBLE26', 'TCII11', 'TEPA12', 'TIET18', 'TIET29', 'TIET39', 'TIMS12', 'TNHL11', 'TOME12', 'TPEN11', 'TPNO12', 'TPNO13', 'TRCC11', 'TRGO11', 'TRPLA4', 'TRPLA7', 'TRPLB4', 'TRPLB7', 'TSSG21', 'TVVH11', 'UHSM12', 'UNEG11', 'UNTE11', 'USAS11', 'UTPS11', 'UTPS12', 'UTPS21', 'UTPS22', 'VALE38', 'VALE48', 'VALEA0', 'VALEB0', 'VALEC0', 'VAMO33', 'VAMO34', 'VBRR11', 'VDBF12', 'VDEN12', 'VERO12', 'VERO13', 'VERO24', 'VERO44', 'VLIM13', 'VLIM14', 'VLIM15', 'VLIM16', 'VPLT12', 'VRDN12', 'WDPR11', 'XNGU17']

Cra = ["CRA Ferroeste 2ª Série", "CRI Bem Brasil"]

df_posicao = pd.read_excel(
    'Dados/Relatório de Posição 2025-08-08.xlsx', sheet_name='Worksheet')

df_posicao = df_posicao[df_posicao['Ativo'].isin(ativos)]
ativos = df_posicao['Ativo'].unique()
ativos = ativos.tolist()
print(ativos)

# ['CDES11', 'VAMO33', 'VALEC0', 'UHSM12', 'TSSG21', 'TIMS12', 'SUZBC1', 'SUZBA0', 'RISP12', 'JSMLB5', 'GASC23', 'GASC17', 'GASC25', 'RIPR22', 'RDVE11', 'RAIZ23', 'PEJA11', 'PEJA22', 'HZTC14', 'OMGE12', 'NTEN11', 'NEOE26', 'NMCH11', 'MRSAB2', 'MRSAB1', 'MRSAC1', 'CCROB6', 'MSGT33', 'LTTE15', 'LIGH2B', 'KLBNA5', 'CTEE29', 'IGSN15', 'HARG11', 'HBSA11', 'HVSP11', 'SUMI17', 'FBRI13', 'EQTR11', 'CGOS16', 'CGOS34', 'ENTV12', 'EGIEB2', 'EGIEA1', 'ENEV32', 'ENEV29', 'ENEVB0', 'CESE32', 'ENEV18', 'ENGIA5', 'ENGIA6', 'ENGIB2', 'ENGIB9', 'ESULA6', 'ENMTB3', 'ENAT14', 'ESAM14', 'ELPLA7', 'EKTRC1', 'ERDVC3', 'ERDVB4', 'ECHP11', 'DESK17', 'EEELB1', 'CPLD37', 'CPLD29', 'CBAN12', 'CBAN72', 'CBAN52', 'CBAN32', 'CART13', 'IVIAA0', 'RSAN26', 'PALFB3', 'CHSF13', 'COCE18', 'CAEC12', 'CAEC21', 'COMR14', 'BCPSA5', 'SBSPE3', 'CSMGB9', 'CEEBC3', 'EQPA18', 'ELET14', 'CMGDB1', 'CMGDB0', 'CLCD27', 'BRKP28', 'RMSA12', 'TEPA12', 'BARU11', 'BHSA11', 'PLSB1A', 'ACRC21', 'ARTR19', 'ANET11', 'ANET12', 'ALGAB1', 'RIS424', 'RIS422', 'GSTS14', 'GSTS24', 'TIET29', 'AESOA1']

# ANALISAR SE O AESO1 ESTÁ INDO


Nao_achei = ["LFSN210099R"]

deu_ruim = ["BRFS31", 'NTN-B50', 'CRTA12', 'CPLD15',
            'CTGE11', 'LSVE39', 'SUMI37', 'SUZBC1']

# Configurar o serviço do ChromeDriver
# ativos =['PEJA11', 'ASAB11']

# options.add_argument("--headless")  # Opcional: Executar sem abrir a janela
service = Service()
driver = webdriver.Chrome(service=service)
driver.maximize_window()
# Diminuir o zoom
driver.execute_script("document.body.style.zoom='60%'")
# driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

# actions = ActionChains(driver)
# actions.move_by_offset(10, 10).perform()


df = pd.DataFrame()

try:
    # Acessar o site de login mudando o ativo a cada iteração
    for i in range(len(ativos)):
        # Acessar o site de login com cada ativo da lista
        driver.get(
            f"https://data.anbima.com.br/ferramentas/calculadora/debentures/{ativos[i]}?ativo=debentures")

        # Configura um tempo de espera máximo de 20 segundos
        wait = WebDriverWait(driver, 20)
        driver.execute_script("document.body.style.zoom='60%'")

        try:
            # Verifica se o elemento está presente
            time.sleep(2)
            elemento = driver.find_element(
                By.XPATH, "//p[contains(text(), 'Taxa ANBIMA do ativo')]")

            taxa_anbima_encontrada = True
        except:
            taxa_anbima_encontrada = False

        if taxa_anbima_encontrada:
            # Aguardar até que o botão esteja clicável
            button = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "#card-calcular-precificacao > article > article > section > div > form > div.col-xs-12.precificacao-content__calculate-button.col-no-padding > button")))
            # Clicar no botão
            button.click()
            # Aguarde para garantir que a tabela carregue após o clique
            time.sleep(4)
            # Aguardar a tabela carregar
            table_element = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#card-fluxo-pagamento > article > article > section > div > div > table")))
            print("Tabela carregada com sucesso!")
            # Capturar o conteúdo da tabela com BeautifulSoup
            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.select_one(
                "#card-fluxo-pagamento > article > article > section > div > div > table")
            rows = table.find_all("tr")
            # Make a dataframe WITH THE NAME OF THE ATIVO
            data_list = []
            for row in rows:
                columns = row.find_all("td")
                data = [col.text.strip() for col in columns]
                if data:
                    print(data)
                    data_list.append(data)
                print(ativos[i])

            df_append = pd.DataFrame(data_list)

            # Adiciona o nome do ativo em uma coluna no DataFrame
            df_append["Ativo"] = ativos[i]
            # Concat
            df = pd.concat([df, df_append])

        else:
            driver.get(
                f"https://data.anbima.com.br/debentures/{ativos[i]}/caracteristicas")
            driver.execute_script("document.body.style.zoom='60%'")

            # Configura um tempo de espera máximo de 20 segundos
            wait = WebDriverWait(driver, 20)
            try:
                driver.maximize_window()
                # Localiza o elemento com a classe 'lower-card-item-value'
                taxa_elemento = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "p.lower-card-item-value")))

                # Extrai o texto do elemento
                taxa_texto = taxa_elemento.text

                # Remove o símbolo de porcentagem e converte para número
                taxa_valor = float(taxa_texto.replace(
                    " %", "").replace(",", "."))

                # Armazena o valor da taxa
                print(f"Taxa ANBIMA encontrada: {taxa_valor}")
                driver.get(
                    f"https://data.anbima.com.br/ferramentas/calculadora/debentures/{ativos[i]}?ativo=debentures")

                # Configura um tempo de espera máximo de 20 segundos
                wait = WebDriverWait(driver, 20)
                driver.execute_script("document.body.style.zoom='60%'")

                # Aguardar até que o botão esteja clicável
                # Formata a taxa para o formato com vírgula (como no exemplo do campo)
                taxa_formatada = str(f"{taxa_valor:.6f}").replace(".", ",")

                # Localiza o campo de entrada pelo seletor CSS da classe 'anbima-ui-input__input'
                # input_elemento = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.anbima-ui-input__input")))
                input_elemento = wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//div[@id='precificacao-input-taxa']//input")
                ))

                driver.execute_script("""
                    const input = arguments[0];
                    const valor = arguments[1];
                    input.value = valor;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                """, input_elemento, taxa_formatada)
                input_elemento.send_keys(Keys.ENTER)
                button = wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "#card-calcular-precificacao > article > article > section > div > form > div.col-xs-12.precificacao-content__calculate-button.col-no-padding > button")))
                # Clicar no botão
                button.click()
                # Aguarde para garantir que a tabela carregue após o clique
                time.sleep(4)

                # Aguardar a tabela carregar
                table_element = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#card-fluxo-pagamento > article > article > section > div > div > table")))
                print("Tabela carregada com sucesso!")

                # Capturar o conteúdo da tabela com BeautifulSoup
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.select_one(
                    "#card-fluxo-pagamento > article > article > section > div > div > table")

                rows = table.find_all("tr")
                # Make a dataframe WITH THE NAME OF THE ATIVO
                data_list = []
                for row in rows:
                    columns = row.find_all("td")
                    data = [col.text.strip() for col in columns]
                    if data:
                        print(data)
                        data_list.append(data)
                    print(ativos[i])

                df_append = pd.DataFrame(data_list)

                # Adiciona o nome do ativo em uma coluna no DataFrame
                df_append["Ativo"] = ativos[i]
                # Concat
                df = pd.concat([df, df_append])
            except Exception as e:
                print(f"Erro ao extrair taxa ANBIMA: {e}")

    # Definir os nomes das colunas
    columns = ["Dados do evento", "Data de pagamento",
               "Prazos (dias úteis)", "Dias entre pagamentos", "Expectativa de juros (%)", "Juros projetados", "Amortizações", "Fluxo descontado (R$)", "Ativo"]
    df.columns = columns

    # Salvar o DataFrame em um arquivo CSV
    df.to_csv("Dados/tabela_debentures222.csv", index=False)
    print("Tabela salva com sucesso!")


except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    # Fechar o navegador
    driver.quit()
