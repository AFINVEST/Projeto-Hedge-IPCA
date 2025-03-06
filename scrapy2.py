from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import pandas as pd
import time

ativos = ['MSGT13', 'PEJA11', 'CAEC12', 'VAMO33', 'NEOE26', 'PEJA21', 'GSTS14', 'VERO13', 'ENEV32', 'GASC23', 'CPLD37', 'RMSA12', 'TIET29', 'CMDT33',
          'ENAT14', 'BHSA11', 'EQTR11', 'TBCR18', 'ARTR19', 'ERDVC3', 'ELET14', 'IGSN15', 'MOVI37', 'JSMLB5', 'CEEBC3', 'ALGAB1', 'COCE18', 'ECHP11']

ativos = ['VERO13', 'VAMO33', 'VAMO23',
       'VALEC0', 'TBCR18', 'RISP12', 'JSMLB5', 'GASC23', 'PEJA11',
       'PEJA21', 'NEOE26', 'MOVI37', 'MSGT33', 'MSGT13', 'IGSN15',
       'SUMI17', 'EQTR11', 'ENEV32', 'ENGIA5', 'ESULA6', 'ENAT14',
       'ERDVC3', 'ECHP11', 'CPLD37', 'CHSF13', 'COCE18',
       'CAEC12', 'CAEC21', 'CEEBC3', 'EQPA18', 'ELET14', 'BRKP28',
       'RMSA12', 'BHSA11', 'PLSB1A', 'ARTR19', 'ALGAB1',
       'GSTS14', 'TIET29', 'AESOA1']

Cra = ["CRA Ferroeste 2ª Série","CRI Bem Brasil"]

Nao_achei = ["LFSN210099R"]

deu_ruim = ["BRFS31", 'NTN-B50', 'AFHI11','CRTA12']
# Configurar o serviço do ChromeDriver
service = Service()

# Inicializar o navegador
driver = webdriver.Chrome()

df = pd.DataFrame()


try:
    # Acessar o site de login com cada ativo da lista
    driver.get(f"https://www.anbima.com.br/pt_br/informar/taxas-de-debentures.htm")

    # Configura um tempo de espera máximo de 20 segundos
    wait = WebDriverWait(driver, 20)

    # Aguardar até que o iframe esteja presente e mudar o foco para ele
    iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
    driver.switch_to.frame(iframe)

    # Aguardar até que a imagem esteja clicável usando o seletor CSS fornecido
    image = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "#cinza50 > form > div > table > tbody > tr > td > img")))

    # Clicar na imagem
    image.click()

    # Aguarde para garantir que a tabela carregue após o clique
    time.sleep(3)

    # Esperar a tabela carregar
    table_element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#cinza50 > center > table:nth-child(3)"))
    )
    print("Tabela carregada com sucesso!")

    # Capturar o conteúdo da página com BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.select_one("#cinza50 > center > table:nth-child(3)")

    # Extrair as linhas da tabela
    rows = table.find_all("tr")
    data_list = []

    for row in rows:
        columns = row.find_all("td")
        data = [col.text.strip() for col in columns]
        if data:
            data_list.append(data)

    # Criar o DataFrame
    df = pd.DataFrame(data_list)

    # Definir os nomes das colunas
    df.columns = [
        "Código", "Nome", "Repac./Venc.", "Índice/Correção", "Taxa de Compra", "Taxa de Venda", 
        "Taxa Indicativa", "Desvio Padrão", "Min.", "Máx.", "PU", "% PU Par", "Duration", "% Reune", 
        "Referência NTN-B"
    ]

    # Salvar o DataFrame em um arquivo CSV
    df.to_csv("lista_deb.csv", index=False, encoding="utf-8-sig")
    print("Arquivo CSV salvo com sucesso!")
    print("Lista salva com sucesso!")


except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    # Fechar o navegador
    driver.quit()
