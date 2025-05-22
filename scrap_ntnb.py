from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
from selenium.webdriver.common.keys import Keys
from datetime import datetime
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
import time
import random
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

service = Service()
# Inicia o Chrome
driver = webdriver.Chrome()
# Remove a flag que indica que é um WebDriver
driver.execute_script(
    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

# URL da b3
url = 'https://calculadorarendafixa.com.br/#/navbar/calculadora'
driver.execute_script("document.body.style.zoom='60%'")

# Lista de ativos (sem o principal)
ativos = ['NTN-B26', 'NTN-B28', 'NTN-B30', 'NTN-B32', 'NTN-B50']

# Mapeamento de ativos para seus valores correspondentes (pode precisar ajustar de acordo com os valores reais)

ativos_map = {
    'NTN-B26': '76019920260815',  # NTN-B 08/2026
    'NTN-B28': '76019920280815',  # NTN-B 08/2028
    'NTN-B30': '76019920300815',  # NTN-B 08/2030
    'NTN-B32': '76019920320815',  # NTN-B 08/2032
    'NTN-B50': '76019920500815'   # NTN-B 08/2050
}
taxas_map = {
    'NTN-B26': '9.2874',
    'NTN-B28': '7,6290',
    'NTN-B30': '7,3796',
    'NTN-B32': '7,3702',
    'NTN-B50': '7,0847'
}

try:
    driver.get(url)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "tabTitulosPublicos"))
    )
    print("Página carregada com sucesso!")
    time.sleep(5)
    # Rolagem da tela para baixo
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # Clicar no botão 'TÍTULOS PÚBLICOS'
    titulos_publicos_button = driver.find_element(By.ID, "tabTitulosPublicos")
    titulos_publicos_button.click()
    time.sleep(5)
    print("Botão 'TÍTULOS PÚBLICOS' clicado!")

    # Esperar o carregamento do <select> para os títulos
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "codigoTitulo"))
    )

    # Localizar o elemento <select>
    select_element = Select(driver.find_element(By.ID, "codigoTitulo"))
    df = pd.DataFrame()
    # Loop para selecionar os ativos da lista
    for ativo in ativos:
        # Obter o valor do ativo a partir do mapeamento
        ativo_value = ativos_map.get(ativo)
        if ativo_value:
            # Selecionar o ativo correspondente
            select_element.select_by_value(ativo_value)
            time.sleep(5)

            taxa_value = taxas_map.get(ativo)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "taxa"))
            )
            # Localizar o input de taxa e preencher com o valor
            taxa_input = driver.find_element(By.ID, "taxa")
            taxa_input.clear()  # Limpar o campo antes de preencher
            # Preencher com o valor da variável
            taxa_input.send_keys(str(taxa_value))
            time.sleep(5)

            # Esperar o botão "Calcular" aparecer
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "popupCalcular"))
            )

            # Clicar no botão "Calcular"
            calcular_button = driver.find_element(By.NAME, "popupCalcular")
            calcular_button.click()
            print("Botão 'Calcular' clicado!")
            # Maximizar a tela
            driver.maximize_window()

            # Aguardar um pouco para ver os resultados
            time.sleep(10)
            # Aguardar o botão de confirmar aparecer
            xpath_confirmar = "//div[@id='limiteAtingidoModal']//button[@id='confirmar']"
            botao_confirmar = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, xpath_confirmar))
            )
            botao_confirmar.click()

            # Aguardar o carregamento da tabela
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "cash-flow"))
            )
            time.sleep(3)
            # Coletar as linhas da tabela
            table = driver.find_element(By.CLASS_NAME, "cash-flow")
            rows = table.find_elements(By.TAG_NAME, "tr")

            # Criar listas para armazenar os dados da tabela
            data = []
            tipos = []
            dias_uteis = []
            taxas = []
            valores_futuros = []
            valores_presentes = []

            # Iterar pelas linhas e armazenar os dados
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                col_data = [col.text for col in cols]
                if col_data:  # Verificar se a linha não está vazia
                    data.append(col_data[0])
                    tipos.append(col_data[1])
                    dias_uteis.append(col_data[2])
                    taxas.append(col_data[3])
                    valores_futuros.append(col_data[4])
                    valores_presentes.append(col_data[5])

            # Criar o DataFrame com os dados coletados
            df2 = pd.DataFrame({
                'Data': data,
                'Tipo': tipos,
                'Dias úteis': dias_uteis,
                'Taxa': taxas,
                'Valor futuro': valores_futuros,
                'Valor presente': valores_presentes
            })
            df2['Ativo'] = ativo
            df = pd.concat([df, df2])
            # Aguardar um pouco para ver os resultados
            time.sleep(5)
            # Criar um dataframe com os dados

        else:
            print(f"Ativo {ativo} não encontrado no mapeamento!")

    df.to_csv('Dados/ntnb.csv', index=False)
    print("Dados salvos com sucesso!")

except Exception as e:
    print("Erro:", e)


driver.quit()
