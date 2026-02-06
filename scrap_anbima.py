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
from pathlib import Path

ARQUIVO_EXCEL = "Dados/debentures-precos-05-02-2026-19-47-15.xlsx"

df = pd.read_excel(ARQUIVO_EXCEL)

colunas_necessarias = {"Código", "Tipo Remuneração"}
colunas_faltantes = colunas_necessarias - set(df.columns)

if colunas_faltantes:
    raise ValueError(f"Colunas ausentes no arquivo: {colunas_faltantes}")

mask_ipca = (
    df["Tipo Remuneração"]
    .astype(str)
    .str.contains("IPCA", case=False, na=False)
)

ativos = (
    df.loc[mask_ipca, "Código"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
)

Cra = ["CRA Ferroeste 2ª Série", "CRI Bem Brasil"]

df_posicao = pd.read_excel(
    'Dados/Relatório de Posição 2026-01-26.xlsx', sheet_name='Worksheet'
)

df_posicao = df_posicao[df_posicao['Ativo'].isin(ativos)]
ativos = df_posicao['Ativo'].unique()
ativos = ativos.tolist()

Nao_achei = ["LFSN210099R"]

deu_ruim = ["BRFS31", 'NTN-B50', 'CRTA12', 'CPLD15',
            'CTGE11', 'LSVE39', 'SUMI37', 'SUZBC1']

# =========================================================
# FUNÇÕES (BACKUP / PREENCHIMENTO ATIVO)
# =========================================================
def _norm_str(x):
    # normaliza valores para comparação de chave
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    # evita diferenças triviais de espaços
    s = " ".join(s.split())
    return s

def _build_key(row, key_cols):
    # chave determinística baseada em TODAS as colunas exceto 'Ativo'
    return tuple(_norm_str(row.get(c, "")) for c in key_cols)

def preencher_ativo_com_backup(df_atual, backup_path):
    """
    Antes de sobrescrever o arquivo final:
    - lê o backup (se existir)
    - identifica linhas no df_atual com Ativo vazio
    - tenta preencher com base em uma chave composta das demais colunas
    """
    try:
        backup_path = Path(backup_path)
        if not backup_path.exists():
            print(f"[BACKUP] Arquivo não existe, nada a preencher: {backup_path}")
            return df_atual

        df_backup = pd.read_csv(backup_path)

        if "Ativo" not in df_backup.columns:
            print("[BACKUP] Backup não tem coluna 'Ativo'. Nada a preencher.")
            return df_atual

        if "Ativo" not in df_atual.columns:
            print("[BACKUP] DF atual não tem coluna 'Ativo'. Nada a preencher.")
            return df_atual

        # garante mesmo conjunto mínimo de colunas para montar chave
        # chave = todas as colunas do df_atual exceto 'Ativo'
        key_cols = [c for c in df_atual.columns if c != "Ativo"]
        if len(key_cols) == 0:
            print("[BACKUP] Não há colunas suficientes para criar chave (exceto 'Ativo').")
            return df_atual

        # cria mapa: key -> Ativo (preferindo Ativo não vazio)
        mapa = {}
        for _, r in df_backup.iterrows():
            ativo_bk = _norm_str(r.get("Ativo", ""))
            if ativo_bk == "":
                continue
            k = _build_key(r, key_cols)
            # se ainda não existe, grava
            if k not in mapa:
                mapa[k] = ativo_bk

        # encontra linhas com Ativo vazio no df_atual
        ativo_series = df_atual["Ativo"].apply(_norm_str)
        mask_vazio = (ativo_series == "")

        qtd_vazios = int(mask_vazio.sum())
        print(f"[BACKUP] Linhas com 'Ativo' vazio no DF atual: {qtd_vazios}")

        if qtd_vazios == 0:
            return df_atual

        # preencher usando o mapa
        preenchidos = 0
        for idx, r in df_atual.loc[mask_vazio].iterrows():
            k = _build_key(r, key_cols)
            if k in mapa:
                df_atual.at[idx, "Ativo"] = mapa[k]
                preenchidos += 1

        print(f"[BACKUP] Linhas preenchidas a partir do backup: {preenchidos}")
        if preenchidos < qtd_vazios:
            print(f"[BACKUP] ATENÇÃO: {qtd_vazios - preenchidos} linhas permaneceram com 'Ativo' vazio (sem match no backup).")

        return df_atual

    except Exception as e:
        print(f"[BACKUP] Erro ao tentar preencher 'Ativo' com backup: {e}")
        return df_atual


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
            f"https://data.anbima.com.br/ferramentas/calculadora/debentures/{ativos[i]}?ativo=debentures"
        )

        # Configura um tempo de espera máximo de 20 segundos
        wait = WebDriverWait(driver, 20)
        driver.execute_script("document.body.style.zoom='60%'")

        try:
            # Verifica se o elemento está presente
            time.sleep(2)
            elemento = driver.find_element(
                By.XPATH, "//p[contains(text(), 'Taxa ANBIMA do ativo')]"
            )
            taxa_anbima_encontrada = True
        except:
            taxa_anbima_encontrada = False

        if taxa_anbima_encontrada:
            # Aguardar até que o botão esteja clicável
            button = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "#card-calcular-precificacao > article > article > section > div > form > div.col-xs-12.precificacao-content__calculate-button.col-no-padding > button")
            ))
            # Clicar no botão
            button.click()
            # Aguarde para garantir que a tabela carregue após o clique
            time.sleep(5)
            # Aguardar a tabela carregar
            table_element = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#card-fluxo-pagamento > article > article > section > div > div > table")
            ))
            print("Tabela carregada com sucesso!")
            # Capturar o conteúdo da tabela com BeautifulSoup
            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.select_one(
                "#card-fluxo-pagamento > article > article > section > div > div > table"
            )
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
                f"https://data.anbima.com.br/debentures/{ativos[i]}/caracteristicas"
            )
            driver.execute_script("document.body.style.zoom='60%'")

            # Configura um tempo de espera máximo de 20 segundos
            wait = WebDriverWait(driver, 20)
            try:
                driver.maximize_window()
                # Localiza o elemento com a classe 'lower-card-item-value'
                taxa_elemento = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "p.lower-card-item-value")
                ))

                # Extrai o texto do elemento
                taxa_texto = taxa_elemento.text

                # Remove o símbolo de porcentagem e converte para número
                taxa_valor = float(taxa_texto.replace(" %", "").replace(",", "."))

                # Armazena o valor da taxa
                print(f"Taxa ANBIMA encontrada: {taxa_valor}")
                driver.get(
                    f"https://data.anbima.com.br/ferramentas/calculadora/debentures/{ativos[i]}?ativo=debentures"
                )

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
                    (By.CSS_SELECTOR, "#card-calcular-precificacao > article > article > section > div > form > div.col-xs-12.precificacao-content__calculate-button.col-no-padding > button")
                ))
                # Clicar no botão
                button.click()
                # Aguarde para garantir que a tabela carregue após o clique
                time.sleep(6)

                # Aguardar a tabela carregar
                table_element = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#card-fluxo-pagamento > article > article > section > div > div > table")
                ))
                print("Tabela carregada com sucesso!")

                # Capturar o conteúdo da tabela com BeautifulSoup
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.select_one(
                    "#card-fluxo-pagamento > article > article > section > div > div > table"
                )

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
               "Prazos (dias úteis)", "Dias entre pagamentos", "Expectativa de juros (%)",
               "Juros projetados", "Amortizações", "Fluxo descontado (R$)", "Ativo"]
    df.columns = columns

    # =========================================================
    # NOVO: Antes de sobrescrever, carrega o output antigo e
    #       preenche 'Ativo' vazio no DF atual usando o backup.
    # =========================================================
    OUTPUT_PATH = Path("Dados/tabela_debentures222.csv")

    # (1) tenta preencher quaisquer linhas com Ativo vazio usando backup
    df = preencher_ativo_com_backup(df, OUTPUT_PATH)

    # (2) somente depois sobrescreve o arquivo final
    df.to_csv(OUTPUT_PATH, index=False)
    print("Tabela salva com sucesso!")


except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    # Fechar o navegador
    driver.quit()
