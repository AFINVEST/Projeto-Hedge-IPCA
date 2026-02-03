DashHedge — Fluxo Operacional e Objetivo do Sistema
==================================================

## 1) Objetivo do sistema
----------------------
O objetivo do DashHedge é permitir a análise, monitoramento e simulação de hedge
de carteiras de crédito (debêntures, CRI, CRA, etc.) contra curvas de juros
(majoritariamente DAP e NTN-B), integrando:

- Posições reais dos fundos
- Fluxos financeiros dos ativos
- Curvas de mercado (ANBIMA / BBG)
- Spreads entre ativos e benchmarks

Toda a camada de análise e visualização ocorre dentro do arquivo:

    app2.py

Esse arquivo é um aplicativo Streamlit que consome uma base de dados já tratada,
consolidada e enriquecida por scripts de pré-processamento. Nenhum cálculo de
mercado ou scraping ocorre dentro do app — ele apenas consome as bases prontas.


## 2) Regra crítica antes de rodar qualquer coisa (ANBIMA)
-------------------------------------------------------
A ANBIMA possui mecanismos de proteção contra scraping automático.

Antes de executar qualquer script que acesse o site da ANBIMA:
→ O notebook DEVE estar conectado via 3G/4G (hotspot do celular)
→ Nunca via Wi-Fi corporativo ou internet fixa

Caso contrário, o IP será bloqueado e o scraping falhará.


## 3) Etapa 1 — Atualização das curvas e fluxos via ANBIMA (scrap_anbima.py)
------------------------------------------------------------------------
O primeiro passo do fluxo sempre é rodar:

    scrap_anbima.py

Esse script:
- Lê o Relatório de Posição do dia
- Identifica todos os ativos da carteira
- Acessa a calculadora da ANBIMA ativo a ativo
- Para cada ativo:
    • Se a taxa estiver disponível na calculadora, usa diretamente
    • Caso contrário, busca a taxa na página de características do papel
      e a injeta manualmente na calculadora
- Executa a precificação
- Captura a tabela “Fluxo de Pagamento” (juros, amortizações, prazos)
- Consolida todas as debêntures da carteira

O resultado é salvo em:

    Dados/tabela_debentures222.csv

Esse arquivo contém, por ativo:
- Datas de pagamento
- Juros e amortizações
- Prazo em dias úteis
- Fluxo financeiro bruto


## 4) Etapa 2 — Tratamento de exceções e ativos fora da ANBIMA
----------------------------------------------------------
Nem todos os ativos da carteira existem ou funcionam corretamente
na calculadora da ANBIMA.

Para esses casos existe o processo de exceções, executado por:

    excecoes_tratamento.py

Esse script:
- Contém uma lista de ativos problemáticos (debêntures, CRI, CRA)
- Para cada ativo:
    • Busca a taxa indicativa (site interno da AFinvest ou valor manual)
    • Entra na calculadora da XP
    • Seleciona a aba correta (DEB, CRI ou CRA)
    • Digita o código do ativo
    • Preenche a taxa
    • Executa a precificação
    • Extrai a tabela de fluxo (datas, juros, amortizações)

Em seguida, o script:
- Converte os fluxos para o mesmo padrão do CSV da ANBIMA
- Calcula dias úteis até cada pagamento
- Calcula valor presente (VP) usando a taxa anual e base 252
- Gera linhas sintéticas de “Juros | Amortização”

Além disso, ele incorpora:
- Fluxos das NTN-B a partir de Dados/ntnb.csv

No final, o script:
- Lê Dados/tabela_debentures222.csv (ANBIMA)
- Concatena com as exceções calculadas na XP
- Concatena com os fluxos de NTN-B
- Gera a base final:

    Dados/deb_table_completa2.csv


## 5) Etapa 3 — Consolidação e pré-processamento
---------------------------------------------
A partir dos CSVs finais, o sistema constrói bases financeiras usadas pelo app.

Esse pré-processamento gera:
- Fluxos financeiros consolidados por ativo
- DV01 unitário por ativo
- DV01 por fundo
- Mapeamento ativo → vértice DAP
- Séries históricas de spreads
- Bases Parquet para performance e posição

Os resultados ficam salvos principalmente em:

    Dados_Carteira/
    Dados/deb_table_completa2.csv
    Dados/deb_table_completa3.csv


## 6) Etapa 4 — Aplicativo de análise (app2.py)
--------------------------------------------
Somente depois de todas as etapas acima é que se roda:

    streamlit run app2.py

O app2.py:
- NÃO faz scraping
- NÃO consulta ANBIMA
- NÃO consulta XP
- NÃO busca taxas externas

Ele apenas consome as bases prontas e realiza:
- Cálculo de DV01 (DIV1_ATIVO)
- Projeção de juros e amortizações
- Consolidação por fundo
- Cálculo de hedge em DAP
- Análise de spreads

Dentro do app existem quatro grandes módulos:


6.1) Analisar Ativo
------------------
Permite selecionar um ativo específico e:
- Simular quantidade
- Ver fluxos de juros e amortizações
- Ver DV01
- Calcular quantos contratos de DAP fazem hedge
- Exportar a base filtrada


6.2) Analisar Fundo
------------------
Permite selecionar um fundo e:
- Agregar todos os ativos
- Incluir ativos simulados
- Calcular DV01 do fundo
- Calcular hedge em DAP
- Comparar com DAPs já existentes
- Salvar simulações


6.3) Análise Geral
-----------------
Agrega múltiplos fundos e exibe:
- Fluxos consolidados
- DV01 total
- Hedge consolidado


6.4) Analisar Spreads
--------------------
Duas visões principais:

A) NTN-B × DAP  
Compara curvas da ANBIMA e mostra:
- Série histórica dos spreads
- Spreads por vértice (25, 30, 35 etc)

B) Debêntures × NTN-B  
Cruza:
- Posições reais
- Histórico de taxas dos ativos
- Curvas NTN-B
e calcula:
- Spread implícito
- Evolução por ativo
- Evolução por fundo


## 7) Filosofia do sistema
-----------------------
O DashHedge foi desenhado com separação rígida:

| Camada | Função |
|---------------------|------------------------------------|
| scrap_anbima.py     | Coleta fluxos e taxas da ANBIMA     |
| excecoes_tratamento| Calcula ativos fora da ANBIMA via XP|
| scripts de base    | Transformam dados em bases financeiras |
| app2.py            | Apenas analisa e visualiza          |

Isso garante:
- Reprodutibilidade
- Transparência
- Rastreabilidade
- Nenhum cálculo “escondido” no app


## 8) Regra de ouro
----------------
Se os números estiverem errados no app:
→ O erro nunca está no app2.py  
→ O erro está no scraping, nas exceções ou nas bases intermediárias

O app apenas revela o que foi construído antes.
