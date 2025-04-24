# (Mantendo as importações originais)
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import io


# NOVA importação para usar ggplot em Python
from plotnine import ggplot, aes, geom_col, labs, theme, theme_bw, element_text

st.set_page_config(
    page_title="Dashboard de Análise",
    layout="wide",
    initial_sidebar_state="expanded"
)

def process_df():
    # Simulação de carga de dados - substituir com seus arquivos reais
    df_posicao = pd.read_excel('Relatório de Posição 2025-04-22.xlsx', sheet_name='Worksheet')
    df_debentures = pd.read_csv('deb_table_completa2.csv')
    df_debentures['Juros projetados'] = (
        df_debentures['Juros projetados']
        .str.replace('-', '0')
        .str.replace('.', '')
        .str.replace(',', '.')
        .astype(float)
    )
    
    df_debentures['Fluxo descontado (R$)'] = (
        df_debentures['Fluxo descontado (R$)']
        .str.replace('.', '')
        .str.replace(',', '.')
        .astype(float)
    )
    df_debentures['Amortizações'] = (
        df_debentures['Amortizações']
        .str.replace('-', '0')
        .str.replace('.', '')
        .str.replace(',', '.')
        .astype(float)
    )

    # Processamento dos dados
    #ativos = ['ACRC21', 'AEAB11', 'AEGP23', 'AESL17', 'AESLA5', 'AESLA7', 'AESLB7', 'AESOA1', 'AGVF12', 'AHGD13', 'ALGA27', 'ALGAB1', 'ALGAC2', 'ALGE16', 'ALGTA4', 'ALIG12', 'ALIG13', 'ALIG15', 'ALUP18', 'ANET11', 'ANET12', 'APFD19', 'APPSA2', 'APRB18', 'ARTR19', 'ASAB11', 'ASCP13', 'ASCP23', 'ASER12', 'ASSR21', 'ATHT11', 'ATII12', 'AURE12', 'BARU11', 'BCPSA5', 'BHSA11', 'BLMN12', 'BRFS31', 'BRKP28', 'BRST11', 'CAEC12', 'CAEC21', 'CAJS11', 'CAJS12', 'CART13', 'CASN23', 'CBAN12', 'CBAN32', 'CBAN52', 'CBAN72', 'CCLS11', 'CCROA5', 'CCROB4', 'CCROB6', 'CDES11', 'CEAD11', 'CEAP12', 'CEAP14', 'CEAP17', 'CEAR26', 'CEEBA1', 'CEEBB6', 'CEEBB7', 'CEEBC3', 'CEEBC4', 'CEED12', 'CEED13', 'CEED15', 'CEED17', 'CEED21', 'CEMT19', 'CEPE19', 'CEPEB3', 'CEPEC1', 'CEPEC2', 'CESE32', 'CESPA2', 'CESPA3', 'CGASA1', 'CGASA2', 'CGASB1', 'CGMG18', 'CGOS13', 'CGOS16', 'CGOS24', 'CGOS28', 'CGOS34', 'CHSF13', 'CJEN13', 'CLAG13', 'CLCD26', 'CLCD27', 'CLNG11', 'CLTM14', 'CMGD27', 'CMGD28', 'CMGDB0', 'CMGDB1', 'CMIN11', 'CMIN12', 'CMIN21', 'CMIN22', 'CMTR29', 'CNRD11', 'COCE18', 'COMR14', 'COMR15', 'CONF11', 'CONX12', 'CPFGA2', 'CPFPA0', 'CPFPA5', 'CPFPA7', 'CPFPB7', 'CPGT15', 'CPGT26', 'CPGT27', 'CPGT28', 'CPLD15', 'CPLD26', 'CPLD29', 'CPLD37', 'CPTM15', 'CPXB22', 'CRMG15', 'CRTR12', 'CSAN33', 'CSMGA2', 'CSMGA6', 'CSMGB4', 'CSMGB8', 'CSMGB9', 'CSMGC3', 'CSNAA4', 'CSNAA5', 'CSNAA6', 'CSNAB4', 'CSNAB5', 'CSNAB6', 'CSNAC4', 'CSNP12', 'CSRN19', 'CSRN29', 'CSRNA1', 'CSRNB2', 'CSRNC0', 'CTEE17', 'CTEE18', 'CTEE1B', 'CTEE29', 'CTEE2B', 'CTGE11', 'CTGE13', 'CTGE15', 'CTNS14', 'CTRR11', 'CUTI11', 'CXER12', 'DESK17', 'EBAS13', 'EBENA8', 'ECER12', 'ECHP11', 'ECHP12', 'ECHP22', 'ECOV16', 'ECPN11', 'EDFT11', 'EDPA11', 'EDPT11', 'EDTE12', 'EDVP14', 'EDVP17', 'EEELA0', 'EEELA1', 'EEELB1', 'EGIE17', 'EGIE19', 'EGIE27', 'EGIE29', 'EGIE39', 'EGIE49', 'EGIEA0', 'EGIEA1', 'EGIEB1', 'EGIEB2', 'EGVG11', 'EGVG21', 'EKTRB3', 'EKTRC0', 'EKTRC1', 'EKTT11', 'ELEK37', 'ELET14', 'ELET16', 'ELET23', 'ELET42', 'ELPLA5', 'ELPLA7', 'ELPLB4', 'ELTN15', 'ENAT11', 'ENAT12', 'ENAT13', 'ENAT14', 'ENAT24', 'ENAT33', 'ENERA1', 'ENERB4', 'ENEV13', 'ENEV15', 'ENEV16', 'ENEV18', 'ENEV19', 'ENEV26', 'ENEV28', 'ENEV29', 'ENEV32', 'ENEV39', 'ENEVA0', 'ENEVB0', 'ENGI39', 'ENGIA1', 'ENGIA4', 'ENGIA5', 'ENGIA6', 'ENGIA9', 'ENGIB0', 'ENGIB2', 'ENGIB4', 'ENGIB6', 'ENGIB9', 'ENGIC0', 'ENJG21', 'ENMI21', 'ENMTA3', 'ENMTA4', 'ENMTA5', 'ENMTA7', 'ENMTB3', 'ENMTB5', 'ENSEA1', 'ENTV12', 'EQMAA0', 'EQMAA2', 'EQPA18', 'EQSP11', 'EQSP21', 'EQTC11', 'EQTN11', 'EQTR11', 'EQTR21', 'EQTS11', 'EQUA11', 'ERDV17', 'ERDV38', 'ERDVA4', 'ERDVB4', 'ERDVC3', 'ERDVC4', 'ESAM14', 'ESULA1', 'ESULA6', 'ETAP22', 'ETBA12', 'ETEN11', 'ETEN12', 'ETEN21', 'ETEN22', 'ETEN31', 'ETSP12', 'EUBE11', 'EXTZ11', 'FBRI13', 'FGEN13', 'FLCLA0', 'FRAG14', 'FURN21', 'GASC15', 'GASC16', 'GASC17', 'GASC22', 'GASC23', 'GASC25', 'GASC26', 'GASC27', 'GASP19', 'GASP29', 'GASP34', 'GBSP11', 'GEPA28', 'GRRB24', 'GSTS14', 'GSTS24', 'HARG11', 'HBSA11', 'HBSA21', 'HGLB23', 'HVSP11', 'HZTC14', 'IBPB11', 'IGSN15', 'IRJS14', 'IRJS15', 'ITPO14', 'IVIAA0', 'JALL11', 'JALL13', 'JALL14', 'JALL15', 'JALL21', 'JALL24', 'JSMLB5', 'JTEE11', 'JTEE12', 'KLBNA5', 'LCAMD1', 'LCAMD3', 'LGEN11', 'LIGH1B', 'LIGH2B', 'LIGHA5', 'LIGHB4', 'LIGHB6', 'LIGHB7', 'LIGHC3', 'LIGHC4', 'LIGHC6', 'LIGHC7', 'LIGHD2', 'LIGHD3', 'LIGHE2', 'LORTA7', 'LSVE39', 'LTTE15', 'MEZ511', 'MGSP12', 'MNAU13', 'MOVI18', 'MOVI37', 'MRSAA1', 'MRSAA2', 'MRSAB1', 'MRSAB2', 'MRSAC1', 'MRSAC2', 'MSGT12', 'MSGT13', 'MSGT23', 'MSGT33', 'MTRJ19', 'MVLV16', 'NEOE16', 'NEOE26', 'NMCH11', 'NRTB11', 'NRTB21', 'NTEN11', 'ODTR11', 'ODYA11', 'OMGE12', 'OMGE22', 'OMGE31', 'OMGE41', 'OMNG12', 'ORIG11', 'PALF38', 'PALFA3', 'PALFB3', 'PASN12', 'PEJA11', 'PEJA22', 'PEJA23', 'PETR16', 'PETR17', 'PETR26', 'PETR27', 'PLSB1A', 'POTE11', 'POTE12', 'PPTE11', 'PRAS11', 'PRPO12', 'PRTE12', 'PTAZ11', 'QUAT13', 'RAHD11', 'RAIZ13', 'RAIZ23', 'RATL11', 'RDOE18', 'RDVE11', 'RECV11', 'RESA14', 'RESA15', 'RESA17', 'RESA27', 'RIGEA3', 'RIPR22', 'RIS412', 'RIS414', 'RIS422', 'RIS424', 'RISP12', 'RISP14', 'RISP22', 'RISP24', 'RMSA12', 'RRRP13', 'RSAN16', 'RSAN26', 'RSAN34', 'RSAN44', 'RUMOA2', 'RUMOA3', 'RUMOA4', 'RUMOA5', 'RUMOA6', 'RUMOA7', 'RUMOB1', 'RUMOB3', 'RUMOB5', 'RUMOB6', 'RUMOB7', 'SABP12', 'SAELA1', 'SAELA3', 'SAELB3', 'SAPR10', 'SAPRA2', 'SAPRA3', 'SAPRB3', 'SAVI13', 'SBSPB6', 'SBSPC4', 'SBSPC6', 'SBSPD4', 'SBSPE3', 'SBSPE9', 'SBSPF3', 'SBSPF9', 'SERI11', 'SMTO14', 'SMTO24', 'SNRA13', 'SPRZ11', 'SRTI11', 'STBP35', 'STBP45', 'STRZ11', 'SUMI17', 'SUMI18', 'SUMI37', 'SUZB19', 'SUZB29', 'SUZBA0', 'SUZBC1', 'TAEB15', 'TAEE17', 'TAEE18', 'TAEE26', 'TAEEA2', 'TAEEA4', 'TAEEA7', 'TAEEB2', 'TAEEB4', 'TAEEC2', 'TAEEC4', 'TAEED2', 'TAES15', 'TBEG11', 'TBLE26', 'TCII11', 'TEPA12', 'TIET18', 'TIET29', 'TIET39', 'TIMS12', 'TNHL11', 'TOME12', 'TPEN11', 'TPNO12', 'TPNO13', 'TRCC11', 'TRGO11', 'TRPLA4', 'TRPLA7', 'TRPLB4', 'TRPLB7', 'TSSG21', 'TVVH11', 'UHSM12', 'UNEG11', 'UNTE11', 'USAS11', 'UTPS11', 'UTPS12', 'UTPS21', 'UTPS22', 'VALE38', 'VALE48', 'VALEA0', 'VALEB0', 'VALEC0', 'VAMO33', 'VAMO34', 'VBRR11', 'VDBF12', 'VDEN12', 'VERO12', 'VERO13', 'VERO24', 'VERO44', 'VLIM13', 'VLIM14', 'VLIM15', 'VLIM16', 'VPLT12', 'VRDN12', 'WDPR11', 'XNGU17']
    #Sem LIGHT
    ativos = ['ACRC21', 'AEAB11', 'AEGP23', 'AESL17', 'AESLA5', 'AESLA7', 'AESLB7', 'AESOA1', 'AGVF12', 'AHGD13', 'ALGA27', 'ALGAB1', 'ALGAC2', 'ALGE16', 'ALGTA4', 'ALIG12', 'ALIG13', 'ALIG15', 'ALUP18', 'ANET11', 'ANET12', 'APFD19', 'APPSA2', 'APRB18', 'ARTR19', 'ASAB11', 'ASCP13', 'ASCP23', 'ASER12', 'ASSR21', 'ATHT11', 'ATII12', 'AURE12', 'BARU11', 'BCPSA5', 'BHSA11', 'BLMN12', 'BRFS31', 'BRKP28', 'BRST11', 'CAEC12', 'CAEC21', 'CAJS11', 'CAJS12', 'CART13', 'CASN23', 'CBAN12', 'CBAN32', 'CBAN52', 'CBAN72', 'CCLS11', 'CCROA5', 'CCROB4', 'CCROB6', 'CDES11', 'CEAD11', 'CEAP12', 'CEAP14', 'CEAP17', 'CEAR26', 'CEEBA1', 'CEEBB6', 'CEEBB7', 'CEEBC3', 'CEEBC4', 'CEED12', 'CEED13', 'CEED15', 'CEED17', 'CEED21', 'CEMT19', 'CEPE19', 'CEPEB3', 'CEPEC1', 'CEPEC2', 'CESE32', 'CESPA2', 'CESPA3', 'CGASA1', 'CGASA2', 'CGASB1', 'CGMG18', 'CGOS13', 'CGOS16', 'CGOS24', 'CGOS28', 'CGOS34', 'CHSF13', 'CJEN13', 'CLAG13', 'CLCD26', 'CLCD27', 'CLNG11', 'CLTM14', 'CMGD27', 'CMGD28', 'CMGDB0', 'CMGDB1', 'CMIN11', 'CMIN12', 'CMIN21', 'CMIN22', 'CMTR29', 'CNRD11', 'COCE18', 'COMR14', 'COMR15', 'CONF11', 'CONX12', 'CPFGA2', 'CPFPA0', 'CPFPA5', 'CPFPA7', 'CPFPB7', 'CPGT15', 'CPGT26', 'CPGT27', 'CPGT28', 'CPLD15', 'CPLD26', 'CPLD29', 'CPLD37', 'CPTM15', 'CPXB22', 'CRMG15', 'CRTR12', 'CSAN33', 'CSMGA2', 'CSMGA6', 'CSMGB4', 'CSMGB8', 'CSMGB9', 'CSMGC3', 'CSNAA4', 'CSNAA5', 'CSNAA6', 'CSNAB4', 'CSNAB5', 'CSNAB6', 'CSNAC4', 'CSNP12', 'CSRN19', 'CSRN29', 'CSRNA1', 'CSRNB2', 'CSRNC0', 'CTEE17', 'CTEE18', 'CTEE1B', 'CTEE29', 'CTEE2B', 'CTGE11', 'CTGE13', 'CTGE15', 'CTNS14', 'CTRR11', 'CUTI11', 'CXER12', 'DESK17', 'EBAS13', 'EBENA8', 'ECER12', 'ECHP11', 'ECHP12', 'ECHP22', 'ECOV16', 'ECPN11', 'EDFT11', 'EDPA11', 'EDPT11', 'EDTE12', 'EDVP14', 'EDVP17', 'EEELA0', 'EEELA1', 'EEELB1', 'EGIE17', 'EGIE19', 'EGIE27', 'EGIE29', 'EGIE39', 'EGIE49', 'EGIEA0', 'EGIEA1', 'EGIEB1', 'EGIEB2', 'EGVG11', 'EGVG21', 'EKTRB3', 'EKTRC0', 'EKTRC1', 'EKTT11', 'ELEK37', 'ELET14', 'ELET16', 'ELET23', 'ELET42', 'ELPLA5', 'ELPLA7', 'ELPLB4', 'ELTN15', 'ENAT11', 'ENAT12', 'ENAT13', 'ENAT14', 'ENAT24', 'ENAT33', 'ENERA1', 'ENERB4', 'ENEV13', 'ENEV15', 'ENEV16', 'ENEV18', 'ENEV19', 'ENEV26', 'ENEV28', 'ENEV29', 'ENEV32', 'ENEV39', 'ENEVA0', 'ENEVB0', 'ENGI39', 'ENGIA1', 'ENGIA4', 'ENGIA5', 'ENGIA6', 'ENGIA9', 'ENGIB0', 'ENGIB2', 'ENGIB4', 'ENGIB6', 'ENGIB9', 'ENGIC0', 'ENJG21', 'ENMI21', 'ENMTA3', 'ENMTA4', 'ENMTA5', 'ENMTA7', 'ENMTB3', 'ENMTB5', 'ENSEA1', 'ENTV12', 'EQMAA0', 'EQMAA2', 'EQPA18', 'EQSP11', 'EQSP21', 'EQTC11', 'EQTN11', 'EQTR11', 'EQTR21', 'EQTS11', 'EQUA11', 'ERDV17', 'ERDV38', 'ERDVA4', 'ERDVB4', 'ERDVC3', 'ERDVC4', 'ESAM14', 'ESULA1', 'ESULA6', 'ETAP22', 'ETBA12', 'ETEN11', 'ETEN12', 'ETEN21', 'ETEN22', 'ETEN31', 'ETSP12', 'EUBE11', 'EXTZ11', 'FBRI13', 'FGEN13', 'FLCLA0', 'FRAG14', 'FURN21', 'GASC15', 'GASC16', 'GASC17', 'GASC22', 'GASC23', 'GASC25', 'GASC26', 'GASC27', 'GASP19', 'GASP29', 'GASP34', 'GBSP11', 'GEPA28', 'GRRB24', 'GSTS14', 'GSTS24', 'HARG11', 'HBSA11', 'HBSA21', 'HGLB23', 'HVSP11', 'HZTC14', 'IBPB11', 'IGSN15', 'IRJS14', 'IRJS15', 'ITPO14', 'IVIAA0', 'JALL11', 'JALL13', 'JALL14', 'JALL15', 'JALL21', 'JALL24', 'JSMLB5', 'JTEE11', 'JTEE12', 'KLBNA5', 'LCAMD1', 'LCAMD3', 'LGEN11', 'LORTA7', 'LSVE39', 'LTTE15', 'MEZ511', 'MGSP12', 'MNAU13', 'MOVI18', 'MOVI37', 'MRSAA1', 'MRSAA2', 'MRSAB1', 'MRSAB2', 'MRSAC1', 'MRSAC2', 'MSGT12', 'MSGT13', 'MSGT23', 'MSGT33', 'MTRJ19', 'MVLV16', 'NEOE16', 'NEOE26', 'NMCH11', 'NRTB11', 'NRTB21', 'NTEN11', 'ODTR11', 'ODYA11', 'OMGE12', 'OMGE22', 'OMGE31', 'OMGE41', 'OMNG12', 'ORIG11', 'PALF38', 'PALFA3', 'PALFB3', 'PASN12', 'PEJA11', 'PEJA22', 'PEJA23', 'PETR16', 'PETR17', 'PETR26', 'PETR27', 'PLSB1A', 'POTE11', 'POTE12', 'PPTE11', 'PRAS11', 'PRPO12', 'PRTE12', 'PTAZ11', 'QUAT13', 'RAHD11', 'RAIZ13', 'RAIZ23', 'RATL11', 'RDOE18', 'RDVE11', 'RECV11', 'RESA14', 'RESA15', 'RESA17', 'RESA27', 'RIGEA3', 'RIPR22', 'RIS412', 'RIS414', 'RIS422', 'RIS424', 'RISP12', 'RISP14', 'RISP22', 'RISP24', 'RMSA12', 'RRRP13', 'RSAN16', 'RSAN26', 'RSAN34', 'RSAN44', 'RUMOA2', 'RUMOA3', 'RUMOA4', 'RUMOA5', 'RUMOA6', 'RUMOA7', 'RUMOB1', 'RUMOB3', 'RUMOB5', 'RUMOB6', 'RUMOB7', 'SABP12', 'SAELA1', 'SAELA3', 'SAELB3', 'SAPR10', 'SAPRA2', 'SAPRA3', 'SAPRB3', 'SAVI13', 'SBSPB6', 'SBSPC4', 'SBSPC6', 'SBSPD4', 'SBSPE3', 'SBSPE9', 'SBSPF3', 'SBSPF9', 'SERI11', 'SMTO14', 'SMTO24', 'SNRA13', 'SPRZ11', 'SRTI11', 'STBP35', 'STBP45', 'STRZ11', 'SUMI17', 'SUMI18', 'SUMI37', 'SUZB19', 'SUZB29', 'SUZBA0', 'SUZBC1', 'TAEB15', 'TAEE17', 'TAEE18', 'TAEE26', 'TAEEA2', 'TAEEA4', 'TAEEA7', 'TAEEB2', 'TAEEB4', 'TAEEC2', 'TAEEC4', 'TAEED2', 'TAES15', 'TBEG11', 'TBLE26', 'TCII11', 'TEPA12', 'TIET18', 'TIET29', 'TIET39', 'TIMS12', 'TNHL11', 'TOME12', 'TPEN11', 'TPNO12', 'TPNO13', 'TRCC11', 'TRGO11', 'TRPLA4', 'TRPLA7', 'TRPLB4', 'TRPLB7', 'TSSG21', 'TVVH11', 'UHSM12', 'UNEG11', 'UNTE11', 'USAS11', 'UTPS11', 'UTPS12', 'UTPS21', 'UTPS22', 'VALE38', 'VALE48', 'VALEA0', 'VALEB0', 'VALEC0', 'VAMO33', 'VAMO34', 'VBRR11', 'VDBF12', 'VDEN12', 'VERO12', 'VERO13', 'VERO24', 'VERO44', 'VLIM13', 'VLIM14', 'VLIM15', 'VLIM16', 'VPLT12', 'VRDN12', 'WDPR11', 'XNGU17']
    outros = ["BRFS31","CRA Ferroeste 2ª Série","CRI Bem Brasil", "NTN-B26", "NTN-B28","NTN-B30", "NTN-B32","NTN-B50",'CRI Vic Engenharia 1ª Emissão','TBCR18','CRTA12','CERT11','CRI PERNAMBUCO 35ª (23J1753853)','CRI Vic Engenharia 2ª Emissão']
    ativos = ativos + outros
    dap_dict = {
    2025: 'DAP25',
    2026: 'DAP26',
    2027: 'DAP27',
    2028: 'DAP28',
    2029: 'DAP29',  # Ano 2029 agora no DAP29 (incrementado)
    2030: 'DAP30',  # Inclui ano 31 (2031)
    2031: 'DAP30',  
    2032: 'DAP32',  # Inclui ano 33 (2033)
    2033: 'DAP32',
    2034: 'DAP35',  # Inclui anos 34,36,37
    2035: 'DAP35',  # (ano 35 incluso por padrão)
    2036: 'DAP35',
    2037: 'DAP35',
    2038: 'DAP40',  # Demais anos acima de 37
    2039: 'DAP40',
    2040: 'DAP40',
    2041: 'DAP40',
    2042: 'DAP40',
    2043: 'DAP40',  # Original NTN-B45 convertido para DAP40
    2044: 'DAP40',
    2045: 'DAP40'
}
    df_posicao = df_posicao[df_posicao['Ativo'].isin(ativos)]

    df_debentures.columns = ["Dados do evento", "Data de pagamento", "Prazos (dias úteis)",
                            "Dias entre pagamentos", "Expectativa de juros (%)", 
                            "Juros projetados", "Amortizações", "Fluxo descontado (R$)", "Ativo"]
    df_debentures = df_debentures[df_debentures['Ativo'].isin(df_posicao['Ativo'])]
    # Processamento de datas
    df_debentures['Data de pagamento'] = pd.to_datetime(df_debentures['Data de pagamento'])
    df_debentures['Data de pagamento_str'] = df_debentures['Data de pagamento'].dt.strftime('%Y-%m')  # Manter como string
    
    # Criação de colunas temporais
    df_debentures['Ano'] = df_debentures['Data de pagamento'].dt.year
    df_debentures['Semestre'] = df_debentures['Data de pagamento'].dt.quarter.replace(
        {1: '1º Semestre', 2: '1º Semestre', 3: '2º Semestre', 4: '2º Semestre'})

    # Merge dos dados
    df_quantidade = df_posicao.groupby(['Fundo','Ativo']).sum()[['Quantidade','Valor']].reset_index()
    df_quantidade['Valor'] = df_quantidade['Valor'].astype(str).str.replace(',', '.').astype(float)
    df_debentures.drop(columns=['Data de pagamento'], inplace=True)

    df_posicao_juros = pd.merge(df_debentures, df_quantidade, on='Ativo', how='left')   
    #st.dataframe(df_posicao_juros)

    df_posicao_juros['Juros projetados'] = df_posicao_juros['Fluxo descontado (R$)'] * df_posicao_juros['Quantidade']
    df_posicao_juros['Amortizações'] = df_posicao_juros['Amortizações'] * df_posicao_juros['Quantidade']
    df_posicao_juros['Juros projetados'] = df_posicao_juros['Juros projetados']
    df_posicao_juros['DIV1_ATIVO'] = df_posicao_juros['Juros projetados'] * 0.0001 * (df_posicao_juros['Prazos (dias úteis)']/252)
    df_posicao_juros['DAP'] = df_posicao_juros['Ano'].map(dap_dict)
    
    return df_posicao_juros.rename(columns={'Data de pagamento_str': 'Data de pagamento'})

def process_div01():
    df_div1 = pd.read_excel("AF_Trading.xlsm", sheet_name="Base IPCA", skiprows=16)
    df_div1 = df_div1.iloc[:, :13]
    df_div1 = df_div1.dropna()
    #Manter somente a coluna DAP e DV01
    df_div1 = df_div1[['DAP', 'DV01']]
    #Deixar as 3 primeiras e 2 ultimas strings da coluna DAP
    df_div1['DAP'] = df_div1['DAP'].apply(lambda x: x[:3] + x[-2:] if isinstance(x, str) and len(x) >= 5 else x)
    return df_div1

def main():
    #st.title("Painel de Crédito V1")
    df = process_df()
    df_div1 = process_div01()


    # Definição de colunas
    categorical_cols = {"Fundo", "Ativo", "Data de pagamento", "Semestre"}
    numeric_cols = {"Quantidade", "Valor", "Juros projetados", "Ano"}
    
    # Filtros na sidebar
    st.sidebar.header("Filtros")
    default_filters = [ "Ano","Fundo", "Semestre",'Ativo']
    
    selected_filters = st.sidebar.multiselect(
        "Selecione os filtros:",
        list(categorical_cols.union(numeric_cols)),
        default=default_filters
    )

    # Aplicação de filtros
    filters = {}
    for col in selected_filters:
        if col in categorical_cols and col != 'Ativo':
            unique_vals = df[col].unique().tolist()
            selected = st.sidebar.multiselect(f"Filtrar {col}:", unique_vals)
            if col == "Fundo":
                lista_fundos = selected
            if selected:
                filters[col] = selected
        elif col in numeric_cols:
            min_val = float(df[col].min())
            max_val = float(df[col].max())
            if col == "Ano":
                selected_range = st.sidebar.slider(
                    f"Filtrar {col}:",
                    int(min_val),
                    int(max_val),
                    (int(min_val), int(max_val)),step=1)
                filters[col] = selected_range
            else:            
                selected_range = st.sidebar.slider(
                    f"Filtrar {col}:",
                    min_val,
                    max_val,
                    (min_val, max_val))
                filters[col] = selected_range

    # Filtragem de dados
    df_filtered = df.copy()
    for col, value in filters.items():
        if col in categorical_cols:
            df_filtered = df_filtered[df_filtered[col].isin(value)]
        elif col in numeric_cols:
            df_filtered = df_filtered[
                (df_filtered[col] >= value[0]) & 
                (df_filtered[col] <= value[1])]
    
    # Filtrando opções de "Ativo" com base nos filtros já aplicados
    ativos_disponiveis = df_filtered["Ativo"].unique().tolist()
    if "Ativo" in selected_filters:
        selected_ativos = st.sidebar.multiselect("Filtrar Ativo:", ativos_disponiveis)
        # Aplicando filtro de Ativo
        if selected_ativos:
            df_filtered = df_filtered[df_filtered["Ativo"].isin(selected_ativos)]
    
    # ------------------------------------------------------------------
    # NOVA SEÇÃO: TESTAR NOVAS QUANTIDADES (MÚLTIPLOS ATIVOS)
    # ------------------------------------------------------------------
    st.sidebar.write("---")
    st.sidebar.subheader("Testar novas quantidades")
    testar_nova_qtd = st.sidebar.checkbox("Deseja simular nova quantidade de vários ativos?")
    if testar_nova_qtd:
        # Múltipla seleção de ativos disponíveis
        #Criar checkbox para usar os selected_ativos ou os ativos_disponiveis
        repetir_ativos = st.sidebar.checkbox("Repetir ativos filtrados", value=False)
        novos_ativos = st.sidebar.checkbox("Deseja simular com novos ativos no fundo?")
        #Preciso completar a lógica para o novos_ativos
        if novos_ativos:
            # Lista com todos os ativos disponíveis no DataFrame original
            ativos_possiveis = df['Ativo'].unique().tolist()

            # Multiselect para o usuário escolher novos ativos
            ativos_possiveis_escolhidos = st.sidebar.multiselect("Selecione ativos para adicionar ao fundo:", ativos_possiveis)

            # Atualiza a lista de ativos disponíveis
            ativos_disponiveis += ativos_possiveis_escolhidos
            ativos_disponiveis = list(set(ativos_disponiveis))
            ativos_disponiveis.sort()
            for ativo in ativos_possiveis_escolhidos:
                # Filtra os dados dos ativos escolhidos e concatena ao df_filtered
                novos_dados = df[df['Ativo'].isin(ativos_possiveis_escolhidos)]
                #Deixar somente os ativos do primeiro fundo que o usuario selecionar
                if novos_dados['Fundo'].nunique() > 1:
                    fundo_selecionado = st.sidebar.selectbox(f"Selecione o fundo do ativo {ativo}:", novos_dados['Fundo'].unique(), index=0)
                    novos_dados = novos_dados[novos_dados['Fundo'] == fundo_selecionado]
                # Concatenar os novos dados ao DataFrame filtrado
                df_filtered = pd.concat([df_filtered, novos_dados], ignore_index=True)

        if repetir_ativos:
            ativos_para_teste = st.sidebar.multiselect("Selecione ativos para teste:", ativos_disponiveis, default=selected_ativos)
        else:
            ativos_para_teste = st.sidebar.multiselect("Selecione ativos para teste:", ativos_disponiveis)

        # Form para alterar quantidades
        novas_quantidades = {}
        with st.sidebar.form(key="form_alterar_qtd"):
            for ativo in ativos_para_teste:
                # Pega a quantidade atual
                qtd_atual_array = df_filtered.loc[df_filtered['Ativo'] == ativo, 'Quantidade'].unique()
                qtd_atual_val = qtd_atual_array[0] if len(qtd_atual_array) else 0
                
                # Input numérico para cada ativo selecionado
                nova_qtd = st.number_input(
                    f"Nova qtd - {ativo} (atual: {qtd_atual_val:.0f})",
                    min_value=-100000,
                    value=int(qtd_atual_val)
                )
                novas_quantidades[ativo] = nova_qtd
            
            # Botão de aplicar
            aplicar = st.form_submit_button("Aplicar quantidades")
        
        if aplicar:
            # Aplica as novas quantidades a cada ativo
            for atv, qtd in novas_quantidades.items():
                df_filtered.loc[df_filtered['Ativo'] == atv, 'Quantidade'] = qtd

            # Recalcula colunas dependentes (ex.: Juros projetados, DIV1_ATIVO)
            df_filtered['Juros projetados'] = (
                df_filtered['Fluxo descontado (R$)'] * df_filtered['Quantidade']
            )
            df_filtered['DIV1_ATIVO'] = (
                df_filtered['Juros projetados'] * 0.0001 *
                (df_filtered['Prazos (dias úteis)']/252)
            )

            st.success("Novas quantidades aplicadas aos ativos selecionados!")


    
    # --- MUDANÇA AQUI: GRÁFICO COM GGPlot (plotnine) ---
    st.write("## Relação Juros Projetados vs Ano/Semestre")
    if selected_filters:
        from plotnine import (
        ggplot, aes, geom_col, labs, theme, element_text, 
        element_rect, scale_fill_brewer,scale_y_continuous,scale_fill_manual,geom_text,position_dodge
         )
        # Agregar por Ano e Semestre a soma de "Juros projetados" e dividir por 1000
        df_plot = (
            df_filtered
            .groupby(["Ano", "Semestre"], as_index=False)
            ["Juros projetados"]
            .sum()
            .assign(**{"Juros projetados (R$ mil)": lambda x: x["Juros projetados"] / 1000})
        )
        df_plot['Ano'] = df_plot['Ano'].astype(int)
        # Montar o bar plot com valores em milhares
        p = (
            ggplot(df_plot, aes(x='factor(Ano)', y='Juros projetados (R$ mil)', fill='Semestre'))
            + geom_col(position='dodge')          
            + scale_fill_manual(values=['#1F4E79', '#A5C8E1'])  # Substitua por cores desejadas
            + labs(
                title="Juros + Amortização Projetados por Ano e Semestre",
                x="Ano",
                y="Soma dos Juros Projetados (R$ mil)"  # Label ajustado
            )
            + theme(
                figure_size=(10, 4),
                axis_text_x=element_text(rotation=45, ha='right'),
                panel_background=element_rect(fill='white'),
                plot_background=element_rect(fill='white'),
                plot_title=element_text(margin={'b': 20})  # Aumenta a distância entre o título e o gráfico (margin de 20)

            )
        )
        st.pyplot(p.draw(), use_container_width=True)
        st.write("---")
        col1, col2, col3 = st.columns([4.9, 0.2, 4.9])
        with col1:
            df_div = df_filtered
            # 1) Agrupe e some os valores para cada DAP
            df_summarized = df_div.groupby('DAP', as_index=False)['DIV1_ATIVO'].sum()
            df_summarized = df_summarized.merge(df_div1, on='DAP', how='left')
            df_summarized.rename(columns={'DV01': 'DV01_DAP'}, inplace=True)
            df_summarized['CONTRATOS'] = df_summarized['DIV1_ATIVO'] / df_summarized['DV01_DAP']
            # 2) Crie uma coluna para o texto formatado
            df_summarized['DIV1_format'] = df_summarized['DIV1_ATIVO'].apply(lambda x: f"{x:,.0f}")

            # 3) Faça o gráfico usando o DataFrame resumido
            p = (
                ggplot(df_summarized, aes(x='DAP', y='DIV1_ATIVO'))
                + geom_col(fill='#1F4E79')
                + geom_text(
                    aes(label='DIV1_format'),
                    stat='identity',
                    va='bottom',
                    nudge_y=50,  # Ajuste conforme necessário
                    size=8
                )
                + labs(
                    title="DIV1 vs DAP",
                    x="DAP",
                    y="DIV1(R$)"
                )
                + theme(
                    figure_size=(6, 4),
                    axis_text_x=element_text(rotation=45, ha='right'),
                    panel_background=element_rect(fill='white'),
                    plot_background=element_rect(fill='white')
                )
            )

            st.pyplot(p.draw(), use_container_width=True)
        with col2:
                # Adicionar linha vertical
                st.html(
                    '''
                            <div class="divider-vertical-lines"></div>
                            <style>
                                .divider-vertical-lines {
                                    border-left: 2px solid rgba(49, 51, 63, 0.2);
                                    height: 60vh;
                                    margin: auto;
                                }
                                @media (max-width: 768px) {
                                    .divider-vertical-lines {
                                        display: none;
                                    }
                                }
                            </style>
                            '''
                )

        with col3:
            df_div = df_filtered[['DIV1_ATIVO', 'DAP']]
            #Agrupar por DAP e somar os valores de DIV1
            df_div = df_div.groupby('DAP', as_index=False).sum()
            df_div.set_index('DAP', inplace=True)
            df_div.loc['Total'] = df_div.sum()
            #Adicionar coluna com o texto formatado
            df_div['DIV1_ATIVO'] = df_div['DIV1_ATIVO'].apply(lambda x: f"{x:,.0f}")
            
            #Criar sum row
            #AJUSTAR SUM ROW
            

            st.table(df_div)

        
        df_div = df_filtered

        # 1) Agrupe e some os valores para cada DAP
        df_summarized = df_div.groupby('DAP', as_index=False)['DIV1_ATIVO'].sum()
        df_summarized = df_summarized.merge(df_div1, on='DAP', how='left')
        #trocar todos para Contratos
        df_summarized.rename(columns={'DV01': 'DV01_DAP'}, inplace=True)
        df_summarized['CONTRATOS'] = df_summarized['DIV1_ATIVO'] / df_summarized['DV01_DAP']
        # 2) Crie uma coluna para o texto formatado
        df_summarized['DIV1_format'] = df_summarized['DIV1_ATIVO'].apply(lambda x: f"{x:,.2f}")
        #Criar sum row 
        #AJUSTAR SUM ROW
        #df_summarized.loc['Total'] = df_summarized.sum()
        df_summarized2 = df_summarized[['DAP','CONTRATOS']]
        st.write("---")
        
        df_summarized['CONTRATOS_FORMAT'] = df_summarized['CONTRATOS'].apply(lambda x: f"{x:,.0f}")
        # 3) Faça o gráfico usando o DataFrame resumido
        p = (
            ggplot(df_summarized, aes(x='DAP', y='CONTRATOS'))
            + geom_col(fill='#1F4E79')
            + geom_text(
                aes(label='CONTRATOS_FORMAT'),
                stat='identity',
                va='bottom',
                size=8
            )
            + labs(
                title="CONTRATOS DAP",
                x="DAP",
                y="CONTRATOS"
            )
            + theme(
                figure_size=(16, 6),
                axis_text_x=element_text(rotation=45, ha='right'),
                panel_background=element_rect(fill='white'),
                plot_background=element_rect(fill='white')
            )
        )

        st.pyplot(p.draw(), use_container_width=True)

        st.write("---")
        df_summarized2.set_index('DAP', inplace=True)
        df_summarized2['CONTRATOS'] = df_summarized2['CONTRATOS'].apply(lambda x: f"{x:,.0f}")
        st.table(df_summarized2) 
        #Salvar o df_summarized2 para um arquivo excel
        #Substituir o nome da coluna 'CONTRATOS' para os nomes dos Fundos filtrados - lista_fundos em string
        if testar_nova_qtd:
            fundo = ''
            for fundos in lista_fundos:
                if fundo == '':
                    fundo = fundos

            ## 1) Garante que DAP seja índice numérico / string -- depende da sua base
            # 2) NÃO formate para string antes de agregar!
            #    (formatação faz sentido só na hora de mostrar)
            df_new = df_summarized2.rename(columns={"CONTRATOS": fundo})
            #Transformar string para float
            df_new[fundo] = df_new[fundo].astype(float)


            # 3) Cria o DataFrame master (persistente na sessão) se ainda não existir
            # 1) Inicializa acumulador, se ainda não existir
            if "df_total" not in st.session_state:
                st.session_state.df_total = pd.DataFrame()

            # 2) Se o usuário quer gerar uma nova posição...
            if testar_nova_qtd and lista_fundos:

                fundo = lista_fundos[0]                 # só 1 fundo por vez
                df_new = df_summarized2.rename(columns={"CONTRATOS": fundo}).astype(float)

                # 2.1) Congela a pré-visualização para esta run
                st.session_state["df_preview"] = df_new

                st.subheader("Pré-visualização (não salvo ainda)")
                st.table(df_new.style.format("{:,.0f}"))


                # 2.2) Callback a ser executado DEPOIS do clique
                def salvar_posicao():
                    df_preview = st.session_state["df_preview"]
                    fundo_local = df_preview.columns[0]

                    if fundo_local in st.session_state.df_total.columns:
                        st.session_state["msg"] = ("warn",
                            f"Já existe a coluna “{fundo_local}”. Use “Limpar posições” antes de salvar de novo.")
                        return

                    st.session_state.df_total = (
                        st.session_state.df_total.add(df_preview, fill_value=0)
                    )
                    st.session_state["msg"] = ("ok",
                        f"Posição do {fundo_local} salva/atualizada!")


                # 2.3) Botão que dispara o callback
                st.button(f"Salvar posição de “{fundo}”", on_click=salvar_posicao)


            # 3) Mensagens pós-salvar
            if "msg" in st.session_state:
                tipo, texto = st.session_state["msg"]
                (st.success if tipo == "ok" else st.warning)(texto)
                del st.session_state["msg"]                # limpa para não repetir


            # 4) Mostra acumulado + download (se houver algo)
            if not st.session_state.df_total.empty:
                st.subheader("Posições acumuladas")
                st.table(st.session_state.df_total.style.format("{:,.0f}"))

                def to_excel_bytes(df: pd.DataFrame) -> bytes:
                    buf = io.BytesIO()
                    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                        df.to_excel(writer, index=True, sheet_name="Posições")
                    return buf.getvalue()

                st.download_button(
                    "Baixar Excel",
                    data=to_excel_bytes(st.session_state.df_total),
                    file_name="posicoes_por_fundo.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )


            # 5) Botão para zerar tudo
            if st.button("Limpar posições"):
                st.session_state.df_total = pd.DataFrame()
                st.success("Posições zeradas.")
                #Colocar uma checkbox para exivir o df_fitlered

        exibir = st.sidebar.checkbox("Mostrar DataFrame filtrado", value=False, key="show_df")
        if exibir:
            st.write("## Base Para os Calculos da Página:")
            st.dataframe(df_filtered)


    # Tabela de dados
#    st.write("## Dados Filtrados")
#    if selected_filters:
#        st.dataframe(df_filtered[selected_filters])

    # CSS personalizado
    add_custom_css()


def add_custom_css():
    # CSS personalizado
    st.markdown(
        """
        <style>

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
        </style>
        """,
        unsafe_allow_html=True,
    )

    # JavaScript para aplicar zoom de 80%
    components.html(
        """
        <script>
            document.body.style.zoom = "80%";
        </script>
        """,
        height=0,
    )


if __name__ == "__main__":
    main()
