import pandas as pd
import os
import datetime
import difflib
import numpy as np

def adicionar_log(informacoes, log_file):
    df_log = pd.DataFrame(informacoes,
                          columns=['Data', 'Arquivo', 'Marca', 'Status', 'Linhas Oportunidades', 'Linhas QW'])
    if os.path.exists(log_file):
        df_log_antigo = pd.read_excel(log_file)
        df_log = pd.concat([df_log_antigo, df_log], ignore_index=True)
    df_log.to_excel(log_file, index=False)

def formatar_moeda_brasileira(valor):
    valor_float = float(valor)
    return f'R$ {valor_float:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

def get_latest_file2(directory):
    files = [f for f in os.listdir(directory) 
             if f.endswith('.XLSX') and not f.startswith('~$') and f.startswith('QuerySap')]
    if not files:
        raise FileNotFoundError(f"No Excel files found in {directory}")
    files = [os.path.join(directory, f) for f in files]
    latest_file = max(files, key=os.path.getmtime)
    print(f'Lendo {latest_file}')
    return latest_file

def get_latest_file(directory):
    files = [f for f in os.listdir(directory)
             if f.endswith('.xlsb') and not f.startswith('~$')]
    if not files:
        raise FileNotFoundError(f"No Excel files found in {directory}")
    files = [os.path.join(directory, f) for f in files]
    latest_file = max(files, key=os.path.getmtime)
    print(f'Lendo {latest_file}')
    return latest_file

def inverter_data(data):
    if pd.isna(data):
        return np.nan
    
    if isinstance(data, str):
        if '/' in data:
            dia, mes, ano = data.split('/')
            return ano + mes + dia
        elif len(data) >= 10 and data[4] == '-' and data[7] == '-':
            return data[:10].replace('-', '')  # Converte 'yyyy-mm-dd' para 'yyyymmdd'
    
    return np.nan

resutado_dir = r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\Resultado"
log_file = r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\log_execucoes.xlsx"
valores_indesejados = ['', 'N/A', '0', 'Sem correlação', 'nan']

data_atual = datetime.datetime.now()
data_formatada = data_atual.strftime("%d-%m-%Y")
data_hoje = pd.to_datetime(datetime.datetime.now().date()).strftime('%d/%m/%Y')
data_hoje = int(inverter_data(data_hoje))

ano_atual = data_atual.year
mes_atual = data_atual.strftime(f"%B_{ano_atual}")
mes_atual_pt = mes_atual.replace('January', 'Janeiro').replace('February', 'Fevereiro').replace('March', 'Março') \
    .replace('April', 'Abril').replace('May', 'Maio').replace('June', 'Junho').replace('July', 'Julho') \
    .replace('August', 'Agosto').replace('September', 'Setembro').replace('October', 'Outubro') \
    .replace('November', 'Novembro').replace('December', 'Dezembro')

query_sap_dir = fr"\\grupofleury\Diretoria de Estrategia Inovacao\Dir de AN e Dados\Pricing, Prod e IC\Pública\Produtos\Relatórios\Query - Extrações de bases\Query portfólio {ano_atual}\03. Março"
caminho_arq = '\\\\grupofleury\\Auditoria\\Auditoria_Interna\\Auditoria_Continua\\Rogerio.Mello\\TRABALHOS REALIZADOS\\2 - [ADITIVOS] Leitura de PDFs e Imagens, Comparativos, Criação de Interface\\2.3 - Vef. exame disp\\BASES\\'
produtos_dir = '\\\\grupofleury\\Diretoria de Estrategia Inovacao\\Dir de AN e Dados\\Pricing, Prod e IC\\Pública\\Produtos\\Relatórios\\Portfólio e Preços'
caminho_tabela_BI = rf'\\grupofleury\Diretoria Comercial\Projeto automação\Extração Diária - Qlik\{data_formatada}.xlsx'
Query_sap_real_dir = fr'\\grupofleury\Dir_Exec_Suporte_Operacoes\GCC - Query SAP\2025\03 Março _2025'

Query_sap_real = get_latest_file2(Query_sap_real_dir)
produtos_file = get_latest_file(produtos_dir)
query_sap_file = get_latest_file(query_sap_dir)
###### LER DATAFRAMES
tabela_bi = pd.read_excel(caminho_tabela_BI, keep_default_na=False, na_values=[])
df_sap = pd.read_excel(Query_sap_real, usecols=['Nome da sigla de contrato', 'Descrição Convênio', 'Descrição Plano', 'Tabela de Preços', 'Validade do plano até', 'Válido até', 'Descrição Empresa', 'Capitation'], keep_default_na=False, na_values=[])
df = pd.read_excel(produtos_file,
                   usecols=['SIGLA', 'Nomenclatura', 'Tipo', 'Seção Processante', 'Cód. TUSS', 'Descrição TUSS',
                            'Seção Comercial', 'Classificação Produtos', 'Rol ANS',
                            'Agrupamento\n(Cadastro de Produtos)', 'Composição\nProdutos'], header=3, keep_default_na=False, na_values=[], engine='pyxlsb')
df2 = pd.read_excel(query_sap_file, usecols=['SIGLA', 'STATUS', 'STATUS_LIBERACAO', 'MARCA', 'DIVISÃO_DE_NEGÓCIO'],
                    engine='pyxlsb', keep_default_na=False, na_values=[])
###### TRATAMENTO QUERY_SAP
df_sap[['Validade do plano até', 'Válido até']] = (df_sap[['Validade do plano até', 'Válido até']].fillna('').astype(str))
df_sap['Validade do plano até'] = df_sap['Validade do plano até'].apply(inverter_data)
df_sap['Válido até'] = df_sap['Válido até'].apply(inverter_data)
df_sap[['Validade do plano até', 'Válido até']] = df_sap[['Validade do plano até', 'Válido até']].fillna(0).astype(int)
df_sap = df_sap[df_sap['Validade do plano até'] > data_hoje]
df_sap = df_sap[df_sap['Válido até'] > data_hoje]
df_sap = df_sap[df_sap['Capitation'] != 'S']
df_sap_pesquisa = df_sap
df_sap = df_sap.drop_duplicates(['Tabela de Preços', 'Descrição Convênio'])           ################
df_sap['Descrição Convênio'] = df_sap['Descrição Convênio'].str.strip()
nomes_coluna = df_sap['Descrição Convênio'].unique().tolist()
nomes_coluna = df_sap['Descrição Convênio'].astype(str).unique().tolist()

###### TRATAMENTO TABELA BI
tabela_bi['SIGLA_PRODUTO'] = tabela_bi['SIGLA_PRODUTO'].astype(str)
tabela_bi['CONVENIO'] = tabela_bi['CONVENIO'].str.strip()

###### CARREGAR PEDIDOS
pedidos = pd.read_csv(r'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\Demanda planejamento.del',encoding='latin-1' , sep='|', quotechar='"')
pedidos = pedidos.astype(str)

lista_filtros_pedido = []

#df_pedido_extra = pd.read_excel(rf'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\teste.xlsx')    #Tabela extra
#tabelas = df_pedido_extra['Tabela de Preços'].unique().tolist()   #Tabela extra
#for tabela_preco in tabelas:   #Tabela extra
#    lista_filtros_pedido.append(tabela_preco)   #Tabela extra
#    print(tabela_preco)   #Tabela extra

for index, row in pedidos.iterrows():
    if 'Oportunidade' not in row['Opção']:
        continue
    if row['Descrição_Convênio'] == 'nan' and row['Marca'] != 'nan':
        if row['Marca'] != 'Fleury':
            filtro_pedido = df_sap[df_sap['Descrição Convênio'].str.startswith(row['Marca'], na=False)]
            tabelas = filtro_pedido['Tabela de Preços'].unique().tolist()
            for tabela_preco in tabelas:
                lista_filtros_pedido.append(tabela_preco)
                print(tabela_preco)
    elif row['Descrição_Convênio'] != 'nan':
        descricoes_convênio = row['Descrição_Convênio'].split('\\')
        nomes_proximos_pedido = []
        for descricao in descricoes_convênio:
            descricao = descricao.strip()
            nomes_parecidos = difflib.get_close_matches(descricao, nomes_coluna, n=3)
            print(nomes_parecidos)
            if nomes_parecidos:
                nome_escolhido = nomes_parecidos[0]
                nome_escolhido = nome_escolhido.strip()
                filtro_pedido = df_sap[df_sap['Descrição Convênio'] == nome_escolhido]
                tabelas = filtro_pedido['Tabela de Preços'].unique().tolist()
                for tabela_preco in tabelas:
                    lista_filtros_pedido.append(tabela_preco)
                    print(tabela_preco)
    else:
        print("Nenhuma correspondência encontrada.")
        continue

dataframes = []
for string_tabela in lista_filtros_pedido:
    operadora = tabela_bi[tabela_bi['TABELA_PRECO'] == string_tabela]
    dataframes.append(operadora)

oportunidades_lista = []
# Processar todos os arquivos na pasta da operadora
for df_operadora in dataframes:
    if df_operadora.empty:  # Verifica se o DataFrame não está vazio
        print("Este DataFrame está vazio e será ignorado.")
        continue
    else:
        df_operadora.info()
        primeira_linha = df_operadora['DIVISAO_NEGOCIO'].iloc[0]
        primeira_linha2 = df_operadora['TABELA_PRECO'].iloc[0]
        empresa = df_operadora['EMPRESA'].iloc[0]
        if primeira_linha == 'GRANDE SAO PAULO' or primeira_linha == 'BRASILIA' or primeira_linha == 'CAMPINAS':
            df2_divisao_e_marca = df2[(df2['DIVISÃO_DE_NEGÓCIO'] == 'GRANDE SAO PAULO') | 
                          (df2['DIVISÃO_DE_NEGÓCIO'] == "BRASILIA") | 
                          (df2['DIVISÃO_DE_NEGÓCIO'] == "CAMPINAS")]
        else: 
            df2_divisao_e_marca = df2[(df2['DIVISÃO_DE_NEGÓCIO'] == primeira_linha)]

        marcas_selecionadas = ['teste']
        #marcas_selecionadas = selecionar_marcas()

        for marca in marcas_selecionadas:
            marca = df2_divisao_e_marca['MARCA'].iloc[0]
            #print(f"\n\nProcessando arquivo: {df_operadora}, Marca: {marca}")

            # Filtragem e processamento
            status_desejado = 'Ativo'
            status_desejado2 = 'Liberado'

            df2_filtrado = df2_divisao_e_marca[
                (df2_divisao_e_marca['STATUS'] == status_desejado) &
                (df2_divisao_e_marca['STATUS_LIBERACAO'] == status_desejado2)
            ]

            df2_filtrado = df2_filtrado.drop_duplicates(subset='SIGLA', keep='first')

            sigla_query = df2_filtrado['SIGLA']
            df_iguais = pd.merge(sigla_query, df, on='SIGLA')

            df_operadora = df_operadora.rename(columns={'SIGLA_PRODUTO': 'SIGLA', 'COD_TUSS': 'Cód. TUSS'})

            merged_df = pd.merge(df_iguais, df_operadora, on='SIGLA', how='outer', indicator=True)
            pre_oportunidades = merged_df[merged_df['_merge'] == 'left_only']
            pre_oportunidades = pre_oportunidades.rename(columns={'Cód. TUSS_x': 'Cód. TUSS'})
            pre_oportunidades = pre_oportunidades.drop(columns=['_merge', 'Cód. TUSS_y', 'QTD_CH', 'QTD_M2', 'PORTE', 'AUTORIZACAO', 'VALOR_TOTAL_M2', 'SECAO_PROCESSANTE'])
            pre_oportunidades['Cód. TUSS'] = pre_oportunidades['Cód. TUSS'].astype(str).str.strip()
            df_operadora['Cód. TUSS'] = df_operadora['Cód. TUSS'].astype(str).str.strip()
            qw = pd.merge(pre_oportunidades, df_operadora, on='Cód. TUSS')
            qw = qw.rename(columns={'Cód. TUSS_x': 'Cód. TUSS', 'SIGLA_x': 'SIGLA'})
            qw = qw.drop(columns=['SIGLA_y'])
            qw.drop_duplicates(subset=['SIGLA', 'Cód. TUSS', 'QTD_CH'], keep='first', inplace=True)

            merged_df = pd.merge(pre_oportunidades, df_operadora, on='Cód. TUSS', how='outer', indicator=True)
            oportunidades = merged_df[merged_df['_merge'] == 'left_only']
            oportunidades = oportunidades.rename(columns={'Cód. TUSS_x': 'Cód. TUSS', 'SIGLA_x': 'SIGLA'})
            oportunidades = oportunidades.drop(columns=['_merge', 'SIGLA_y'])

            oportunidades['Cód. TUSS'] = oportunidades['Cód. TUSS'].replace(valores_indesejados, pd.NA)
            oportunidades = oportunidades[oportunidades['Cód. TUSS'].notna()]

            qw = qw[qw['Descrição TUSS'].notna() & (qw['Descrição TUSS'] != '')]
            oportunidades = oportunidades[['Descrição TUSS', 'SIGLA', 'Seção Comercial', 'Seção Processante', 'Cód. TUSS', 'Classificação Produtos', 'Tipo', 'Rol ANS', 'Agrupamento\n(Cadastro de Produtos)', 'Composição\nProdutos']]
            qw = qw[['Descrição TUSS', 'SIGLA', 'Seção Comercial', 'Seção Processante', 'Cód. TUSS', 'Classificação Produtos', 'Tipo', 'Rol ANS', 'QTD_CH', 'Agrupamento\n(Cadastro de Produtos)', 'Composição\nProdutos', 'QTD_M2', 'PORTE', 'AUTORIZACAO', 'VALOR_TOTAL_M2', 'SECAO_PROCESSANTE']]

            output_file = f'{resutado_dir}/{marca}/Oportunidades e QW ({primeira_linha2} - {empresa}).xlsx'
            if not os.path.exists(f'{resutado_dir}/{marca}'):
                os.makedirs(f'{resutado_dir}/{marca}')
            print(output_file)
            # concatenar oportunidades e qw
            oportunidades = pd.concat([oportunidades, qw], ignore_index=True)
            oportunidades['Tabela de Preço'] = primeira_linha2
            oportunidades['Empresa'] = empresa
            oportunidades = oportunidades[(oportunidades['Tipo'] != 'Exame CD') & (oportunidades['Tipo'] != 'Consulta')]
            with pd.ExcelWriter(output_file) as writer:
                oportunidades.to_excel(writer, sheet_name='Oportunidades e QW', index=False)
                #qw.to_excel(writer, sheet_name='QW', index=False)
            oportunidades_lista.append(oportunidades)
            

            num_linhas_oportunidades = oportunidades['SIGLA'].count()
            num_linhas_qw = qw['SIGLA'].count()

            log_info = {
                'Data': data_atual.strftime("%Y-%m-%d %H:%M:%S"),
                'Arquivo': primeira_linha2,
                'Marca': marca,
                'Status': 'Concluído',
                'Linhas Oportunidades': num_linhas_oportunidades,
                'Linhas QW': num_linhas_qw
            }
            adicionar_log([log_info], log_file)
            print(f'Processamento concluído para o arquivo: {primeira_linha2}, Marca: {marca}')
    qw["QTD_CH"] = qw["QTD_CH"].astype(float)
    qw["VALOR_TOTAL_M2"] = qw["VALOR_TOTAL_M2"].astype(float)
    df_operadora["Preço Novo"] = df_operadora["QTD_CH"] + df_operadora["VALOR_TOTAL_M2"]
    qw["Preço Novo"] = qw["QTD_CH"] + qw["VALOR_TOTAL_M2"]
    df_operadora["Cob."] = "SIM"
    qw["Cob."] = "SIM"
    df_operadora["Co-Pagto (R$)"] = ""
    df_operadora["Tabela de Dominio"] = ""
    df_operadora["Seção Comercial"] = ""
    df_operadora["Dolar"] = ""
    aditivo = df_operadora[["NOME_PRODUTO", "SIGLA", "Cód. TUSS", "Preço Novo", "Cob.", "AUTORIZACAO"]]
    aditivo_qw = qw[['Descrição TUSS', 'SIGLA', 'Cód. TUSS', "Preço Novo" , 'Cob.', 'AUTORIZACAO']]
    aditivo_qw = aditivo_qw.rename(columns={"Descrição TUSS": "NOME_PRODUTO"})
    aditivo_final = pd.concat([aditivo, aditivo_qw])
    aditivo_final = aditivo_final.rename(columns={
    "NOME_PRODUTO": "Nome do Exame",
    "SIGLA": "Sigla",
    "Cód. TUSS": "Código",
    "AUTORIZACAO": "Aut."
    })
    query_sap_pesquisa2 = df_sap_pesquisa[df_sap_pesquisa['Tabela de Preços'] == str(primeira_linha2).strip()]
    cep = query_sap_pesquisa2[["Descrição Convênio", "Descrição Empresa", "Descrição Plano", "Nome da sigla de contrato", "Tabela de Preços"]]
    csv = df_operadora[["SIGLA", "NOME_PRODUTO", "Cód. TUSS", "QTD_M2", "QTD_CH", "Dolar", "Co-Pagto (R$)", "Tabela de Dominio", "SECAO_COMERCIAL", "PORTE", "SECAO_PROCESSANTE"]]
    csv = csv.rename(columns={
    "SIGLA": "Sigla",
    "NOME_PRODUTO": "Descrição",
    "Cód. TUSS": "Código",
    "QTD_M2": "M2",
    "QTD_CH": "Preco (R$)",
    "PORTE": "Porte (R$)",
    "SECAO_COMERCIAL": "Seção Comercial"
    })
    qw["Co-Pagto (R$)"] = ""
    qw["Tabela de Dominio"] = ""
    qw["Dolar"] = ""
    csv_qw = qw[["SIGLA", "Descrição TUSS", "Cód. TUSS", "QTD_M2", "QTD_CH", "Dolar", "Co-Pagto (R$)", "Tabela de Dominio", "Seção Comercial", "PORTE", "SECAO_PROCESSANTE"]]
    csv_qw = csv_qw.rename(columns={
    "SIGLA": "Sigla",
    "Descrição TUSS": "Descrição",
    "Cód. TUSS": "Código",
    "QTD_M2": "M2",
    "QTD_CH": "Preco (R$)",
    "PORTE": "Porte (R$)"
    })
    csv_final = pd.concat([csv, csv_qw])
    duplicados = csv_final[csv_final.duplicated(subset='Sigla', keep=False)] ##VERF
    #csv_final = csv_final[csv_final['SECAO_PROCESSANTE'] != '-']       #ALGUM FILTRO DA SEÇÃO PROCESSANTE
    #csv_final = csv_final[~((csv_final['Sigla'].isin(duplicados['Sigla'])) & (csv_final['SECAO_PROCESSANTE'] != '-'))]
    nome_saida_adit = f'{resutado_dir}/{marca}/{marca} - {primeira_linha2} - {empresa} Aditivo.xlsx'
    nome_saida_csv = f'{resutado_dir}/{marca}/{marca} - {primeira_linha2} - {empresa} CSV.xlsx'
    #with pd.ExcelWriter(nome_saida_adit) as writer:
    #    aditivo_final.to_excel(writer, sheet_name='ADIT', index=False)
    #    cep.to_excel(writer, sheet_name='CEP', index=False)
    #with pd.ExcelWriter(nome_saida_csv) as writer:
    #    csv_final.to_excel(writer, sheet_name='CSV', index=False)
    #    cep.to_excel(writer, sheet_name='CEP', index=False)

#concatenando oportunidades_lista
oportunidades_final = pd.concat(oportunidades_lista, ignore_index=True)
output_file = f'{resutado_dir}/Oportunidades e QW ({mes_atual_pt}).xlsx'
with pd.ExcelWriter(output_file) as writer:
    oportunidades_final.to_excel(writer, sheet_name='Oportunidades e QW', index=False)