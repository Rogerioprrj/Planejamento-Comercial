import pandas as pd
import os
import datetime
import difflib
from google.cloud import bigquery
from google.oauth2 import service_account
import numpy as np
import time
import json

caminho_credenciais = r'C:\Users\rogerio.mello\Documents\Repositorio\credenciais.json'

# Abrir e carregar o arquivo JSON
with open(caminho_credenciais, 'r') as f:
    credenciais_json = json.load(f)

credentials = service_account.Credentials.from_service_account_info(credenciais_json)
client = bigquery.Client(credentials=credentials, project=credenciais_json["project_id"])
print('Conexão ao GCP realizada com sucesso!')

def inverter_data(data):
    # Verifica se o dado é NaN
    if pd.isna(data):
        return np.nan  # Retorna NaN se o valor for ausente
    
    # Verifica se o dado é uma string
    if isinstance(data, str):
        # Tenta dividir a data no formato dd/mm/yyyy
        if '/' in data:
            dia, mes, ano = data.split('/')
            return ano + mes + dia  # Retorna no formato yyyymmdd
        # Verifica se está no formato de datetime ou similar
        elif len(data) >= 10 and data[4] == '-' and data[7] == '-':
            return data[:10].replace('-', '')  # Converte 'yyyy-mm-dd' para 'yyyymmdd'
    
    return np.nan  # Retorna NaN para entradas que não são válidas


data_atual = datetime.datetime.now()
data_hoje = pd.to_datetime(datetime.datetime.now().date()).strftime('%d/%m/%Y')
data_hoje = int(inverter_data(data_hoje))

ano_atual = data_atual.year
mes_atual = data_atual.strftime(f"%B_{ano_atual}")
mes_atual_pt = mes_atual.replace('January', 'Janeiro').replace('February', 'Fevereiro').replace('March', 'Março') \
    .replace('April', 'Abril').replace('May', 'Maio').replace('June', 'Junho').replace('July', 'Julho') \
    .replace('August', 'Agosto').replace('September', 'Setembro').replace('October', 'Outubro') \
    .replace('November', 'Novembro').replace('December', 'Dezembro')

valores_indesejados = ['', 'N/A', '0', 'Sem correlação', 'nan']  # valores indesejados na tuss
marcas = [
    'FLEURY', 'CPMA', 'IR', 'amais - PI', 'BIOCLINICO', 'amais - SP', 'Felippe Mattoso',
    'amais - PR', 'Marcelo Magalhães', 'Labs amais', 'Weinmann', 'CAMPANA', 'Serdil',
    'Novamed', 'Novamed - RS', 'Novamed - PR', 'Novamed - RJ', 'amais - RS', 'amais - DF',
    'DIAGMAX', 'LAFE', 'SOMMOS DNA', 'LAFE HUB', 'INLAB', 'amais - BA', 'amais - PE',
    'PRETTI', 'CPC', 'SANTECORP'
]
resutado_dir = r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\Resultado"
log_file = r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\log_execucoes.xlsx"


def selecionar_marcas():
    print("Selecione uma ou mais marcas (digite os números separados por vírgula):")
    for i, marca in enumerate(marcas, 1):
        print(f"{i}. {marca}")

    # Solicitar a seleção do usuário
    opcoes = input("Digite os números das marcas desejadas: ").split(',')
    opcoes = [int(opcao.strip()) for opcao in opcoes]  # Converte para inteiros e remove espaços

    # Obter as marcas selecionadas
    marcas_selecionadas = [marcas[opcao - 1] for opcao in opcoes]
    return marcas_selecionadas


def get_latest_file(directory):
    files = [f for f in os.listdir(directory)
             if f.endswith('.xlsb') and not f.startswith('~$')]
    if not files:
        raise FileNotFoundError(f"No Excel files found in {directory}")
    files = [os.path.join(directory, f) for f in files]
    latest_file = max(files, key=os.path.getmtime)
    print(f'Lendo {latest_file}')
    return latest_file


def get_latest_file2(directory):
    files = [f for f in os.listdir(directory)
             if f.endswith('.XLSX') and not f.startswith('~$') and f.startswith('Query')]
    if not files:
        raise FileNotFoundError(f"No Excel files found in {directory}")
    files = [os.path.join(directory, f) for f in files]
    latest_file = max(files, key=os.path.getmtime)
    print(f'Lendo {latest_file}')
    return latest_file


def adicionar_log(informacoes, log_file):
    df_log = pd.DataFrame(informacoes,
                          columns=['Data', 'Arquivo', 'Marca', 'Status', 'Linhas Oportunidades', 'Linhas QW'])
    if os.path.exists(log_file):
        df_log_antigo = pd.read_excel(log_file)
        df_log = pd.concat([df_log_antigo, df_log], ignore_index=True)
    df_log.to_excel(log_file, index=False)

try:
    cubo = pd.read_parquet(r'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\TRABALHOS REALIZADOS\2 - [ADITIVOS] Leitura de PDFs e Imagens, Comparativos, Criação de Interface\cubo.parquet')
except:
    cubo = pd.read_excel(r'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\TRABALHOS REALIZADOS\2 - [ADITIVOS] Leitura de PDFs e Imagens, Comparativos, Criação de Interface\cubo.xlsx', header=6)
    cubo.to_parquet(r'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\TRABALHOS REALIZADOS\2 - [ADITIVOS] Leitura de PDFs e Imagens, Comparativos, Criação de Interface\cubo.parquet', index=False)

# Caminhos das pastas #resolver xlsb
query_sap_dir = f"\\\\grupofleury\\Diretoria de Estrategia Inovacao\\Dir de AN e Dados\\Pricing, Prod e IC\\Pública\\Produtos\\Relatórios\\Query - Extrações de bases\\Query portfólio {ano_atual}\\02. Fevereiro"
produtos_dir = '\\\\grupofleury\\Diretoria de Estrategia Inovacao\\Dir de AN e Dados\\Pricing, Prod e IC\\Pública\\Produtos\\Relatórios\\Portfólio e Preços'
#Query_sap_real_dir = fr'\\grupofleury\Dir_Exec_Suporte_Operacoes\GCC - Query SAP\2025\03 {mes_atual_pt}'
Query_sap_real_dir = fr'\\grupofleury\Dir_Exec_Suporte_Operacoes\GCC - Query SAP\2025\03 Março _2025'

# Encontrar os arquivos mais recentes em cada pasta
query_sap_file = get_latest_file(query_sap_dir)
produtos_file = get_latest_file(produtos_dir)
Query_sap_real = get_latest_file2(Query_sap_real_dir)

# Carregar os arquivos Excel mais recentes

Query_sap = pd.read_excel(Query_sap_real,
                          usecols=['Descrição Convênio', 'Descrição Empresa', 'Nome da sigla de contrato', 'Descrição Plano', 'Tabela de Preços', 'Validade do plano até',
                                   'Válido até', 'Capitation'], sheet_name='QUERY SAP')
#Query_sap = Query_sap.rename(columns={'Tabela de preços': 'Tabela de Preços'})
Query_sap[['Validade do plano até', 'Válido até']] = (Query_sap[['Validade do plano até', 'Válido até']].fillna('').astype(str))
Query_sap['Validade do plano até'] = Query_sap['Validade do plano até'].apply(inverter_data)
Query_sap['Válido até'] = Query_sap['Válido até'].apply(inverter_data)
Query_sap[['Validade do plano até', 'Válido até']] = Query_sap[['Validade do plano até', 'Válido até']].fillna(0).astype(int)
Query_sap = Query_sap[Query_sap['Validade do plano até'] > data_hoje]
Query_sap = Query_sap[Query_sap['Válido até'] > data_hoje]
Query_sap = Query_sap[Query_sap['Capitation'] != 'S']
query_sap_pesquisa = Query_sap
Query_sap['Descrição Convênio2'] = Query_sap['Descrição Convênio'].str.strip().str.lower()
nomes_coluna = Query_sap['Descrição Convênio2'].unique().tolist()     ###################
dataframes = []
pedidos = pd.read_csv(r'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\Demanda planejamento.del',encoding='latin-1' , sep='|', quotechar='"')
pedidos = pedidos.astype(str)
pedidos.drop_duplicates(inplace=True)
todas_descricoes = []
todas_tbprecos = []
todas_empresas = []
todos_convenios = []
# Parte 1: Loop pelos pedidos
pedidos['Descrição_Convênio'] = pedidos['Descrição_Convênio'].str.strip().str.lower()
for index, row in pedidos.iterrows():
    # Row ['Descrição']
    print('CARREGANDO PEDIDO: ', row['Descrição_Convênio'])
    lista_filtros_pedido = []
    if 'Oportunidade' not in row['Opção']:
        #if row['Descrição_Convênio'] == 'nan' and row['Marca'] != 'nan':
        #    if row['Marca'] != 'Fleury':
        #        filtro_pedido = Query_sap[Query_sap['Descrição Convênio'].str.startswith(row['Marca'], na=False)]
        #        lista_filtros_pedido.append(filtro_pedido)
        #        print(filtro_pedido)

        if row['Descrição_Convênio'] != 'nan':
            descricoes_convênio = row['Descrição_Convênio'].split('\\')
            for descricao in descricoes_convênio:
                print(f"descrição_convenio pedido: {descricao}")
                nomes_parecidos = difflib.get_close_matches(descricao, nomes_coluna, n=3)
                if nomes_parecidos:
                    print(f"Nome encontrado: {nomes_parecidos}")
                    nome_escolhido = nomes_parecidos[0]
                    nome_escolhido = nome_escolhido.strip()
                    filtro_pedido = Query_sap[Query_sap['Descrição Convênio2'] == nome_escolhido]
                    lista_filtros_pedido.append(filtro_pedido)
        else:
            print("Nenhuma correspondência encontrada.")
            continue
    if 'Oportunidade' in row['Opção']:
        if row['Descrição_Convênio'] == 'nan' and row['Marca'] != 'nan':   #FAZER PARA TODA UMA MARCA
            if row['Marca'] != 'Fleury':
                filtro_pedido = Query_sap[Query_sap['Descrição Convênio'].str.startswith(row['Marca'], na=False)]
                lista_filtros_pedido.append(filtro_pedido)
        elif row['Descrição_Convênio'] != 'nan':                           #FAZER PARA UMA DESCRIÇÃO CONVÊNIO
            descricoes_convênio = row['Descrição_Convênio'].split('\\')
            for descricao in descricoes_convênio:
                print(f"descrição_convenio pedido: {descricao}")
                nomes_parecidos = difflib.get_close_matches(descricao, nomes_coluna, n=3)
                if nomes_parecidos:
                    print(f"Nome encontrado: {nomes_parecidos}")
                    nome_escolhido = nomes_parecidos[0]
                    nome_escolhido = nome_escolhido.strip()
                    filtro_pedido = Query_sap[Query_sap['Descrição Convênio2'] == nome_escolhido]
                    lista_filtros_pedido.append(filtro_pedido)
    #df_pedido_extra = pd.read_excel(rf'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\teste.xlsx')
    #lista_filtros_pedido.append(df_pedido_extra)
    print(lista_filtros_pedido)
    try:
        filtro = pd.concat(lista_filtros_pedido, ignore_index=True)
    except:
        filtro = lista_filtros_pedido[0]
    tabelas_de_preco = filtro.drop_duplicates(['Tabela de Preços'])
    tabelas_de_preco = tabelas_de_preco['Tabela de Preços']
    filtro = filtro[filtro['Tabela de Preços'].isin(tabelas_de_preco)]
    filtro.to_excel(r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\Resultado\teste2.xlsx")
    filtro = pd.merge(filtro, cubo[['SIGLA CONTRATO', 'PLANO', 'VL TOTAL EXAMES']], how='left', left_on=['Nome da sigla de contrato', 'Descrição Plano'], right_on=['SIGLA CONTRATO', 'PLANO'])
    filtro['VL TOTAL EXAMES'] = filtro['VL TOTAL EXAMES'].fillna(0)
    filtro = filtro.sort_values(by='VL TOTAL EXAMES', ascending=False).drop_duplicates(subset=['Tabela de Preços'])
    #filtro = filtro.sort_values(by='VL TOTAL EXAMES', ascending=False).drop_duplicates(subset=['Tabela de Preços', 'Descrição Convênio'])
    print('filtro')
    print(filtro)
    #filtro = filtro.drop_duplicates(['Tabela de Preços', 'Descrição Empresa', 'Descrição Plano'])
    filtro.to_excel(r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\Resultado\teste.xlsx")
    relacionamento = [
        (convenio, empresa, descricao, tbpreco) for convenio, empresa, descricao, tbpreco in zip(filtro['Descrição Convênio'].tolist(), filtro['Descrição Empresa'].tolist(), filtro['Descrição Plano'].tolist(), filtro['Tabela de Preços'].tolist()) 
        if tbpreco != 0
    ]
    print(fr'relacionamento {relacionamento}')
    for convenio, empresa, descricao, tbpreco in relacionamento:
        todas_descricoes.append(descricao)
        todas_tbprecos.append(tbpreco)
        todas_empresas.append(empresa)
        todos_convenios.append(convenio)

        todas_descricao_conv = [row['Descrição_Convênio']] * len(relacionamento)
        todas_marcas = [row['Marca']] * len(relacionamento)

# Parte 2: Criar a query única
if todas_descricoes and todas_tbprecos:  # Verifica se há descrições e tabelas de preços()
    descricoes_unicas = "', '".join([descricao.replace("'", "''") for descricao in set(todas_descricoes)])
    tbprecos_unicos = "', '".join([str(tbpreco) for tbpreco in set(todas_tbprecos)])
    empresas_unicas = "', '".join([empresa.replace("'", "''") for empresa in set(todas_empresas)])
    convenios_unicos = "', '".join([convenio.replace("'", "''") for convenio in set(todos_convenios)])
    print(descricoes_unicas)
    print(tbprecos_unicos)
    print(empresas_unicas)
    print(convenios_unicos)

    print(zip(todas_descricoes, todas_tbprecos, todas_empresas, todos_convenios))
    print(todas_descricoes)
    print(todas_tbprecos)
    print(todas_empresas)
    print(todos_convenios)
    query = f"""
    SELECT * 
    FROM `data-lake-prd-276215.rz.BI_RZTB_TABELAS_PRECO_FINAL_PRICING`
    WHERE TABELA_PRECO IN ('{tbprecos_unicos}')
    AND PLANO IN ('{descricoes_unicas}')
    AND EMPRESA IN ('{empresas_unicas}')
    AND CONVENIO IN ('{convenios_unicos}')
    ORDER BY SIGLA_PRODUTO, COD_TUSS ASC
    """
    print("query")
    requisicao = client.query(query)
    print("to dataframe")
    df5 = requisicao.result().to_dataframe()
    #df5['PLANO'] = df5['PLANO'].str.strip()

    #df5.to_excel(r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\Resultado\tabelaBI.xlsx")
    #df5 = pd.read_excel(r"C:\Users\rogerio.mello\Desktop\Oportunidade QW\MM.xlsx")

    print(len(todas_descricoes))
    print(len(todas_tbprecos))
    for descricao_plano, tbpreco, empresa, convenio in zip(todas_descricoes, todas_tbprecos, todas_empresas, todos_convenios):
        filtered_df = df5[(df5['PLANO'] == descricao_plano) & (df5['TABELA_PRECO'] == str(tbpreco)) & (df5['EMPRESA'] == empresa) & (df5['CONVENIO'] == convenio)]
        #filtered_df = df5[(df5['PLANO'].str.startswith(descricao_plano)) & (df5['TABELA_PRECO'].str.startswith(str(tbpreco)))]
        filtered_df = filtered_df.reset_index(drop=True)
        dataframes.append(filtered_df)
else:
    print("Nenhuma descrição ou tabela de preço encontrada.")
data_formatada = data_atual.strftime("%d-%m-%Y")
caminho_arquivo = rf"\\grupofleury\Diretoria Comercial\Projeto automação\Extração Diária - Qlik\{data_formatada}.xlsx"
try:
    final_df = pd.concat(dataframes, ignore_index=True)
except:
    final_df = dataframes[0]
final_df = final_df[[col for col in final_df.columns if col != 'ID'] + ['ID']]
final_df.to_excel(caminho_arquivo, index=False)