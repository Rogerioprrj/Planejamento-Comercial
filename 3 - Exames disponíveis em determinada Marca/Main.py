import pandas as pd
import os
import datetime
import difflib

def get_latest_file(directory):
    files = [f for f in os.listdir(directory)
             if f.endswith('.xlsb') and not f.startswith('~$')]
    if not files:
        raise FileNotFoundError(f"No Excel files found in {directory}")
    files = [os.path.join(directory, f) for f in files]
    latest_file = max(files, key=os.path.getmtime)
    print(f'Lendo {latest_file}')
    return latest_file

data_atual = datetime.datetime.now()
ano_atual = data_atual.year
mes_atual = data_atual.strftime("%m. %B")  # Formato "09. Setembro"
mes_atual_pt = mes_atual.replace('January', 'Janeiro').replace('February', 'Fevereiro').replace('March', 'Março') \
    .replace('April', 'Abril').replace('May', 'Maio').replace('June', 'Junho').replace('July', 'Julho') \
    .replace('August', 'Agosto').replace('September', 'Setembro').replace('October', 'Outubro') \
    .replace('November', 'Novembro').replace('December', 'Dezembro')
status_desejado = 'Ativo'
status_desejado2 = 'Liberado'
mes_atual_pt = '11. Novembro'
query_portfolio = fr"\\grupofleury\Diretoria de Estrategia Inovacao\Dir de AN e Dados\Pricing, Prod e IC\Pública\Produtos\Relatórios\Query - Extrações de bases\Query portfólio {ano_atual}"
query_portfolio = get_latest_file(query_portfolio)
produtos_dir = r'\\grupofleury\Diretoria de Estrategia Inovacao\Dir de AN e Dados\Pricing, Prod e IC\Pública\Produtos\Relatórios\Portfólio e Preços'
produtos_file = get_latest_file(produtos_dir)


df = pd.read_excel(produtos_file, header=3, engine='pyxlsb')
df2 = pd.read_excel(query_portfolio) #Planilha Portfolio
Marcas = pd.read_excel(rf'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\TRABALHOS REALIZADOS\2 - [ADITIVOS] Leitura de PDFs e Imagens, Comparativos, Criação de Interface\2.3 - Vef. exame disp\Marcas.xlsx')
##### CARREGAR PEDIDOS

nomes_coluna = Marcas['MARCA'].astype(str).unique().tolist()
pedidos = pd.read_csv(r'\\grupofleury\Auditoria\Auditoria_Interna\Auditoria_Continua\Rogerio.Mello\Demanda planejamento exames.del',encoding='latin-1' , sep='|', quotechar='"')
pedidos = pedidos.astype(str)
todos_pedidos = []

for index, row in pedidos.iterrows():
    coluna_filtro = row['Código'].replace('CBHPM', 'CBHPM\n')
    marca = row['Marca']
    nomes_parecidos = difflib.get_close_matches(marca, nomes_coluna, n=3)
    if nomes_parecidos:
        nome_escolhido = nomes_parecidos[0]
        print(nome_escolhido)
    if coluna_filtro not in df.columns:
        colunas_com_codigo = [col for col in df.columns if col.lower().startswith("cód")]
        print("Colunas que começam com 'cód':", colunas_com_codigo)
        print(f"Coluna {coluna_filtro} não encontrada no arquivo.")
    else:
        print(f"Valores únicos na coluna {coluna_filtro}:")
        print(df[coluna_filtro].unique())
        df[coluna_filtro] = df[coluna_filtro].astype(str).str.strip()
    
        valores_indesejados = ['', 'N/A', '0', 'Sem correlação', 'nan']
        df[coluna_filtro] = df[coluna_filtro].replace(valores_indesejados, pd.NA)
        df_filtrado = df[df[coluna_filtro].notna()]
    
        # Passo 4: Retornar a coluna SIGLA
        if 'SIGLA' in df.columns:

            sigla_coluna = df_filtrado[['SIGLA']].drop_duplicates('SIGLA')
        else:
            print("Coluna SIGLA não encontrada no arquivo.")

        df2_filtrado = df2[
            (df2['STATUS'] == status_desejado) &
            (df2['STATUS_LIBERACAO'] == status_desejado2) &
            (df2['MARCA'] == marca)
        ]
        df_iguais = pd.merge(sigla_coluna, df2_filtrado, on='SIGLA')
        
        df_iguais.to_excel('\\\\grupofleury\\Diretoria Comercial\\Equipe - Planejamento Comercial\\Projeto automação\\Exames disponíveis - Marca e Codificação\\' + row['Código'] + ' - ' + row['Marca'] + '.xlsx' , index=False)