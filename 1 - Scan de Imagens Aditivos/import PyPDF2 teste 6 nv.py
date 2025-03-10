import PyPDF2
import os
import re
import pandas as pd
import openpyxl

convenios = {"amil"}                    # Convenios ativos
c_extração = "C://Users//rogerio.mello//Documents//arquivos pyton//"         # Caminho para extrair o pdf
c_salvar_txt = "C:\\Users\\rogerio.mello\\Documents\\arquivos pyton\\"       # Caminho para salvar o txt
codigos = ['40103307', '41301137']
arquivo_entrada = 'C:\\Users\\rogerio.mello\\Documents\\arquivos pyton\\Contrato Diario unimed.txt'
arquivo_saida = 'C:\\Users\\rogerio.mello\\Documents\\arquivos pyton\\saida.txt'

def processar_linhaSeq8_e_2_valores(linha):
    # Verificar quantos valores "R$" a linha possui
    valores_reais = re.findall(r"R\$ \d{1,3}(?:\.\d{3})*,\d{2}", linha)
    if re.match(r"^\d{8}", linha) and "R$" in linha:
        if len(valores_reais) >= 2:
            # Padrão para linhas com dois ou mais valores em reais
            pattern = r"^(.*?R\$ \d{1,3}(?:\.\d{3})*,\d{2})(.*?R\$ \d{1,3}(?:\.\d{3})*,\d{2})"
            match = re.match(pattern, linha)
            if match:
                return match.group(1) + match.group(2)
        elif len(valores_reais) == 1:
            # Padrão para linhas com apenas um valor em reais
            pattern = r"^(.*?R\$ \d{1,3}(?:\.\d{3})*,\d{2})"
            match = re.match(pattern, linha)
            if match:
                return match.group(1)
        return linha

def encontrar_pdfs(c_extração):                                      # Encontrar os pdfs na pasta
    arquivos_pdf = []
    for arquivos in os.walk(c_extração):
        for lista in arquivos:
            for nomes in lista:
                if nomes.endswith('.pdf'):
                    arquivos_pdf.append(nomes)
    return arquivos_pdf

def extrair_o_texto_do_pdf(pdf_file: str) -> [str]:                  # Extrair texto do pdf
    with open(pdf_file, 'rb') as pdf:
        reader = PyPDF2.PdfReader(pdf)
        texto_pdf = []

        for page in reader.pages:
            linhas = page.extract_text()
            texto_pdf.append(linhas)
        return texto_pdf

def extrair(convenios, c_extração, c_salvar_txt):           # Enviar o texto para o txt
    pdfs = encontrar_pdfs(c_extração)
    for pdf in pdfs:
        texto_extraido = extrair_o_texto_do_pdf(c_extração + str(pdf))
        convenio = encontrar_convenios(convenios, texto_extraido)
        with open(c_salvar_txt + "Contrato Diario "+ str(convenio) + ".txt", "w", encoding='utf-8') as txt:
            for text in texto_extraido:
                if text is not None:                                 # Verifica se o texto extraído não é None
                    txt.write(text)
    return texto_extraido

def encontrar_convenios(convenios, texto_extraido):                  # Vai verificar qual convenio está no texto para colocar no txt
    for i in convenios:
        for texto in texto_extraido:
            t = texto.lower()
            if i in t:
                return i

def fixar_linhas_cortadas(file_path, convenios):
    for convenio in convenios:
        with open((c_extração + "Contrato Diario " + convenio + ".txt"), 'r', encoding='utf-8') as file:
            lines = file.readlines()
        fixed_lines = []
        buffer = ""
        for line in lines:
            # Se a linha começa com um número, significa que é o início de um novo registro
            if (line.strip()[:8].isdigit() or re.match(r'\d{8,}', line.strip()) or re.match(r'\d{2}\.\d{2}\.\d{4}', line.strip())):
                # Se houver algo no buffer, adicione ao resultado
                if buffer:
                    fixed_lines.append(buffer.strip())
                    buffer = ""
                buffer = line.strip()
            else:
                buffer += " " + line.strip()
    
    # Adiciona a última linha processada
        if buffer:
            fixed_lines.append(buffer.strip())

        with open((c_extração + "Contrato Diario " + convenio + ".txt"), 'w', encoding='utf-8') as file:
            file.write("\n".join(fixed_lines))

def extrair_codigos(c_extração, convenios):
    codigos = []
    for convenio in convenios:
        with open(c_extração + "Contrato Diario " + convenio + ".txt", 'r', encoding='utf-8') as file:
            lines = file.readlines()
        for line in lines:
            if (line.strip()[:8].isdigit() or re.match(r'\d{8,}', line.strip()) or re.match(r'\d{2}\.\d{2}\.\d{4}', line.strip())):
                codigos.append(line.strip())
    return codigos


def procurar_e_salvar_linhas(c_extracao, c_salvar_txt, convenios):               #FINAL DO PROCESSAMENTO TXT
    for convenio in convenios:
        if convenio == "unimed":
            with open(f"{c_extracao}Contrato Diario {convenio}.txt", 'r', encoding='utf-8') as arquivo_entrada:
                processed_lines = []
                for linha in arquivo_entrada:
                    processed_line = processar_linhaSeq8_e_2_valores(linha)
                    if processed_line:
                        processed_lines.append(processed_line)
    return processed_lines

def criar_planilha(processed_lines, caminho_arquivo_excel):
    dados = []
    for linha in processed_lines:
        partes = re.split(r"R\$ \d{1,3}(?:\.\d{3})*,\d{2}", linha)
        id_nome = partes[0].strip()
        valores_reais = re.findall(r"R\$ \d{1,3}(?:\.\d{3})*,\d{2}", linha)

        # Separar o ID e o nome
        id_match = re.match(r"^\d{8}", id_nome)
        if id_match:
            id = id_match.group(0)
            nome = id_nome[len(id):].strip()
        else:
            id = ""
            nome = id_nome

        if len(valores_reais) == 2:
            dados.append([id, nome, valores_reais[0], valores_reais[1]])
        elif len(valores_reais) == 1:
            dados.append([id, nome, valores_reais[0], ""])

    # Criar DataFrame
    df = pd.DataFrame(dados, columns=["Código TUSS", "Nome", "Primeiro Valor", "Segundo Valor"])

    # Salvar o DataFrame como um arquivo Excel
    df.to_excel(caminho_arquivo_excel, index=False)

    # Abrir o arquivo Excel e ajustar o tamanho das colunas
    wb = openpyxl.load_workbook(caminho_arquivo_excel)
    ws = wb.active
    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 60
    ws.column_dimensions['C'].width = 15
    ws.column_dimensions['D'].width = 15
    wb.save(caminho_arquivo_excel)

extrair(convenios, c_extração, c_salvar_txt)
fixar_linhas_cortadas(c_extração, convenios)
codigos = extrair_codigos(c_extração, convenios)
linhas_processadas = procurar_e_salvar_linhas(c_extração, c_salvar_txt, convenios)
criar_planilha(linhas_processadas, caminho_arquivo_excel)