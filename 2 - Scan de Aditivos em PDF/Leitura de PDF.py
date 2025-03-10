import tabula

lista_tabelas = tabula.read_pdf("C:\\Users\\rogerio.mello\\Desktop\\Atualizar dados por aqui\\rghr\\Agendamento_344.pdf", pages="11")
print(len(lista_tabelas))

for tabela in lista_tabelas:
	display(tabela)
