#vamos encontrar qual é o filme mais popular

from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("FilmeMaisPopular")
sc=SparkContext(conf=conf)

#abaixo a base de dados
base=sc.textFile("file:/home/hadoop/Projetos_Spark/Base_ex5.txt")

#vamos separar os dados por espaço em branco e ficar apenas com a segunda
#coluna, que nos informa o id do filme

def clean_data(line):
	x=line.split()
	return (str(x[1]))

#abaixo vamos aplicar a função acima à nossa base, aplicando o código a
#cada linha de dados

dados=base.map(clean_data)

#agora vamos criar tuplas (id,1) com os valores obtidos e depois somar por 
#key o que nos dará o número de vezes que um filme foi selecionado
#vamos também ordenar os resultados

idfilme_1=dados.map(lambda x: (x,1))
resultados=idfilme_1.reduceByKey(lambda x,y: x+y)
resultados=resultados.sortByKey()

#agora, por fim, vamos extrair os resultados e dar o print
resultados=resultados.collect()
for resultado in resultados:
	print(resultado)
