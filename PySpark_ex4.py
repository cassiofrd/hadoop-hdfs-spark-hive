#vamos agora encontrar o quanto cada consumidor gastou

from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("GastoTotal")
sc=SparkContext(conf=conf)

#nossa base de dados é composta por linhas em que, separadas por vírgula, temos
#informação sobre id do clinete, id do priduto e o dinheiro gasto no produto

lines=sc.textFile("hdfs:/bases/Base_ex4.txt")

#vamos agora criar a função que vai me permiter limpar os dados, ficando apenas
#com o que me interessante e colocando no formato de tupla, para que eu poça
#manipular

#IMPORTANTÍSSIMO: o termo da função é a unidade de análise que está dentro
#do arquivo com os dados (lines), no caso cada linha, por isso devemos
#colocar termo na função que vai representar esta unidade que vai sofrer
#a transformação split, pois o arquivo como o todo não a sofre

def clean_data(line):
	x=line.split(',')
	return (int(x[0]),float(x[2]))

#abaixo vamos aplicar a função acima à nossa base

dados=lines.map(clean_data)

#agora vamos criar a função que vai possibilitar somarmos tudo pelo id do cliente

soma_por_id=dados.reduceByKey(lambda x,y: x+y)

#agora vamos ordenar os resultado do que gasta menos para o que gasta mais
#para usar o sortByKey devemos inverter a ordem de id e gato total, pois
#o nosso key é o id e nós queremos ordenar pelo value, ou seja, temos que
#inverter

inverso=soma_por_id.map(lambda x: (x[1],x[0]))
results=inverso.sortByKey()

#agora temos que coletar e printar os resultados

result=results.collect()

for x in result:
	print(x)
