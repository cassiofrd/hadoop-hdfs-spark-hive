#queremos calcular a média de amigos por idade

#como em toda a aplicação spark, começamos importando SparkConf e SparkContext

from pyspark import SparkConf, SparkContext

#agora vamos criar o SparkConf e com base nele gerar o SparkContext
#vamos escolher um nome para a aplicação

conf=SparkConf().setMaster("local").setAppName("FriendsByAge")
sc=SparkContext(conf=conf)

#agora vamos importar a base de dados fakefriends que apresenta quatro colunas
#id,nome,idade,número de amigos

lines=sc.textFile("endereço no arquivo, seja local ou no hdfs")

#o nosso separa dor de variáveis é a vírgula, pois temos quatro variáveis por linha
#vamos então separar as variáveis

fields=line.split(',')

#agora vamos transformar em rdd para poder manipular a base de dados
#vamos aplicar a função parseLine, que criamos abaixo, em todos os termos de nosso
#rdd, tem como insumo o arquivo lines, que é nossa base de dados
#como vemos, vamos ficar apenas com as duas linhas que nos interessam

defparseLine(line):
	fields=line.split(',')
	age=int(fields[2])
	numFriends=int(fields[3])
	return(age,numFriends)

#agora sim vamos criar o rdd (que nada mais é que um esquema chave-valor), ou seja
#quando dizemos rdd na verdade estamos nos referindo a um dicionário
#temos que chegar a esta unidade para a partir desta fazer manipulações

rdd=lines.map(parseLine)

#agora vamos criar um comando grande que vai nos dar exatamente o resultado que
#queremos
#vamos analisar a função abaixo parte por parte

totalsByAge=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

#mapValues deixa a parte key, idade, inalterada e cria uma tupla que contém o
#número de amigos, x, seguido do valor 1
#já reduceByKey vai pegar essa tupla e somar, entre as pessoas da mesma idade,
#as posições x com x e y com y de cada tupla, de modo que teremos como resultado
#final um conjunto de tuplas que informa o total de amigos e o número de pessoas
#com determinada idade (pega o primeiro elemento de cada tupla e soma, pega o
#segundo elemento de cada tupla e soma)

averagesByAge=totalsByAge.mapValues(sambda x:x[0]/x[1])

#agora, só falta fazer o print dos resultados

results=averagesByAge.collect()
for result in results:
	print(result)