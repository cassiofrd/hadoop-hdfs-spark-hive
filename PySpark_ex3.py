#vamos contar o número de ocorrências de uma palavra utilizando flatmap()

#map transforma cada elemento de um rdd em um elemento novo
#já flatMap pega como insumo um rdd com um determinado número
#de entradas e transforma em um rdd com um número maior de entradas
#abaixo,vamos importar uma classe que vamos precisar para limpar os
#dados (colocar tudo em minúsculo e retirar a pontuação)

from pyspark import SparkConf, SparkContext
import re

#abaixo temos a função que vamos aplicar em todas as linhas usando flatMap()
#na primeira parte do código nós re.compile() nós eliminamos todo tipo de
#pontuação, todo o que não faz parte das palavras
#na parte split(text.lower()) separamos por espaço em branco e transformamos
#todas as letras em minúsculas

def normalizeWords(text):
	return re.compile(r'\W+',re.UNICODE).split(text.lower())

conf=SparkConf().setMaster("local").setAppName("WordCount")
sc=SparkContext(conf=conf)

#vamos utilizar um livro aleatório em txt

input=sc.textFile("endereço de um livro aleatório")

#abaixo, utilizando o flatMap() separar nossas unidades de análise, que
#eram as linhas para palavras, utilizando como critério os espaços em branco
#vemos que criamos tuplas com muitas entradas

words=input.flatMap(normalizeWords)

#no comando abaixo, tranformamos as palavras em tuplas com a palavra e o número
#1, depois usamos reduceByKey para somar as tuplas, encontrando o número de
#ocorrências de determinada palavra

wordCounts=sords.map(lambda x: (x,1)).reduceByKey(lambada x,y: x+y)

#agora, por último, vamos modificar a ordem de chave e valor para que o número
#de vezes apareça primeiro na lista e então ordenar pe número, que é a nova chave

wordCountsSorted=wordCounts.map(lambda x: (x[1],x[0])).sortByKey()

#agora vsmos coletar os resultados

result=wordCountsSorted.collect()

#agora vamos printar os resultados

for result in results:
	count=str(result[0])
	word=result[1].encode('ascii','ignore')
	if (word):
		print(word.decode()+":\t\t"+count)