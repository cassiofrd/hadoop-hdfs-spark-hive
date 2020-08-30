#queremos filtrar os dados de temperatura para obter a 
#temperatura mínima em cada estação do ano

#a lista são dados separados por virgula com informações sobre
#estação do ano,data,informação sobre o tempo (que pode ser
#temperatura máxima, mínima ou precipitação) e temperatura

from pyspark import SparkConf, SparkContext

conf=SparkConf().setMaster("local").setAppName("TemperaturaMin")
sc=SparkContext(conf=conf)

#vamos fazer uma função que separe os dados que interessam na base
#e coloque no formato que queremos
def parseLine(line):
	fields = line.split(',')
	stationID=fields[0]
	entryType=fields[2]
	temperatura=float(fields[3])*0.1*(9.0/5.0)+32.0
	return (stationID,entryType,temperature)

#agora vamos aplicar a função em cada uma das linhas da base utilizando
#das unidades do arquivo, que são as linhas
lines=sc.textFile("endereço do arquivo")
parsedLines=lines.map(parseLine)

#utilizando a função filter, vamos filtrar apenas os dados com informações
#sobre as temperaturas mínimas
minTemps=parsedLines.filter(lambda x: "TMIN" in x[1])

#vamos agora criar tuplas apenas com as duas variáveis que interessam (estação
#e temperatura mínimas) para aplicar o reduceByKey
stationTemps=minTemps.map(lambda x:(x[0],x[2]))

#utilizando dentro de reduceByKey a função lambda com min(), vamos comparar
#todas as tuplas que tem a mesma estação duas a duas e ficar apenas com a
#menor temperatura, de forma que encontremos a temperatura mínima
#obs: é possível ajustar o código para encontrar a temperatura máxima ao invés
#da mínima apenas trocando a função min() por max()
minTemps=stationTemps.reduceByKey(lambda x,y:min(x,y))

#agora vamos utilizar o .collect() colocar os dados em formato de lista e
#então iterar sobre a lista para printar os resultados
results=minTemps.collect();

for result in results:
	print(result[0]+"\t{:.2f}F".format(result[1]))