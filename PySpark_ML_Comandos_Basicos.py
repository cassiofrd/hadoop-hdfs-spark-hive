from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("ML1")
sc=SparkContext(conf=conf)

#abaixo o comando para importar dados em csv
#inferSchema=True seleciona a opção na qual o spark infere automaticamente
#qual é o tipo de cada dado, você não vai precisar especificar manualmente
df=spark.read.csv('sample_data.csv',inferSchema=True,header=True)

#para saber o número de linhas e colunas da nossa base, fazemos
print((df.count),len(df.columns))

#para saber o nome das colunas e seus respectivos tipos, basta fazer
df.printSchema()

#para mostrar parte da base no formato tabela, basta fazer
df.show(k) #em que k são as k primeiras linhas que se quer mostrar

#caso queiramos apenas algumas colunas em específico, basta fazer
df.select('age','mobile').show(5)

#para algumas estatísticas descritivas, fazemos
df.describe().show()

#podemos acrescentar uma coluna a nossa base utilizando o método withColumn
df=df.withColumn("age_after_10_yrs",(df["age"]+10))

#para mudar o tipo de uma variável basta utilzar cast
from pyspark.sql.types import StringType,DoubleType

#o comando abaixo cria uma nova coluna em que age é uma variável DoubleType
df=df.withColumn('age_double',df['age'].cast(DoubleType())).show(10,False)

#para consultar a base utilizando algum tipo de filtro, basta utilizar filter
df.filter(df['mobile']=='Vivo').show()

#para selecionar apenas algumas variáveis a partir da base filtrada, basta
#utilizar a variável select
df.filter(df['mobile']=='Vivo').select('age','ratings','mobile').show()

#podemos, é claro, também aplicar mais de um filtro
df.filter(df['mobile']=='Vivo').filter(df['experience']>10).select('age','ratings','mobile').show()

#para ver quais são os valores distintos presentes em uma coluna use distinct()
df.select('mobile').distinct().show()

#para saber quantos são os valores distintos, utilizar count()
df.select('mobile').distinct().count()

#agrupando dados, abaixo, contamos o número de observações por grupos
df.groupBy('mobile').count().orderBy('count',ascending=False).show(5,False)

#abaixo, temos a média dos valores das variáveis por grupos
df.groupBy('mobile').mean().show(5,False)

#abaixo, temos a soma de todas as observações agrupadas por grupos
df.groupBy('mobile').sum().show(5,False)

#para filtrar os maiores e menores valores em um grupo, utilizamos max e min
df.groupBy('mobile').max().show(5,False)
df.groupBy('mobile').min().show(5,False)

#podemos fazer algumas transformações pré-determinadas na base de dados
#utilizando dois tipos de UDFs: UDF e Pandas UDF, vamos ver as duas
#muito interessante, pois utilizando isto com a função lambda podemos
#fazer as variáveis dummies
from pyspark.sql.functions import udf
age_udf=udf(lambda age: "young" if age<=30 else "senior", StringType())
df=df.withColumn("age_group",age_udf(df.age)).show(10,False)

#########################manipulando as colunas da base de dados
#Pandas UDF (Vectorized UDF)
#abaixo, criamos uma variável de com anos restantes de vida subtraindo
#de 100 (expectativa de vida) da idade atual da pessoa
from pyspark.sql.functions import pandas_udf
def remaining_yrs(age):
	yrs_left=(100-age)
       return yrs_left
length_udf=pandas_udf(remaining_yrs,IntegerType())
#abaixo, aplicamos a transformação e criamos a variável
df=df.withColumn("yrs_left",length_udf(df['age'])).show(10,False)
#também é possível pegar mais de uma variável como insumo para criar uma
#outra variável
def prod(rating,exp):
	x=ratomg*exp
	return x
prod_udf=pandas_udf(prod,DoubleType())
df=df.withColumn("product",prod_udf(df['ratings'],df['experience'])).show(10,False)

#agora vamos ver o comando para eliminar os valores duplicados
df=df.dropDuplicates()
#para deletar uma coluna, utilizamos
df_new=df.drop('mobile')
df_new.show()
