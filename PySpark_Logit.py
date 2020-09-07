from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("Logit")
sc=SparkContext(conf=conf)

#vamos primeiro importar os dados
df=spark.read.csv('Log_Reg_dataset.csv',inferSchema=True,header=True)
print((df.count(),len(df.columns)))
df.printSchema()
df.show(5)
df.describe().show()
df.groupBy('Country').count().show()
df.groupBy('Search_Engine').count().show()
df.groupBy('Status').count().show()
df.groupBy('Country').mean().show()
df.groupBy('Search_Engine').mean().show()
df.groupBy(Status).mean().show()

#agora vamos fazer uma regressão
#primeiro vamos importar a classe para gerar as variáveis dummies
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
#vamos transformar a variávesl Search_Engine que é categórica com três valores
#em uma variável numérica (com valores 0,1 e 2), que é Search_Engine_Num
search_engine_indexer=StringIndexer(inputCol='Search_Engine',outputCol='Search_Engine_num').fit(df)
df=search_engine_indexer.transform(df)
df.show(3,False)
df.groupBy('Search_Engine').count().orderBy('count',ascending=False).show(5,False)
df.groupBy('Search_Engine_Num').count().orderBy('count',ascending=False).show(5,False)

#agora vamos transformar o vetor que varia entre 0,1,2 em três vetores
#numéricos, três dummies que variam entre zero e 1 e serão utilizadas
#no modelo, o Search_Engine_Vector será o nosso objeto com os três vetores entre 0 e 1
#o vetor é um pouco diferente, ele tem três entradas que representam
#o número de vetores (o total de categorias menos 1, pois no modelo
#temos que ter uma categoria base, o valor na variável original que é
#referente à dummy, e o valor 1)
from pyspark.ml.feature import OneHotEncoder
search_engine_encoder=OneHotEncoder(inputCol="Search_Engine_Num",outputCol="Search_Engine_Vector")
search_engine_encoder.transform(df)
df.show(3,False)

#vamos fazer algo semelhante com a variável country
country_indexer=StringIndexer(inputCol="Country",outputCol="Coutry_Num").fit(df)
df=country_indexer.tranform(df)
contry_encoder=OneHotEncoder(inputCol="Country_Num",outputCol="Country_Vector")
df=country_encoder.transform(df)

#agora que todas as variáveis categóricas foram convertidas em numéricas,
#temos que colocar todas as colunas em um único vetor que irá funcionar
#como o input feature para o modelo. então, selecionamos as variáveis
#que utilizaremos como independentes e nomeamos o vetor resultado como features
df_assembler=VectorAssembler(inputCols=['Search_Engine_Vector','Country_Vector','Age','Repeat_Visitor','Web_pages_viewed'],outputCol="features")
df=df_assembler.transform(df)
df.printSchema()

#agora temos uma coluna extra, features, que é a combinação de todas as
#variáveis independentes representadas em um único vetor
#agora, basta selecionar a coluna features como input e Status como output
#para treinar nossa regressão logística
model_df=df.select(['features','Status'])

#dividindo entre base de treino e base de teste, temos
training_df,test_df=model_df.randomSplit([0.75,0.25])

#agora, vamos regredir nosso modelo logit
from pyspark.ml.classification import LogisticRegression
log_reg=LogisticRegression(labelCol='Status').fit(training_df)

#vejamos os resultados
train_results=log_reg.evaluate(training_df).predictions
train_results.filter(train_results['Status']==1).filter(train_results['predictions']==1).select(['Status','prediction','probability']).show(10,False)

#agora vamos avaliar o modelo com base nos dados de teste
results=log_reg.evaluate(test_df).predictions
results.printSchema()
results.select(['Status','prediction']).show(10,False)

#para ver a confusion matrix, no caso do logit, temos que calcular caso a caso
tp=results[(results.Status==1) & (results.prediction==1)].count()
tp=results[(results.Status==0) & (results.prediction==0)].count()
tp=results[(results.Status==0) & (results.prediction==1)].count()
tp=results[(results.Status==1) & (results.prediction==0)].count()
