from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("RecommernderSystem")
sc=SparkContext(conf=conf)

#vamos agora baixar nossa base de dados
df=spark.read.csv('movie_ratings_df.csv',inferSchema=True,header=True)
print((df.count(),len(df.columns)))

#vamos converter a coluna de títulos dos filmes de categórica para numérica
#na coluna title_new, temos uma coluna numérica, em que cada número
#representa um filme
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer,IndexToString
stringIndexer=StringIndexer(inputCol="title",outputCol="title_new")
model=stringIndexer.fit(df)
indexed=model.transform(df)

#temos que repetir o mesmo método na coluna user_id, que também é uma
#variável categórica
stringIndexer=StringIndexer(inputCol="user_id",outputCol="userid")
model=stringIndexer.fit(df)
indexed=model.transform(df)

#agora vamos separar a base entre treino e teste
train,test=indexed.randomSplit([0.75,0.25])

#vamos a agora fazer e treinar o modelo de recomendação
from pyspark.ml.recommendation import ALS
rec=ALS(maxIter=10,regParam=0.01,userCol='userID',itemCol='title_new',ratingCol='ratings',nonnegative=True,coldStartStrategy="drop")
rec_model=rec.fit(train)

#agora vamos prever e analisar os resultados preditivos do modelo
predicted_ratings=rec_model.transform(test)
predicted_ratings.printSchema()
predicted_ratings=rec_model.transform(test)
predicted_ratings.orderBy(rand()).show(10)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator=RegressionEvaluator(metricName='rmse',predictionCol='prediction',labelCol='rating')
rmse=evaluator.evaluate(predictions)
print(rmse)

#por fim, vamos recomendar os filmes que os usuários iriam gostar
unique_movies=indexed.select('title_new').distinct()
unique_movies.count() #nos dá o total de filmes no dataframe
#podemos selecionar qualquer um dos unsuários identificar quais filmes são
#recomendáveis para ele, no caso, vamos tentar o user_id=85
a=unique_movies.alias('a')
watched_movies=indexed.filter(indexed['userId']=='user_id').select('title_new').distinct()
watched_movies.count() #total de filmes que o usuário já assistiu
b=watched_movies.alias('b')
total_movies=a.join(b,a.title_new==b.title_new,how='left')
total_movies.show(10,False)
remaining_movies=total_movies.where(col("b.title_new").isNull()).select(a.title_new).distinct()
remaining_movies.count()
remaining_movies=remaining_movies.withColumn("userId",lit(int(user_id)))
remaining_movies.show(10,False) #estes são os resultados

#agora, vamos selecionar apenas os resultados com maior probabilidade de
#serem relevantes
recommendations=rec_model.transform(remaining_movies).orderBy('prediction',ascending=False)
recommendations.show(5,False)

#podemos ver também os resultados com os títulos dos filmes
movie_title=IndexToString(inputCol="title_new",outputCol="title",labels=model.labels)
final_recommendations=movie_title.transform(recommendations)
final_recommendations.show(10,False)

