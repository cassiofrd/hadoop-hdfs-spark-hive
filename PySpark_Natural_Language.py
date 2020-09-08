from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("NaturalLanguageProcessing")
sc=SparkContext(conf=conf)

#primeiramente vamos criar um dataframe que tem dados na forma de texto
df=spark.createDataFrme([(1,'I really liked this movie'),(2,'I would recommend this movie to my friends'),(3,'movie was alright but acting was horrible'),(4,'I am never wtching that movie ever again')],['user_id','review'])
df.show(4,False)

from pyspark.ml.feature import Tokenizer
tokenization=Tokenizer(inputCol='review',outputCol='tokens')
tokenized_df=tokenization.transform(df)
tokenized_df.show(4,False)

#vamos retirar as preposições, pois elas vão aumentar muito a complexidade
#do código e não vão dizer muito sobre o caráter do texto, conseguimos
#fazer isso com a função StopWordsRemover
from pyspark.ml.feature import StopWordsRemover
stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
refined_df=stopword_removal.transform(tokenized_df)
refined_df.select(['user_id','tokens','refined_tokens']).show(4,False)

#agora vamos representar nosso texto com meio de vetores numéricos
#cada coluna representa uma palavra, sendo que se a mensagem conta com
#a palavra, o valor na posição será 1, se não conta, será 0
#o vetor features abaixo informa (ver a primeira linha do plot): o tamanho
#do vetor é 11, contém valores 1 nas posições 0, 4 e 9
from pyspark.ml.feature import CountVectorizer
count_vec=CountVectorizer(inputCol='refined_tokens',outputCol='features')
cv_df=count_vec.fit(refined_df).transform(refined_df)
cv_df.select(['user)id','refined_tokens','features']).show(4,False)

#temos que obter o TF-IDF
#Term Frequency (TF): score baseado na frequência da palavra na linha
#Inverse Document Frequency (IDF): score baseado na frequência de linhas que tem a palavra considerada
from pyspark.ml.feature import HashingTF,IDF
hashing_vec=HashingTF(inputCol='refined_tokens',outputCol='tf_features')
hashing_df=hashing_vec.transform(refined_df)
hashing_df.select(['user_id','refined_tokens','rf_features']).show(4,False)
tf_idf_vec=IDF(inputCol='tf_features',outputCol='tf_idf_features')
tf_idf_df=tf_idf_vec.fit(hashing_df).transform(hashing_df)
tf_idf_df.select(['user_id','tf_idf_features']).show(4,False)

#agora vamos fazer a classificação das palavras utilizando Machine Learning
#vamos utilizar uma nova base de dados de reviw de filmes
text_df=spark.read.csv('Movie_reviews.csv',inferSchema=True,header=True,sep=',')
text_df.printSchema()
text_df=text_df.filter(((text_df.Sentiment=='1') | (text_df.Sentiment=='0')))

#vamos dar uma olhada na base de dados
from pyspark.sql.functions import rand
text_df.orderBy(rand()).show(10,False)

#no próximo passo, criamos uma nova coluna com valores do tipo inteiro
#e substituimos a coluna Sentiment, que apresenta valores
text_df=text_df.withColumn("Label",text_df.Sentiment.cast('float')).drop('Sentiment')
text_df.orderBy(rand()).show(10,False)

#vamos acrescentar também uma coluna que informa o tamanho de cada mensagem
#em número de caracteres
from pyspark.sql.functions import length
text_df=text_df.withColumn('length',length(text_df['Review']))
text_df.orderBy(rand()).show(10,False)
text_df.groupBy('Label').agg({'Length':'mean'}).show()

#vamos retirar as preopsições
from pyspark.ml.feature import StopWordsRemover
tokenization=Tokenizer(inputCol='Review',outputCol='tokens')
tokenized_df=tokenization.transform(text_df)
stopword_removal=StopWordsRemover(inputCol='tokens',outputCol='refined_tokens')
refined_text_df=stopword_removal.transform(tokenized_df)

#vamos criar a coluna token count que vai informar o número de tokens (termos) em cada linha
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
len_udf=udf(lambda s: len(s), IntegerType())
refined_text_df=refined_text_df.withColumn("token_count",len_udf(col('refined_tokens')))
refined_text_df.orderBy(rand()).show(10)

#agora vamos transformar o refined_tokens em vetores numéricos
count_vec=CountVectorizer(inputCol='refined_tokens',outputCol='features')
cv_text_df=count_vec.fit(refined_text_df).transform(refined_text_df)
cv_text_df.select(['refined_tokens','token_count','features','Label']).show(10)
model_text_df=cv_text_df.select(['features','token_count','Label'])

#vamos criar o vetor utilizado como insumo para o machine learning
from pyspark.ml.feature import VectorAssembler
df_assembler=VectorAssembler(inputCols=['features','token_count'],outputCol='features_vec')
model_text_df=df_assembler.transform(model_text_df)
model_text_df.printSchema()

#podemos utilizar qualquer modelo de classificação nesses dados
#no caso, vamos utilizar o modelo logit
#ou seja, com base nas palavras na mensagem, tentamos identificar qual foi
#a classificação feita por quem escreveu a mensagem
from pyspark.ml.classification import LogisticRegression
training_df,test_df=model_text_df.randomSplit([0.75,0.25])
log_reg=LogisticRegression(featuresCol='features_vec',labelCol='Label').fit(training_df)

#após treinar o modelo, vamos avaliar a performance do modelo em nossa
#base de dados
results=log_reg.evaluate(test_df).predictions
result.show()
from pyspark.ml.evaluation import BinaryClassificationEvaluator
true_positives=results[(results.Label==1) & (results.prediction==1)].count()
true_positives=results[(results.Label==0) & (results.prediction==0)].count()
true_positives=results[(results.Label==0) & (results.prediction==1)].count()
true_positives=results[(results.Label==1) & (results.prediction==0)].count()
recall=float(true_positives)/(true_positives+false_negatives)
print(recall)
precision=float(true_positives)/(true_positives+false_positives)
print(precision)
accuracy=float((true_positives+true_negatives)/(results.count()))
print(accuracy)

