from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("RandomForest")
sc=SparkContext(conf=conf)

df=spark.read.csv('affairs.csv',inferSchema=True,header=True)
print((df.count(),len(df.columns)))
df.printSchema()

#vamos criar um vetor único com todas as variáveis independentes que vamos
#utilizar
from pyspark.ml.feature import VectorAssembler
df_assembler=VectorAssembler(inputCols=['rate_marriage','age','yrs_married','children','religious'],outputCol="features")
df=df_assembler.transform(df)

#agora,selecionamos apenas o vetor features como input e a nossa variável
#dependente affairs como output (pois é o que queremos explicar)
model_df=df.select(['features','affairs'])

#agora vamos separar a base de dados entre treino e teste (75% e 25%)
train_df,test_df=model_df.randomSplit([0.75,0.25])

#agora vamos criar o modelo random forest
#abaixo, vemos que escolhermos utilizar 50 árvores de decisão
from pyspark.ml.classification import RandomForestClassifier
rf_classifier=RandomForestClassifier(labelCol='affairs',numTrees=50).fit(train_df)

#vamos avaliar a performance dos dados
rf_predictions=rf_classifier.transform(test_df)
rf_predictions.show()
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
rf_accuracy=MulticlassClassificationEvaluator(labelCol='affairs',metricName='accuracy').evaluate(rf_prections)
rf_accuracy
rf_precision=MulticlassClassificationEvaluator(labelCol='affairs',metricName='weightedPrecision').evaluate(rf_predictions)
rf_precision

#agora vamos salvar o modelo, olha que interessante, vamos salvar o modelo
#localmente como um objeto
from pyspark.ml.classification import RandomForestClassificationModel
rf_classifier.save("/home/jovyan/work/RF_model")
#agora, o próximo passo é carregar o modelo para previsão
rf=RandomForestClassificationModel.load("/home/jovyan/work/RF_model")
new_predictions=rf.transform(new_df)
