from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("LinearRegression")
sc=SparkContext(conf=conf)

#vamos primeiro importar os dados
#devemos colocar o caminho correto, iniciando de file// e ai o endereço
#se o arquivo estiver no hdfs devemos colocar hdfs:// e ai colocar o endereço
df=spark.read.csv('file:///home/hadoop/machine-learning-with-pyspark/chapter_4_Linear_Regression/Linear_regression_dataset.csv',inferSchema=True,header=True)

#vamos fazer uma análise exploratória dos dados
print((df.cont(),len(df.columns)))
df.printSchema()
df.describe().show(3,False)
df.head(3)

#vamos analisar a correlação entre as variáveis utilizadas
from pyspark.sql.functions import corr
df.select(corr('var_1','output')).show()

#vamos importar os pacotes para fazer a estimação
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler

#vamos agora criar um vetor coluna que vai contar com as variáveis que vamos utilizar
#o único vetor coluna irá contar com todas as variáveis
vec_assembler=VectorAssembler(inputCols=['var_1','var_2','var_3','var_4','var_5'],outputCol='features')
features_df=vec_assembler.transform(df)
features_df.printSchema()
#acrescentamos o vetor com todas as 5 colunas à base origina, criando uma
#nova, como vemos abaixo
features_df.select('features').show(5,False)

#então, selecionamos o subconjunto da base de dados e selecionamos apenas
#o vetor features que tem todas as colunas que vamos utilizar e a coluna
#output para criar a Regressão Linear
model_df=features_df.select('features','output') #essa base com vetor e y que vamos usar
model_df.show(5,False)

#para avaliar a performance de nossa regressão, vamos separa-la entre base
#de treino e de teste
train_df,test_df=model_df.randomSplit([0.7,0.3])

#por fim, vamos importar as classes e métodos importantes necessários para
#fazer a regressão
from pyspark.ml.regression import LinearRegression
lin_Reg=LinearRegression(labelCol='output')
lr_model=lin_Reg.fit(train_df)
print(lr_model.coefficients)
print(lr_model.intercept)
training_predictions=lr_model.evaluate(train_df)
print(training_predictions.r2)
#para ver se a performance do modelo é semelhante ao utilizarmos a base
#de teste ao invés da de treino, vamos ver a estatística r2 para este caso
#também
test_predictions=lr_model.evaluate(test_df)
print(test_results.r2)
print(test_results.meanSauaredError)


