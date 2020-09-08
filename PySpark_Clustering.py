from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("Clustering")
sc=SparkContext(conf=conf)

#Esse, diferentes dos anteriores, é um método de ML não supervisionado
#vamos ler a base de dados
df=spark.read.csv('iris_dataset.csv',inferSchema=True,header=True)

#agora vamos preparar nossa base de dados, transformar em vetor
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
input_cols=['sepal_length','sepal_width','petal_length','petal_width']
vec_assembler=VectorAssembler(inputCols=input_cols,outputCol="features")
final_data=vec_assembler.transform(df)

#o próximo passo, é construir o modelo K-Means
#precisamos definir o valor de k
#para tanto, vamos utilizar o método elbow para determinar o valor ideal
#para k (lembrando que k tem que ter um valor mínimo de 2 para formar
#clusters)
from pyspark.ml.custering import KMeans
errors=[]
for k in range(2,10):
	kmeans=
KMeans(featuresCol='features',k=k)
	model=kmeans.fit(final_data)
	intra_distance=
model.computeCost(final_data)
erros.append(intra_distance)

#agora, podemos plotar a distância intracluster com o número de clusters
#utilizando numpy e matplotlib
import pandas as pd
import umpy as np
import matplotlib.pyplot as plt
custer_number=range(2,10)
plt.xlabel('Number of Clusters')
plt.ylabel('SSE')
plt.scatter(cluster_number,errors)
plt.show()

#pelo plot acima, vemos que o valor de SSE passa a apresentar certa estabilidade em k=3,
#com uma taxa de variação apresentando uma redução drástica
#então façamos com k=3
#na terceira linha abaixo, temos o número de observações por cluster
kmeans=KMeans(featuresCol='features',k=3)
model=kmeans.fit(final_data)
model.transform(final_data).groupBy('prediction').count().show()

#podemos utilizar a função de transformação para prever a qual cluster
#cada uma das observações pertenceriam
predictions=model.transform(final_data)
predictions.groupBy('species','prediction').count().show()

#agora vamos visualizar os clusters
pandas_df=predictions.toPandas()
pandas_df.head()
from mpl_toolkits.mplot3d import Axes3D
cluster_vis=plt.figure(figsize=(12,10)).gca(projection='3d')
cluster_vis.scatter(pandas_df.sepal_length,pandas_df.sepal_width,pandas_df.petal_length,c=pandas_df.prediction,depthshade=False)
plt.show()
