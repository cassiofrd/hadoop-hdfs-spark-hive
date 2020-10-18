from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
df=spark.read.csv('Basededados.csv',inferSchema=True,header=True)
