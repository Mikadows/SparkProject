from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession \
    .builder \
    .appName("TP3") \
    .master("local[*]") \
    .getOrCreate()

dataset_path = "data/full.csv"

print("Loading csv...")
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(dataset_path)
print("Loading OK !")

print("Dataset head")
df.show(5)
