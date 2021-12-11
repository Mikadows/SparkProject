from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, regexp_replace

spark = SparkSession \
    .builder \
    .appName("TP3") \
    .master("local[*]") \
    .getOrCreate()

dataset_path = "data/full.csv"

print("Loading csv...")
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("inferSchema", "true") \
    .load(dataset_path)
print("Loading OK !")

print("Cleaning...")
df = df.withColumn("message", regexp_replace("message", "\\n", " "))
df.select("message").show(5, truncate=False)

print("Dataset head")
df.show(5)

# Question 1
# group by repo and count commits
df.groupBy("repo")\
    .count()\
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)
