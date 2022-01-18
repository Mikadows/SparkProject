from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

spark = SparkSession \
    .builder \
    .appName("SparkProject") \
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

# print("Dataset head")
# df.show(5)

# Question 1
# print("Question 1")

# group by repo and count commits
# df.groupBy("repo")\
#     .count()\
#     .orderBy("count", ascending=False) \
#     .show(10, truncate=False)

# Question 2
# print("Question 2")

# df.filter(col("repo") == "apache/spark") \
#     .groupBy("author") \
#     .count() \
#     .orderBy("count", ascending=False) \
#     .show(1, truncate=False)

# Question 3
# print("Question 3")
# filter by repo
# cast date column to timestamp
# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# date from 6 months ago, but we do not have enough data
# date_six_months_ago = int((datetime.now() - timedelta(days=183)).timestamp())
# So we take a date from 8 years and 6 months ago
# date_eight_year_and_six_months_ago = int((datetime.now() - timedelta(days=(183 + 365 * 8))).timestamp())

# We make our calcul on the timestamp to be more performant
# df.filter((col("repo") == "apache/spark")) \
#         .withColumn("timestamp", unix_timestamp(df.date, 'E MMM d HH:mm:ss yyyy Z')) \
#         .where(col("timestamp").between(date_eight_year_and_six_months_ago, int(datetime.now().timestamp()))) \
#         .groupBy("author") \
#         .count() \
#         .orderBy("count", ascending=False) \
#         .show(5, truncate=False)

# Question 4
# load stop words list
stop_words_path_txt = "data/englishST.txt"
stop_words = spark.sparkContext.textFile(stop_words_path_txt).collect()


df.withColumn("words", split(df.message, " ")) \
    .withColumn("words", explode(col("words"))) \
    .withColumn("words", lower(col("words"))) \
    .withColumn("words", regexp_replace(col("words"), "[^a-zA-Z]", "")) \
    .where(col("words").isin([""]) == False) \
    .select("words") \
    .where(col("words").isin(stop_words) == False) \
    .groupby("words") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)

# .subtract(stop_words) \
