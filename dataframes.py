from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lower, col
import time

spark = SparkSession.builder.appName("WordCountDF").getOrCreate()

start = time.time()
df = spark.read.text("shakespeare_100mb.txt")
words = df.select(explode(split(col("value"), "\\s+")).alias("word"))
counts = words.select(lower(col("word")).alias("word")) \
              .groupBy("word").count() \
              .orderBy("count", ascending=False)

counts.write.csv("output_df", header=True)
end = time.time()
print("Tiempo DataFrame:", end - start, "segundos")
spark.stop()
