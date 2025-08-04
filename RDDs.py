from pyspark import SparkContext, SparkConf
import time

conf = SparkConf().setAppName("WordCountRDD").setMaster("local[*]")
sc = SparkContext(conf=conf)

start = time.time()
rdd = sc.textFile("shakespeare_100mb.txt")
words = rdd.flatMap(lambda line: line.split()) \
           .map(lambda word: (word.lower(), 1)) \
           .reduceByKey(lambda a, b: a + b)

words.saveAsTextFile("output_rdd")
end = time.time()
print("Tiempo RDD:", end - start, "segundos")
sc.stop()
