
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.master("local[*]").appName("sprk-rdd").getOrCreate()

file_rdd = spark.sparkContext.textFile("input\in.txt")

whole_rdd = spark.sparkContext.wholeTextFiles('input\ext01.txt')

print(f"parts : {file_rdd.getNumPartitions()}")

print("file_rdd", file_rdd.collect())

print(f"whole_rdd : {whole_rdd.collect()}")

map_rdd = file_rdd.map(lambda x: x.split(" "))
print("map_rdd :", map_rdd.collect())

flat_rdd = file_rdd.flatMap(lambda x: x.split(" "))

print("flat_rdd :", flat_rdd.collect())

line_len = file_rdd.map(lambda x: (x,len(x)))
print(f"line_len: {line_len.collect()}")

line_len_sorted = file_rdd.map(lambda x: (len(x),x)).sortByKey()
print(f"line_len_sorted: {line_len_sorted.collect()}")

lines_sorted = line_len_sorted.map(lambda x: x[1])
print(f"lines_sorted: {lines_sorted.collect()}")

line_len_sum = line_len.reduce(lambda x,y: ("total_words", x[1]+y[1]))
print(f"line_len_sum: {line_len_sum}")

