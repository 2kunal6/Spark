from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Create RDD from external Data source
rdd = spark.sparkContext.textFile("sample.csv")

print(rdd)