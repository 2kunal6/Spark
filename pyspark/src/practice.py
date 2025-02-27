from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import explode

spark = SparkSession.builder.getOrCreate()

data = [
    (1, [1, 3, 5]),
    (3, [5, 6, 7, 3]),
    (2, [1, 3, 9])
]

schema = StructType([
    StructField('id', IntegerType()),
    StructField('item_list', ArrayType(StringType(), True))
])

df = spark.createDataFrame(data, schema)

df.show()

df = df.withColumn('item_list_exploded', explode('item_list'))

df.show()

df.groupBy('item_list_exploded').count().show()