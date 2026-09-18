from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FileStreamingDemo") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema
schema = StructType([
    StructField("empno", IntegerType(), True),
    StructField("ename", StringType(), True),
    StructField("deptno", IntegerType(), True),
    StructField("sal", IntegerType(), True)
])

# Read files as a stream
empDF = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .csv("/content/input/")

# Transformation
resultDF = empDF.filter(empDF.sal > 5000)

# Write streaming output to console
query = resultDF.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .start()

query.awaitTermination()