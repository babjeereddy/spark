from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)
from pyspark.sql.functions import col



spark = SparkSession.builder \
    .appName("EmployeeFileStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



empSchema = StructType([
    StructField("empno", IntegerType(), True),
    StructField("ename", StringType(), True),
    StructField("deptno", IntegerType(), True),
    StructField("sal", IntegerType(), True)
])



empDF = spark.readStream \
    .format("csv") \
    .schema(empSchema) \
    .option("header", "true") \
    .load("/content/input/")



resultDF = empDF \
    .filter(col("sal") > 5000) \
    .select(
        "empno",
        "ename",
        "deptno",
        "sal"
    )




query = resultDF.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .option(
        "checkpointLocation",
        "/content/checkpoint"          #Checkpointing helps Spark recover a streaming query after failure without simply starting from scratch.
    ) \
    .trigger(processingTime="10 seconds") \
    .start()



query.awaitTermination()