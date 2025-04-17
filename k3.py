from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Step 1: Create Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("FileStreamToKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

file_df = spark.readStream \
    .format("text") \
    .load("c:/input/")  # CHANGE THIS

transformed_df = file_df.withColumn("value", expr("upper(value)"))

transformed_df.selectExpr("CAST(value AS STRING) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "my-topic") \
    .option("checkpointLocation", "/tmp/file-to-kafka-checkpoint1") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
