from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .appName("KafkaConnectExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # Optional: reduces log spam

# Read from Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-topic") \
    .option("startingOffsets", "latest") \
    .load()

#df.show().toString()
# Convert Kafka's binary key and value to string
string_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Example: simple transformation (split value by comma)
transformed_df = string_df.withColumn("value_upper", expr("upper(value)"))

# Output to console
query = transformed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()