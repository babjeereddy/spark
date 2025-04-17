from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName("StructuredStreamingExample")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
raw_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "demo-topic") \
    .load()

json_df = raw_stream_df.selectExpr("CAST(value AS STRING) AS json_value")


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("value", IntegerType(), True)
])

parsed_df = json_df.select(from_json("json_value", schema).alias("data"))
final_df = parsed_df.select("data.id", "data.value")
filtered_stream_df = final_df.filter(col("value") > 10)

query = filtered_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='10 seconds')\
    .start()


query.awaitTermination()