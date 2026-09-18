from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum

spark = SparkSession.builder \
    .appName("TumblingWindowExample") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = """
product STRING,
amount DOUBLE,
event_time TIMESTAMP
"""


salesDF = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .csv("/content/input")

salesWatermarkDF = salesDF \
    .withWatermark(
        "event_time",
        "5 minutes"
    )

windowedDF = salesWatermarkDF \
    .groupBy(
        window(
            "event_time",
            "10 minutes"
        )
    ) \
    .agg(
        sum("amount").alias("total_sales")
    )

query = windowedDF.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option(
        "checkpointLocation",
        "/content/checkpoint/tumbling_window"
    ) \
    .start()

query.awaitTermination()