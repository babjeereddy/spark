from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col



spark = SparkSession.builder \
    .master("local[*]") \
    .appName("FileStreamingToFile") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


orderSchema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True)
])



ordersDF = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(orderSchema) \
    .load("/content/input/")




resultDF = ordersDF \
    .filter(col("amount") > 2000) \
    .select(
        "order_id",
        "customer_id",
        "product",
        "category",
        "amount"
    )




query = resultDF.writeStream \
    .format("csv") \
    .option("header", "true") \
    .outputMode("append") \
    .option(
        "path",
        "/content/output"
    ) \
    .option(
        "checkpointLocation","/content/checkpoint/"
    ) \
    .trigger(processingTime="10 seconds") \
    .start()



query.awaitTermination()