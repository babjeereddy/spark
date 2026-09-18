from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)
from pyspark.sql.functions import col, sum, count


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("OrderStructuredStreaming") \
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
    .load("/content/input")



filteredDF = ordersDF.filter(
    col("amount") > 2000
)



resultDF = filteredDF.groupBy(
    "category"
).agg(
    sum("amount").alias("total_sales"),
    count("*").alias("order_count")
)



query = resultDF.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .option(
        "checkpointLocation","/content/checkpoint/"
    ) \
    .trigger(
        processingTime="10 seconds"
    ) \
    .start()



query.awaitTermination()