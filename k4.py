from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import time

# Step 1: Start Spark Session
spark = SparkSession.builder \
    .appName("StructuredStreamingJoinMemoryDemo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Create streaming DataFrame from rate source (simulated stream)
user_activity = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .withColumn("user_id", (expr("value % 3")).cast("int")) \
    .withColumn("action", expr("CASE WHEN value % 2 = 0 THEN 'click' ELSE 'view' END"))

# Step 3: Static user profile data
user_profiles = spark.createDataFrame([
    (0, "Alice"),
    (1, "Bob"),
    (2, "Charlie")
], ["user_id", "name"])

# Step 4: Join stream with static data
joined_df = user_activity.join(user_profiles, on="user_id", how="left")

# Step 5: Write the joined stream to memory table
query = joined_df.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("joined_table") \
    .start()

# Step 6: Wait a few seconds to let the stream populate
time.sleep(10)

# Step 7: Query the in-memory table using SQL
print("=== Streaming Join Results ===")
spark.sql("SELECT * FROM joined_table").show(truncate=False)

# Optional: Keep the stream running (comment out if testing only)
query.awaitTermination()
