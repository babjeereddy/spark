from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Create SparkContext and StreamingContext with 1-second batches
sc = SparkContext(appName="NetworkWordCount")
ssc = StreamingContext(sc, 1)
sc.setLogLevel("WARN")
# Create DStream from socket source
lines = ssc.socketTextStream("localhost", 9999)

# Processing logic
words = lines.flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Print the results
wordCounts.pprint()

# Start streaming
ssc.start()
ssc.awaitTermination()
