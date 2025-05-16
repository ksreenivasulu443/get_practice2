from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaPySparkExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2") \
    .getOrCreate()

# Set config to force delete temporary checkpoint location
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")


# Your Kafka streaming code here, e.g.
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

#.option("startingOffsets", "latest")---> This gives only the latest message.
#.option("startingOffsets", "earliest") ---> This gives all message from start to end.

messages = df.selectExpr("CAST(value AS STRING) as message")

query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination() #this keeps the script running until we manually stop it (CTRL+C)
