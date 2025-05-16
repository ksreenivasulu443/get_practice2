from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoSparkConnectorExample") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .getOrCreate()

df = spark.read.format("mongodb") \
    .option("database", "mydb") \
    .option("collection", "Contact_Info") \
    .load()

df.show()

df.filter(df.Surname == 'Arun').show()

spark.stop()
