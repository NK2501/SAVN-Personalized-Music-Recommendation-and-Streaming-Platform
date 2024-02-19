from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from pymongo import MongoClient

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_NAME = 'music-data'
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB_NAME = 'project_4'
MONGO_COLLECTION_NAME = 'music_data'

scala_version = '2.12'
spark_version = '3.1.2'

# Spark packages
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1',
    'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
]

# Define the schema for music data
schema = StructType([
    StructField("User ID", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
    StructField("danceability", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("mode", IntegerType(), True),
    StructField("speechiness", DoubleType(), True),
    StructField("acousticness", DoubleType(), True),
    StructField("instrumentalness", DoubleType(), True),
    StructField("liveness", DoubleType(), True),
    StructField("valence", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", IntegerType(), True),
    StructField("track_genre", StringType(), True)
])

try:
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("KafkaToMongoDB") \
        .config("spark.jars.packages", ",".join(packages)) \
        .getOrCreate()

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON and select relevant columns
    df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Write data to MongoDB
    def write_to_mongo(batchDF, batchId):
        client = MongoClient(MONGO_URI)
        collection = client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
        batchDF.write.format("mongo").mode("append").option("uri", MONGO_URI).option("database", MONGO_DB_NAME).option(
            "collection", MONGO_COLLECTION_NAME).save()


    writeStream = df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .start()

    writeStream.awaitTermination()

except Exception as e:
    print(f"Error: {e}")

finally:
    # Stop SparkSession
    spark.stop()
