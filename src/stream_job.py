from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Initialisation Spark
spark = SparkSession.builder \
    .appName("MusicStreamLambda") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema des données JSON
schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("song_id", IntegerType()),
    StructField("genre", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("artist", StringType()),
    StructField("track", StringType())
])

# 1. Lecture depuis Kafka
print("Connexion à Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "music_streams") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsing JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# 2. HOT LAYER: Aggregation Temps Réel (Top Genres des 2 dernières minutes)
hot_query = parsed_df \
    .groupBy(window(col("timestamp"), "2 minutes", "30 seconds"), col("genre")) \
    .count() \
    .orderBy("window") \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 3. COLD LAYER: Sauvegarde brute (Data Lake)
cold_query = parsed_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/app/data/music_lake") \
    .option("checkpointLocation", "/app/data/checkpoint") \
    .outputMode("append") \
    .start()

print("Streaming lancé... (Hot Layer + Cold Layer)")
spark.streams.awaitAnyTermination()