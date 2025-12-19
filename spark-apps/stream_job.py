
"""
Real-Time Trending Songs Calculator (HOT LAYER)
Uses Spark Structured Streaming to compute trending songs in 10-minute windows
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, desc, current_timestamp,
    to_timestamp, date_format, hour, minute
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def create_spark_session():
    """Create Spark session with optimized configurations"""
    return SparkSession.builder \
        .appName("MusicStreamingTrending") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def define_schema():
    """Define schema for incoming JSON events"""
    return StructType([
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("completed", BooleanType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True)
    ])


def read_from_kafka(spark, kafka_servers="kafka:29092", topic="song-plays"):
    """Read streaming data from Kafka"""
    logger.info(f"📡 Connecting to Kafka: {kafka_servers}")
    
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()


def process_stream(df, schema):
    """Transform and aggregate streaming data"""
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Convert timestamp string to timestamp type
    parsed_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Filter only completed plays (users who listened >70% of song)
    completed_plays = parsed_df.filter(col("completed") == True)
    trending = completed_plays \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes"),
            col("song_id"),
            col("title"),
            col("artist")
        ) \
        .agg(count("*").alias("play_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("song_id"),
            col("title"),
            col("artist"),
            col("play_count")
        ) \
        .orderBy(desc("play_count"))
    return trending


def save_to_datalake(df, output_path="/opt/spark/data/datalake/song_plays"):
    """Save raw events to Data Lake (parquet format with partitioning)"""
    
    # Add date and hour partitions
    partitioned_df = df.withColumn("date", date_format(col("event_time"), "yyyy-MM-dd")) \
                       .withColumn("hour", hour(col("event_time")))
    
    query = partitioned_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", f"{output_path}/_checkpoint") \
        .partitionBy("date", "hour") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query
def display_trending_console(df):
    """Display trending songs to console for monitoring"""
    
    query = df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .trigger(processingTime="30 seconds") \
        .start()
    return query


def main():
    """Main streaming application"""
    
    logger.info("🚀 Starting Music Streaming Analytics - HOT LAYER")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Define schema
    schema = define_schema()
    # Read from Kafka
    kafka_df = read_from_kafka(spark)
    
    # Process stream
    processed_df = process_stream(kafka_df, schema)   
    # Start queries
    logger.info("📊 Starting streaming queries...")
    
    # Query 1: Display trending to console
    console_query = display_trending_console(processed_df)
    
    parsed_for_save = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    datalake_query = save_to_datalake(parsed_for_save)
    
    logger.info("✓ Streaming queries started successfully!")
    logger.info("📈 Monitoring trending songs in 10-minute windows...")
    logger.info("💾 Raw events being saved to Data Lake")
    logger.info("Press Ctrl+C to stop")
    
    try:
        console_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping streaming queries...")
        console_query.stop()
        datalake_query.stop()
        spark.stop()
        logger.info("✓ Graceful shutdown complete")


if __name__ == "__main__":
    main()