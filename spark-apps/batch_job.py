#!/usr/bin/env python3
"""
Batch Analytics for Historical Data (COLD LAYER)
Analyzes week/month of data from Data Lake using Spark SQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, desc, sum as _sum, avg, date_trunc,
    datediff, current_date, to_date, when, countDistinct, min, max
)
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session for batch processing"""
    return SparkSession.builder \
        .appName("MusicBatchAnalytics") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def load_data_lake(spark, path="/opt/spark/data/datalake/song_plays"):
    """Load data from Data Lake (Parquet files)"""
    try:
        logger.info(f"📂 Loading data from {path}")
        df = spark.read.parquet(path)
        
        if df.schema["event_time"].dataType.typeName() == "string":
            df = df.withColumn("event_time", col("event_time").cast("timestamp"))
        
        total_events = df.count()
        logger.info(f"✓ Loaded {total_events:,} events")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to load data: {e}")
        return None

def generate_summary_report(df):
    """Generate overall summary statistics"""
    logger.info("\n" + "="*60)
    logger.info("📊 SUMMARY REPORT (ALL TIME)")
    logger.info("="*60)
    
    total_plays = df.count()
    unique_songs = df.select(countDistinct("song_id")).collect()[0][0]
    
    logger.info(f"  Total Plays:      {total_plays:,}")
    logger.info(f"  Unique Songs:     {unique_songs:,}")

    # Top Songs
    logger.info("\n🏆 TOP 5 SONGS:")
    df.groupBy("title", "artist").count().orderBy(desc("count")).limit(5).show(truncate=False)

def main():
    logger.info("🚀 Starting Batch Analytics - COLD LAYER")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Lecture des données
    df = load_data_lake(spark)
    
    if df is not None and df.count() > 0:
        generate_summary_report(df)
    else:
        logger.error("❌ No data found. Run Streaming Job first!")
    
    spark.stop()

if __name__ == "__main__":
    main()