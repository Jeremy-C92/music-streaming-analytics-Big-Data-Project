from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder \
    .appName("MusicBatchAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("--- DEMARRAGE ANALYSE BATCH (COLD LAYER) ---")
print("Lecture du Data Lake (/app/data/music_lake)...")

try:
    # Lecture des fichiers Parquet
    df = spark.read.parquet("/app/data/music_lake")
    
    total_count = df.count()
    print(f"Total des écoutes stockées : {total_count}")
    
    if total_count > 0:
        print("\n--- TOP GENRES (ALL TIME) ---")
        df.groupBy("genre").count().orderBy(desc("count")).show()
        
        print("\n--- TOP ARTISTES (ALL TIME) ---")
        df.groupBy("artist").count().orderBy(desc("count")).limit(5).show()
    else:
        print("Pas assez de données pour l'analyse.")
        
except Exception as e:
    print(f"Erreur (Probablement pas encore de données): {e}")