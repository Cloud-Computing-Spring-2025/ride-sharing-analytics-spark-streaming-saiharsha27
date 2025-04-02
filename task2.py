from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Create a Spark session
spark = SparkSession.builder \
    .appName("StreamingDriverStatsToCSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define the schema of the JSON messages
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", TimestampType())

# 3. Read data from the socket stream
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse the incoming JSON strings using the schema
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 5. Aggregate data by driver_id
aggregated_stream = parsed_stream.groupBy("driver_id") \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# 6. Write aggregated results to memory (to periodically fetch and save as CSV)
query = aggregated_stream.writeStream \
    .format("memory") \
    .queryName("driver_stats") \
    .outputMode("complete") \
    .start()

import time
import threading

# Function to periodically save aggregated data to CSV
def save_to_csv():
    while True:
        try:
            time.sleep(5)  # Sleep for a short interval before saving

            # Fetch data from memory table
            df = spark.sql("SELECT * FROM driver_stats")

            if df.count() > 0:
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                df.write \
                    .mode("append") \
                    .option("header", "true") \
                    .csv(f"output/task2/driver_stats_{timestamp}")
                print(f"Saved driver stats to CSV at {timestamp}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")

# Start background thread for CSV saving
csv_thread = threading.Thread(target=save_to_csv, daemon=True)
csv_thread.start()

query.awaitTermination()
