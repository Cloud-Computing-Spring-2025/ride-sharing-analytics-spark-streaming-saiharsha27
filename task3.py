from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import time
import threading

# 1. Create a Spark session
spark = SparkSession.builder \
    .appName("StreamingWindowedStatsToCSV") \
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

# 5. Convert timestamp column
timestamped_stream = parsed_stream.withColumn("event_time", col("timestamp"))

# 6. Perform 5-minute windowed aggregation (sliding by 1 minute)
windowed_aggregation = timestamped_stream \
    .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(sum("fare_amount").alias("total_fare_in_window"))

# 7. Extract window start and end timestamps
windowed_aggregation = windowed_aggregation \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")  # Drop the original STRUCT column

# 8. Write aggregated results to memory (to periodically fetch and save as CSV)
query = windowed_aggregation.writeStream \
    .format("memory") \
    .queryName("windowed_stats") \
    .outputMode("complete") \
    .start()

# Function to periodically save aggregated data to CSV
def save_to_csv():
    while True:
        try:
            time.sleep(5)  # Sleep for a short interval before saving

            # Fetch data from memory table
            df = spark.sql("SELECT * FROM windowed_stats")

            if df.count() > 0:
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                df.write \
                    .mode("append") \
                    .option("header", "true") \
                    .csv(f"output/task3/windowed_stats_{timestamp}")
                print(f"Saved windowed stats to CSV at {timestamp}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")

# Start background thread for CSV saving
csv_thread = threading.Thread(target=save_to_csv, daemon=True)
csv_thread.start()

query.awaitTermination()
