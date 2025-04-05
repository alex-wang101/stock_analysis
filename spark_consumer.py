from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

# Create Spark session
spark = SparkSession.builder \
    .appName("RealTimeStockAnalysis") \
    .master("local[*]") \
    .getOcreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for the JSON data
schema = StructType() \
    .add("ticker", StringType()) \
    .add("time", StringType()) \
    .add("price", FloatType()) \
    .add("volume", IntegerType())

# Read from Kafka
stock_data_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-stream") \
    .load()

# Convert binary value to JSON
stock_data_parsed = stock_data_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Example transformation: Print all data
query = stock_data_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()