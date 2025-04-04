from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


spark = SparkSession.builder \
    .appName("StockStreaming") \
    .getOrCreate()


# Connect to Kafka 
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-stream") \
    .load()

# Extract value field from the Kafka message
stock_data = kafka_stream.selectExpr("CAST(value AS STRING)")

stock_data_parsed = stock_data.select(
    expr("json_tuple(value, 'ticker', 'time', 'price', 'volume')").alias("ticker", "time", "price", "volume")
)

# 
stock_data_parsed = stock_data_parsed \
    .withColumn("price", stock_data_parsed["price"].cast("float")) \
    .withColumn("volume", stock_data_parsed["volume"].cast("integer"))

stock_data_parsed.createOrReplaceTempView("stock_data")

query = stock_data_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination
query.awaitTermination()
