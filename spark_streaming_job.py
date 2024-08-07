from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Define the Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "test-topic"

# Define the schema for the incoming data
schema = StructType().add("value", StringType())

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Extract the value from the Kafka message and cast it to a string
value_df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON string into a DataFrame with the specified schema
json_df = value_df.withColumn("value", from_json(col("value"), schema))

# Select the value column
final_df = json_df.select(col("value.value").alias("message"))

# Define the output directory for the log files
output_dir = "/opt/spark-streaming-job/logs"

# Write the streaming DataFrame to a log file
query = final_df \
    .writeStream \
    .outputMode("append") \
    .format("text") \
    .option("path", output_dir) \
    .option("checkpointLocation", "/opt/spark-streaming-job/checkpoints") \
    .start()

# Wait for the termination of the query
query.awaitTermination()
