from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumerApp") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Read from Kafka topic 'user-interactions'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "user-interactions") \
    .load()

# Convert Kafka data (key and value) to String format
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Process data (e.g., filter by some condition or clean data)
df = df.filter(col("value").contains("property"))

# Pass the processed data to the recommender model
user_data = df.collect()  # Collect data as a list of rows
recommendations = recommender.predict(user_data)  # Send to recommender

# Output recommendations
print(recommendations)
