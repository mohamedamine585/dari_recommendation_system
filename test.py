from pyspark.sql import SparkSession

# Create a Spark session with the necessary configurations
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("spark://0.0.0.0:7077") \
    .config("spark.driver.host", "0.0.0.0") \
    .config("spark.driver.port", "10000") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

# Sample data
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]

# Create a DataFrame
df = spark.createDataFrame(data, ["name", "value"])

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
