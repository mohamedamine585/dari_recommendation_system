from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.sql.functions import udf, desc
from pyspark.sql.types import DoubleType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("TFIDF_CosineSimilarity") \
    .config("spark.jars", "jars/mysql-connector-j-9.3.0.jar,jars/spark-sql-kafka-0-10_2.11-2.1.0-javadoc.jar") \
    .getOrCreate()

# 2. Read MySQL listings table
listings_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/dari_db") \
    .option("dbtable", "listings") \
    .option("user", "admin") \
    .option("password", "admin") \
    .load()

# 3. Read latest Kafka message (user query)
query_kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "query-topic") \
    .load()

query_text = query_kafka_df.selectExpr("CAST(value AS STRING)").collect()[-1]["value"]

# 4. Tokenize and TF-IDF on listings
tokenizer = Tokenizer(inputCol="description", outputCol="words")
words_data = tokenizer.transform(listings_df)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
featurized_data = hashingTF.transform(words_data)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idf_model = idf.fit(featurized_data)
tfidf_data = idf_model.transform(featurized_data)

# 5. Process query text into TF-IDF vector
query_df = spark.createDataFrame([(query_text,)], ["description"])
query_words = tokenizer.transform(query_df)
query_featurized = hashingTF.transform(query_words)
query_tfidf = idf_model.transform(query_featurized).select("features").collect()[0]["features"]

# 6. Define cosine similarity
def cosine_sim(v1, v2):
    return float(v1.dot(v2)) / (v1.norm(2) * v2.norm(2)) if v1.norm(2) != 0 and v2.norm(2) != 0 else 0.0

cosine_udf = udf(lambda x: cosine_sim(x, query_tfidf), DoubleType())

# 7. Compute similarity and show top 10
result = tfidf_data.withColumn("similarity", cosine_udf("features")) \
                   .orderBy(desc("similarity")) \
                   .select("id", "description", "similarity")

result.show(10, truncate=False)

spark.stop()
