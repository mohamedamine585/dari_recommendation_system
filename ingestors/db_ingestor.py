from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, Normalizer, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, from_json, lit, expr
from pyspark.sql.types import StructType, IntegerType, FloatType, StringType
from pyspark.sql import functions as F

# --- Config ---
jdbc_url = "jdbc:mysql://mysql:3306/dari_db"
db_props = {"user": "admin", "password": "admin", "driver": "com.mysql.cj.jdbc.Driver"}
kafka_bootstrap = "kafka:9092"
input_topic = "new_announce_topic"
output_topic = "similar_calc_announces_topic"

# --- Spark session ---
spark = SparkSession.builder \
    .appName("ComputeSimilarAnnoncesStreaming") \
    .config("spark.jars", "mysql-connector-j-8.0.23.jar") \
    .getOrCreate()

# --- Schema for input message ---
input_schema = StructType().add("id", IntegerType())

# --- Load and preprocess annonces ---
annonces = spark.read.jdbc(jdbc_url, "annonce", properties=db_props)
features_cols = ["prix", "type_bien", "latitude", "longitude"]
annonces_filtered = annonces.dropna(subset=features_cols)

# Preprocess categorical feature (type_bien)
indexer = StringIndexer(inputCol="type_bien", outputCol="type_bien_index")
encoder = OneHotEncoder(inputCol="type_bien_index", outputCol="type_bien_encoded")

# Assemble all features
assembler = VectorAssembler(
    inputCols=["prix", "type_bien_encoded", "latitude", "longitude"],
    outputCol="features"
)

# Normalize features
normalizer = Normalizer(inputCol="features", outputCol="norm_features")

# Create pipeline
pipeline = Pipeline(stages=[indexer, encoder, assembler, normalizer])
model = pipeline.fit(annonces_filtered)
normalized_df = model.transform(annonces_filtered).cache()

# --- Proper cosine similarity UDF ---
def cosine_similarity(v1, v2):
    return float(v1.dot(v2)) / (float(v1.norm(2)) * float(v2.norm(2)))

# --- Read new annonce IDs from Kafka ---
incoming_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load()

# --- Parse and extract annonce ID from Kafka message ---
parsed_stream = incoming_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", input_schema).alias("data")) \
    .select("data.id")

# --- Function to process each annonce ID in micro-batches ---
def process_batch(batch_df, batch_id):
    # Collect all IDs in this batch
    ids = [row["id"] for row in batch_df.collect()]
    
    if not ids:
        return
    
    # Get all target vectors at once
    target_vectors = normalized_df.filter(col("id").isin(ids)) \
        .select("id", "norm_features").collect()
    
    for row in target_vectors:
        annonce_id = row["id"]
        target_vector = row["norm_features"]
        
        # Calculate similarity with all other annonces
        similar_annonces = normalized_df.filter(col("id") != annonce_id) \
            .withColumn("similarity", expr(f"aggregate(transform(norm_features, (x,i) -> x * {target_vector[i]}), 0D, (acc, x) -> acc + x) / " +
                       f"(sqrt(aggregate(norm_features, 0D, (acc, x) -> acc + x*x)) * " +
                       f"sqrt(aggregate({target_vector}, 0D, (acc, x) -> acc + x*x)))")) \
            .orderBy(col("similarity").desc()) \
            .limit(10)
        
        # Prepare output
        similar_ids = [r["id"] for r in similar_annonces.select("id").collect()]
        avg_similarity = similar_annonces.agg(F.avg("similarity")).first()[0] or 0.0
        
        # Create result DataFrame
        result = spark.createDataFrame([(annonce_id, str(similar_ids), float(avg_similarity))],
                                     ["announceId", "similarIds", "similarity"])
        
        # Write to Kafka
        result.select(
            F.lit(None).cast("string").alias("key"),
            F.to_json(F.struct("announceId", "similarIds", "similarity")).alias("value")
        ).write \
         .format("kafka") \
         .option("kafka.bootstrap.servers", kafka_bootstrap) \
         .option("topic", output_topic) \
         .mode("append") \
         .save()

# --- Start streaming query ---
query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/checkpoint_similar_annonces") \
    .start()

query.awaitTermination()