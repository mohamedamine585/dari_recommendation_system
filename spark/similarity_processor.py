from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, Normalizer, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col, from_json, lit, expr
from pyspark.sql.types import StructType, IntegerType, FloatType, StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType

import os

# --- Config ---
jdbc_url = "jdbc:mysql://mysqlc:3306/dari_db"
db_props = {"user": "admin", "password": "admin", "driver": "com.mysql.cj.jdbc.Driver"}
kafka_bootstrap = "kafka:9092"
input_topic = "new_announce_topic"
output_topic = "similar_calc_announces_topic"
model_path = "/opt/spark/models/annonce_clustering_model"
metadata_path = "/opt/spark/models/model_metadata"
k_clusters = 10  # Number of clusters for K-means

# --- Spark session ---
spark = SparkSession.builder \
    .appName("ComputeSimilarAnnoncesStreaming") \
    .getOrCreate()

# --- Schema for input message ---
input_schema = StructType().add("announceId", StringType())

schema = StructType([
    StructField("announceId", IntegerType(), False),
    StructField("similarIds", ArrayType(IntegerType()), False),
    StructField("clusterId", StringType(), False)
])


# --- Data Loading and Preprocessing ---
def load_and_preprocess_data():
    annonces = spark.read.jdbc(jdbc_url, "annonce", properties=db_props)
    features_cols = ["prix", "type_bien", "lease_duration", "latitude", "longitude"]
    return annonces.dropna(subset=features_cols)

# --- Model Training and Management ---
def train_model(df):
    # Check if type_bien has at least 2 distinct values
    type_bien_count = df.select("type_bien").distinct().count()
    lease_duration_count = df.select("lease_duration").distinct().count()
    
    # Initialize stages list
    stages = []
    
    # Handle type_bien feature
    if type_bien_count >= 2:
        type_bien_indexer = StringIndexer(inputCol="type_bien", outputCol="type_bien_index")
        type_bien_encoder = OneHotEncoder(inputCol="type_bien_index", outputCol="type_bien_encoded")
        stages.extend([type_bien_indexer, type_bien_encoder])
        type_bien_col = "type_bien_encoded"
    else:
        print("Warning: type_bien has only one value, excluding from features")
        type_bien_col = None
    
    # Handle lease_duration feature
    if lease_duration_count >= 2:
        lease_duration_indexer = StringIndexer(inputCol="lease_duration", outputCol="lease_duration_index")
        lease_duration_encoder = OneHotEncoder(inputCol="lease_duration_index", outputCol="lease_duration_encoded")
        stages.extend([lease_duration_indexer, lease_duration_encoder])
        lease_duration_col = "lease_duration_encoded"
    else:
        print("Warning: lease_duration has only one value, excluding from features")
        lease_duration_col = None
    
    # Prepare feature columns for assembler
    feature_cols = ["prix", "latitude", "longitude"]
    if type_bien_col:
        feature_cols.append(type_bien_col)
    if lease_duration_col:
        feature_cols.append(lease_duration_col)
    
    # Assemble all features
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    normalizer = Normalizer(inputCol="features", outputCol="norm_features")
    kmeans = KMeans(featuresCol="norm_features", k=k_clusters, seed=42)
    
    stages.extend([assembler, normalizer, kmeans])
    
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(df)
    model.save(model_path)
    
    # Save metadata
    max_id = df.agg(F.max("id")).first()[0]
    spark.createDataFrame([(max_id,)], ["last_id"]).write.parquet(metadata_path)
    
    return model

def load_or_train_model():
    if os.path.exists(model_path):
        try:
            model = PipelineModel.load(model_path)
            metadata = spark.read.parquet(metadata_path).first()
            last_id = metadata["last_id"]
            
            # Check if retraining is needed
            new_count = spark.read.jdbc(jdbc_url, "annonce", properties=db_props) \
                .filter(col("id") > last_id) \
                .count()
            
            if new_count >= 100:
                print(f"Retraining model with {new_count} new announcements")
                df = load_and_preprocess_data()
                return train_model(df)
            return model
        except Exception as e:
            print(f"Error loading model: {e}. Retraining...")
            df = load_and_preprocess_data()
            return train_model(df)
    else:
        print("Training new model...")
        df = load_and_preprocess_data()
        return train_model(df)

# --- Initialize Model ---
initial_df = load_and_preprocess_data()
model = load_or_train_model()
normalized_df = model.transform(initial_df).cache()

# --- Streaming Processing ---
# --- Streaming Processing ---
def process_batch(batch_df, batch_id):
    ids = [row["announceId"] for row in batch_df.collect()]
    if not ids:
        return
    
    # Process new announcements
    new_data = spark.read.jdbc(jdbc_url, "annonce", properties=db_props) \
        .filter(col("id").isin(ids)) \
        .dropna(subset=["prix", "type_bien", "lease_duration", "latitude", "longitude"])
    
    # Check if DataFrame is empty using count()
    print(f"Processing {new_data.count()} new announcements")
    if new_data.count() > 0:
        new_transformed = model.transform(new_data)
        
        # Process each new announcement
        for row in new_transformed.collect():
            annonce_id = row["id"]
            cluster = row["prediction"]
            
            # Find similar announcements in same cluster
            similar_annonces = normalized_df.filter(
                (col("prediction") == cluster) & 
                (col("id") != annonce_id)) \
                .orderBy(col("prix")) \
                .limit(10)
            
            similar_ids = [r["id"] for r in similar_annonces.select("id").collect()]
            
            # Prepare output
            similar_ids = [int(r["id"]) for r in similar_annonces.select("id").collect()]  # Cast similarIds to int/long
            annonce_id = int(annonce_id)  # Cast announceId to int/long
            
            result = spark.createDataFrame(
               [(annonce_id, similar_ids, str(cluster))],
                schema=schema
               )

            
            for row in similar_annonces.collect():
                print(f"Annonce ID: {row['id']}, Similarity: {row['prix']}")
                
            # Write to Kafka
            result.select(
                F.lit(None).cast("string").alias("key"),
                F.to_json(F.struct("announceId", "similarIds", "clusterId")).alias("value")
            ).write \
             .format("kafka") \
             .option("kafka.bootstrap.servers", kafka_bootstrap) \
             .option("topic", output_topic) \
             .mode("append") \
             .save()

# --- Start Streaming ---
incoming_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", input_topic) \
    .load()

parsed_stream = incoming_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", input_schema).alias("data")) \
    .select("data.announceId")

query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/checkpoint_similar_annonces") \
    .start()

query.awaitTermination()