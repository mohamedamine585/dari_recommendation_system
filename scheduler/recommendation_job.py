from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.functions import col, explode, avg
from datetime import datetime

def run_recommendation_job(spark, job_id):
    try:
        # Initialize Spark session
        # Instead of creating a new SparkSession, use the one passed in

        # DB connection config
        db_properties = {
            "user": "admin",
            "password": "admin",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        jdbc_url = "jdbc:mysql://mysql:3306/dari_db"

        # Load data from MySQL
        print("Loading data from MySQL...")
        interaction_df = spark.read.jdbc(
            url=jdbc_url,
            table="user_interaction",
            properties=db_properties
        )

        # Prepare ALS data (interaction data with proper types)
        als_data = interaction_df.select(
            col("user_id").cast(IntegerType()),
            col("annonce_id").cast(IntegerType()),
            col("interaction_score").cast(FloatType())
        ).groupBy("user_id", "annonce_id").agg(
            avg("interaction_score").alias("interaction_score")
        )

        print("Training recommendation model...")

        # Configure ALS model
        als = ALS(
            userCol="user_id",
            itemCol="annonce_id",
            ratingCol="interaction_score",
            coldStartStrategy="drop",
            rank=15,
            maxIter=15,
            regParam=0.1
        )
        als_model = als.fit(als_data)

        # --- Save the trained ALS model to disk ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        als_model_path = f"/app/models/als_model_{timestamp}"
        als_model.save(als_model_path)
        print(f"ALS model saved to {als_model_path}")

        # Generate recommendations for all users
        print("Generating recommendations...")
        user_recommendations = als_model.recommendForAllUsers(100)

        # Transform the recommendations to a MySQL-compatible format
        print("Transforming recommendations for database storage...")
        recommendations_exploded = user_recommendations.select(
            "user_id",
            explode("recommendations").alias("recommendation")
        ).select(
            "user_id",
            col("recommendation.annonce_id").alias("annonce_id"),
            col("recommendation.rating").alias("rating")
        )

        # Save recommendations to database
        print("Saving recommendations to database...")
        recommendations_exploded.write.jdbc(
            url=jdbc_url,
            table="user_recommendations",
            mode="append",
            properties=db_properties
        )

        print("Process completed successfully!")
        # Return True if successful, False otherwise
        return True
    except Exception as e:
        print(f"Error in recommendation job: {str(e)}")
        return False

spark = SparkSession.builder \
    .appName("DariRecommendationSystem") \
    .getOrCreate()

job_id = "example_job_id"
run_recommendation_job(spark, job_id)

spark.stop()