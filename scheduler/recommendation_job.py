import pymysql
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, explode, avg
from pyspark.sql.types import FloatType, IntegerType
from datetime import datetime

def reset_user_recommendations_table():
    print("Resetting user_recommendations table...")
    conn = pymysql.connect(
        host='mysql',
        user='admin',
        password='admin',
        database='dari_db',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS user_recommendations;")
            create_table_sql = """
            CREATE TABLE user_recommendations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                annonce_id INT NOT NULL,
                rating FLOAT NOT NULL
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
        print("Table user_recommendations reset successfully.")
    except Exception as e:
        print(f"Error resetting table: {e}")
        raise
    finally:
        conn.close()

def run_recommendation_job(spark, job_id):
    try:
        # Reset the recommendations table before inserting new data
        reset_user_recommendations_table()

        # DB connection config
        db_properties = {
            "user": "admin",
            "password": "admin",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        jdbc_url = "jdbc:mysql://mysql:3306/dari_db"

        print("Loading data from MySQL...")
        interaction_df = spark.read.jdbc(
            url=jdbc_url,
            table="user_interaction",
            properties=db_properties
        )

        als_data = interaction_df.select(
            col("user_id").cast(IntegerType()),
            col("annonce_id").cast(IntegerType()),
            col("interaction_score").cast(FloatType())
        ).groupBy("user_id", "annonce_id").agg(
            avg("interaction_score").alias("interaction_score")
        )

        print("Training recommendation model...")
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

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        als_model_path = f"/app/models/als_model_{timestamp}"
        als_model.save(als_model_path)
        print(f"ALS model saved to {als_model_path}")

        print("Generating recommendations...")
        user_recommendations = als_model.recommendForAllUsers(100)

        print("Transforming recommendations for database storage...")
        recommendations_exploded = user_recommendations.select(
            "user_id",
            explode("recommendations").alias("recommendation")
        ).select(
            "user_id",
            col("recommendation.annonce_id").alias("annonce_id"),
            col("recommendation.rating").alias("rating")
        )

        print("Saving recommendations to database...")
        recommendations_exploded.write.jdbc(
            url=jdbc_url,
            table="user_recommendations",
            mode="append",  # safe to append since table was reset
            properties=db_properties
        )

        print("Process completed successfully!")
        return True
    except Exception as e:
        print(f"Error in recommendation job: {str(e)}")
        return False

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DariRecommendationSystem").getOrCreate()
    job_id = "example_job_id"
    run_recommendation_job(spark, job_id)
    spark.stop()
