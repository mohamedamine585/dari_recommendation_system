# cron_similar_notify.py
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, lit
import os

jdbc_url = "jdbc:mysql://mysql:3306/dari_db"
db_props = {"user": "admin", "password": "admin", "driver": "com.mysql.cj.jdbc.Driver"}
als_model_path = "hdfs:///models/als_model"
als_cache_path = "/tmp/als_model_loaded.flag"

spark = SparkSession.builder \
    .appName("NotifyUsersBasedOnSimilarity") \
    .config("spark.jars", "mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# Load ALS model from HDFS or cache
if not os.path.exists(als_cache_path):
    als_model = ALSModel.load(als_model_path)
    with open(als_cache_path, "w") as f:
        f.write("loaded")
else:
    als_model = ALSModel.load(als_model_path)

# Load similar_announces
similar_df = spark.read.jdbc(jdbc_url, "similar_announces", properties=db_props)
users_df = spark.read.jdbc(jdbc_url, "user", properties=db_props).selectExpr("id as user_id")

for row in similar_df.collect():
    announce_id = row["announceId"]
    similar_ids = list(map(int, row["similarsIds"].split(",")))
    announce_df = spark.createDataFrame([(aid,) for aid in similar_ids], ["annonce_id"])

    user_input = users_df.crossJoin(announce_df)
    predictions = als_model.transform(user_input).filter(col("prediction").isNotNull())
    top_users = predictions.groupBy("user_id") \
        .agg({"prediction": "max"}) \
        .withColumnRenamed("max(prediction)", "score") \
        .orderBy(col("score").desc()) \
        .limit(1000)

    notify_df = top_users.withColumn("announce_id", lit(announce_id)) \
                         .select("announce_id", "user_id")

    notify_df.write.jdbc(jdbc_url, "to_announce_notifications", mode="append", properties=db_props)

# Clean up
if similar_df.count() > 0:
    similar_df.select("announceId").distinct().createOrReplaceTempView("to_delete")
    annonces_ids = spark.sql("SELECT announceId FROM to_delete").rdd.flatMap(lambda x: x).collect()
    for aid in annonces_ids:
        spark.sql(f"DELETE FROM similar_announces WHERE announceId = {aid}")

spark.stop()
