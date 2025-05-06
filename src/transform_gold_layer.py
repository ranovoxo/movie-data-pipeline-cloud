from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import logging


def transform_to_gold(df_silver):
    """Aggregate and transform data to gold-level insights using PySpark"""
    # Initialize a SparkSession
    spark = SparkSession.builder.appName("MovieETL").getOrCreate()

    logging.info("Starting transformation to gold...")

    
    try:
        # Explode the 'genre_ids' list into separate rows for aggregation
        df_gold = df_silver.withColumn("genre_id", explode(col("genre_ids")))

        # Example aggregation: Average vote score per genre
        df_gold = df_gold.groupBy("genre_id").agg({"vote_average": "avg"}) \
                         .withColumnRenamed("avg(vote_average)", "avg_vote")
        
        logging.info("Transformation to gold successful.")
        return df_gold
    except Exception as e:
        logging.error(f"Error transforming to gold: {e}")
        return None