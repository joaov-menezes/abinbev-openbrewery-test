from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import os
import logging


spark = (SparkSession.builder.appName("BreweryData").getOrCreate())

def create_gold_view(silver_path, output_view):
    """
    Cria uma agregação dos dados da camada prata e gera uma view global.

    Args:
        silver_path (str): Caminho da camada prata.
        output_view (str): Nome da view temporária a ser criada.
    """
    df = spark.read.parquet(silver_path)

    df.createOrReplaceTempView("temp_brewery_view")

    
    # Agrega os dados
    aggregated_df = (df.groupBy("country", "brewery_type")
                       .agg(count("*").alias("brewery_count")))
    
    aggregated_df.createOrReplaceGlobalTempView(output_view)
    
    logging.info(f"Gold view {output_view} created successfully.")
    
    count_result = spark.sql(f"SELECT SUM(brewery_count) AS total_breweries FROM global_temp.{output_view}").collect()[0]['total_breweries']
    logging.info(f"Total number of breweries in view '{output_view}': {count_result}")
