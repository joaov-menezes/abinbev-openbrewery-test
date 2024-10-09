from pyspark.sql import SparkSession
from pyspark.sql import DataFrame  # Importação correta
from pyspark.sql.functions import col, trim, lower, current_timestamp, initcap
from pyspark.sql.types import DoubleType
import os
import logging


spark = SparkSession.builder.appName("BreweryData").getOrCreate()

def transform_data(bronze_path: str, silver_path: str):
    """
    Realiza a transformação dos dados da camada bronze e os salva na camada prata.

    Args:
        bronze_path (str): Caminho da camada bronze.
        silver_path (str): Caminho da camada prata.
    """
    
    df = spark.read.json(bronze_path)

    if df.isEmpty():
        logging.error("O DataFrame lido da camada bronze está vazio.")
        return  
    
    df_filtered = (df.filter(col("id").isNotNull())
                   .withColumn("longitude", col("longitude").cast(DoubleType()))
                   .withColumn("latitude", col("latitude").cast(DoubleType()))
                   .withColumn("brewery_type", lower(trim(col("brewery_type"))))
                   .withColumn("street", trim(col("street")))
                   .withColumn("city", initcap(trim(col("city")))) 
                   .withColumn("state", initcap(trim(col("state"))))
                   .withColumn("state_province", initcap(trim(col("state_province"))))
                   .withColumn("postal_code", trim(col("postal_code")))
                   .withColumn("country", initcap(trim(col("country"))))
                   .withColumn("ts_load_silver", current_timestamp())
                   .cache() 
                   )

    record_count = df_filtered.count()
    logging.info(f"Transformed {record_count} records.")

    df_filtered.write.mode('overwrite').partitionBy("country").parquet(silver_path)
    
    logging.info(f"Transformed data saved to silver layer at {silver_path}")