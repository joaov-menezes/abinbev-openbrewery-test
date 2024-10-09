import requests
import logging
import pandas as pd
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv


spark = SparkSession.builder.appName("BreweryData").getOrCreate()

def fetch_breweries_data(api_url: str, max_pages=1000):
    """
    Coleta os dados da API de cervejarias e retorna um DataFrame Pandas.

    Args:
        api_url (str): URL da API de cervejarias.
        max_pages (int): Número máximo de páginas para buscar dados.
        
    Returns:
        pd.DataFrame: Dados das cervejarias coletados da API.
    """
    data = []
    for page in range(1, max_pages + 1):
        try:
            logging.info(f"Fetching data from page {page}...")
            response = requests.get(api_url, params={'page': page, 'per_page': 200}, timeout=10)
            response.raise_for_status()
            breweries = response.json()
            if not breweries:
                logging.info("No more data found, stopping fetch.")
                break
            data.extend(breweries)
        except requests.exceptions.Timeout:
            logging.error(f"Timeout occurred on page {page}. Skipping...")
            continue
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred on page {page}: {http_err}")
            continue
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error occurred on page {page}: {req_err}")
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred on page {page}: {e}")
            continue
    logging.info(f"Fetched {len(data)} records.")
    return pd.DataFrame(data)

def save_bronze_data(df, output_path):
    """
    Converte um DataFrame Pandas para um DataFrame Spark e salva como JSON.

    Args:
        df (pd.DataFrame): DataFrame Pandas contendo os dados.
        output_path (str): Caminho para salvar os dados.
    """
    if df.empty:
        logging.warning("The DataFrame is empty. No data to save.")
        return
    
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode('overwrite').json(output_path)
    logging.info(f"Data saved to {output_path}")

def fetch_and_save_breweries_data(api_url, bronze_path):
    """
    Coleta dados da API e salva-os na camada bronze.

    Args:
        bronze_path (str): Caminho da camada bronze para salvar os dados.
    """
    df = fetch_breweries_data(api_url)
    
    assert isinstance(df, pd.DataFrame), "O retorno deve ser um DataFrame Pandas."
    assert "id" in df.columns, "A coluna 'id' deveria estar no DataFrame."
    
    save_bronze_data(df, bronze_path)