from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.operators.email_operator import EmailOperator
from datetime import timedelta
import logging
import os
import sys
from dotenv import load_dotenv
# Adicionar o caminho da pasta airflow ao sys.path para permitir importações
sys.path.append('/opt/airflow')

from scripts.api_to_bronze import fetch_and_save_breweries_data
from scripts.bronze_to_silver import transform_data
from scripts.silver_to_gold import create_gold_view

load_dotenv('/opt/airflow/.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_PATH =  os.getenv('BRONZE_PATH')
SILVER_PATH =  os.getenv('SILVER_PATH')
OUTPUT_VIEW =  os.getenv('OUTPUT_VIEW')
EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT')
API_URL="https://api.openbrewerydb.org/v1/breweries"


for var in [BRONZE_PATH, SILVER_PATH, OUTPUT_VIEW, EMAIL_RECIPIENT]:
    if var is None:
        logging.error(f"Missing environment variable: {var}")
        raise EnvironmentError("One or more required environment variables are not set.")


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [EMAIL_RECIPIENT],
}


with DAG('brewery_data_pipeline', 
         default_args=default_args, 
         description='Pipeline for Brewery Data', 
         schedule_interval=None, 
         start_date=days_ago(1),  
         catchup=False) as dag:

    # Tarefa 1: Coleta de dados e salvamento no bronze
    fetch_and_save_data = PythonOperator(
        task_id='fetch_and_save_brewery_data',
        python_callable=fetch_and_save_breweries_data,
        op_kwargs={'bronze_path': BRONZE_PATH,'api_url': API_URL},
    )

    # Tarefa 2: Transformação dos dados
    transform_brewery_data = PythonOperator(
        task_id='transform_brewery_data',
        python_callable=transform_data,
        op_kwargs={'bronze_path': BRONZE_PATH, 'silver_path': SILVER_PATH}
    )

    # Tarefa 3: Agregação para camada ouro
    aggregate_data = PythonOperator(
        task_id='aggregate_brewery_data',
        python_callable=create_gold_view,
        op_kwargs={'silver_path': SILVER_PATH, 'output_view': OUTPUT_VIEW}
    )

    # Tarefa de envio de e-mail em caso de falha
    failure_email_alert = EmailOperator(
        task_id='failure_email',
        to=EMAIL_RECIPIENT,
        subject='Airflow Task Failure: Brewery Data Pipeline',
        html_content='A task in the Brewery Data Pipeline has failed. Please check the Airflow logs for details.',
        trigger_rule='one_failed',  # Executa esta tarefa somente em caso de falha
    )

    fetch_and_save_data >> transform_brewery_data >> aggregate_data
    [fetch_and_save_data, transform_brewery_data, aggregate_data] >> failure_email_alert
