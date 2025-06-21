from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task 
import pendulum
import requests
import json 

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_exchange_rate_api'
BASE_CURRENCY = ['USD','SGD']
TARGET_CURRENCY= ['IDR']
API_DEFAULT_KEY = 'fca_live_KM5NwD0riRNtKvejPAa0BVb7AmhlizowcyIDZklJ'


default_args={
    'owner': 'airflow', 
    'start_date': pendulum.now().subtract(days=1),
}

## DAG 
with DAG(dag_id = 'exchange_rate_etl_pipeline',
         default_args=default_args, 
         schedule='@daily', 
         catchup=False
         ) as dags:
    
    ## inside this DAG we will define our tasks
    @task()
    def extract_exchange_rate_data(api_key, base_currency, target_currency):
       ## Extract exchange rate data from Freecurrency API using Airflow Connection.
        all_exchange_data= {}
        # Use HTTP Hook to get connection details from Airflow 
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')

        for base in base_currency: 
            for target in target_currency:
                ## Build the API endpoint
                endpoint = f'v1/latest?apikey={api_key}&base_currency={base}&currencies={target}'

                ## Make the request via the HTTP Hook 
                response=http_hook.run(endpoint)

                if response.status_code==200:
                    all_exchange_data[base] = response.json()
                    
                else: 
                    raise Exception(f"Failed to fetch exchange rate data with base currency {base} and target currency {target}: {response.status_code}")
        return all_exchange_data    

    @task()
    def transform_exchange_rate_data(base, target, exchange_rate_data):
        """Transfrom the extracted ... data"""
        print(exchange_rate_data)
        transformed_data = []
        for b in base:
            for t in target:
               current_exchange_rate= exchange_rate_data[b]["data"]
               transformed_data.append({
                    'BaseCurrency' : b, 
                    'TargetCurrency' : t,
                    'ExchangeRate': round(current_exchange_rate[t],2)
                })
        return transformed_data
    
    @task()
    def load_exchange_rate_data(transformed_data): 
        """Loas transformed data into PosgtreSQL"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor() # Cursor allows us to execute SQL queries on the database connection

        # Create table if it doesn't esixt 
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS exchange_rates (
                       Date DATE DEFAULT CURRENT_DATE,
                       BaseCurrency VARCHAR(3), 
                       TargetCurrency VARCHAR(3),
                       ExchangeRate FLOAT,
                       Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);    
        """)

        # Insert transformed data into the table 
        for row in transformed_data: 
            cursor.execute("""
            INSERT INTO exchange_rates (BaseCurrency, TargetCurrency, ExchangeRate)
            VALUES (%s,%s,%s)
            """, (row['BaseCurrency'],row['TargetCurrency'], 
                row['ExchangeRate']))

        conn.commit()
        cursor.close()

    ## DAG workflow - ETL Pipeline 

    exchange_rate_data = extract_exchange_rate_data(API_DEFAULT_KEY, BASE_CURRENCY, TARGET_CURRENCY)
    transformed_data = transform_exchange_rate_data(BASE_CURRENCY, TARGET_CURRENCY, exchange_rate_data)
    load_exchange_rate_data(transformed_data)


        