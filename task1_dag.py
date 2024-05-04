from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import requests
import json
import pandas as pd
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 29),
    'retries': 1,
}

dag = DAG(
    'covid_data_etl_dag',
    default_args=default_args,
    description='ETL pipeline for COVID-19 data',
    schedule_interval=None,  # Set to None to trigger manually or via sensors
)

def fetch_covid_data(country_code):
    url = f"https://health.google.com/covid-19/open-data/raw-data?country={country_code}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Failed to fetch data for country {country_code}: {response.status_code}")

def clean_covid_data(data):
    # Convert the data to a DataFrame for easier manipulation
    df = pd.DataFrame(data)

    # Handle missing values
    df.fillna(0, inplace=True)  # Example: fill missing values with 0

    # Ensure consistent data types
    df['cases'] = df['cases'].astype(int)
    df['deaths'] = df['deaths'].astype(int)
    df['recoveries'] = df['recoveries'].astype(int)

    # Standardize geographical names
    df['state'] = df['state'].apply(lambda x: x.upper())  # Example: convert state names to uppercase

    cleaned_data = df.to_dict(orient='records')
    return cleaned_data

def aggregate_covid_data(cleaned_data):
    # Example aggregation logic: group by state and calculate total cases, deaths, and recoveries
    aggregated_data = []
    for entry in cleaned_data:
        state = entry['state']
        # district = entry['district']
        # block = entry['block']
        cases = entry['cases']
        deaths = entry['deaths']
        recoveries = entry['recoveries']

        # Aggregate by state
        state_entry = next((item for item in aggregated_data if item['state'] == state), None)
        if state_entry:
            state_entry['cases'] += cases
            state_entry['deaths'] += deaths
            state_entry['recoveries'] += recoveries
        else:
            aggregated_data.append({'state': state, 'cases': cases, 'deaths': deaths, 'recoveries': recoveries})

    return aggregated_data

def insert_data_to_postgres(**kwargs):
    # Get the cleaned and aggregated data from the task instance context
    cleaned_data = kwargs['ti'].xcom_pull(task_ids='clean_covid_data')
    aggregated_data = kwargs['ti'].xcom_pull(task_ids='aggregate_covid_data')

    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host='your_postgresql_host',
        database='covid19data',
        user='postgres',
        password='postgres',
    )
    cursor = conn.cursor()

    try:
        # Insert cleaned data into PostgreSQL table
        for entry in cleaned_data:
            cursor.execute(
                """
                INSERT INTO covid_data (state, cases, deaths, recoveries)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    entry['state'],
                    entry['cases'],
                    entry['deaths'],
                    entry['recoveries'],
                )
            )

        # Insert aggregated data into PostgreSQL table
        for entry in aggregated_data:
            cursor.execute(
                """
                INSERT INTO covid_data_aggregated (state, total_cases, total_deaths, total_recoveries)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    entry['state'],
                    entry['total_cases'],
                    entry['total_deaths'],
                    entry['total_recoveries'],
                )
            )

        # Commit the changes and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise e


fetch_data_task = PythonOperator(
    task_id='fetch_covid_data',
    python_callable=fetch_covid_data,
    op_kwargs={'country_code': '{{ dag_run.conf["country_code"] }}'},  # Extract country code from DAG run configuration
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_covid_data',
    python_callable=clean_covid_data,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="fetch_covid_data") }}'},  # Pass data from fetch task
    dag=dag,
)

aggregate_data_task = PythonOperator(
    task_id='aggregate_covid_data',
    python_callable=aggregate_covid_data,
    op_kwargs={'cleaned_data': '{{ task_instance.xcom_pull(task_ids="clean_covid_data") }}'},  # Pass cleaned data
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',  # Connection ID for PostgreSQL
    sql="""
    CREATE TABLE IF NOT EXISTS covid_data (
        id SERIAL PRIMARY KEY,
        state VARCHAR(255),
        cases INT,
        deaths INT,
        recoveries INT
    );
    """,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data_to_postgres,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> clean_data_task >> aggregate_data_task >> create_table_task >> insert_data_task

# Define a sensor to trigger the DAG when a new file is added to Amazon S3
file_sensor = S3KeySensor(
    task_id='file_sensor',
    aws_conn_id='amazon_default',
    bucket_name='taskpractiseforassignment',
    bucket_key='trigger.txt',
    timeout=3600,  # Set timeout as needed
    poke_interval=60,  # Polling interval in seconds
    mode='poke',
    dag=dag,
)

file_sensor >> fetch_data_task
