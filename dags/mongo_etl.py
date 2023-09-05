from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient

# Function to extract data from MongoDB
def extract_from_mongo1(**kwargs):
    client = MongoClient('mongodb://localhost:27017/')
    db = client.source_db
    collection = db.source_collection

    # Extract data (you can modify the query as per your requirement)
    data = list(collection.find())

    # Push the data to the next task using XCom
    kwargs['ti'].xcom_push(key='data_from_mongo1', value=data)

# Function to save data to another MongoDB
def load_to_mongo2(**kwargs):
    client = MongoClient('mongodb://localhost:27017/')
    db = client.destination_db
    collection = db.destination_collection

    # Pull the data from the previous task using XCom
    data = kwargs['ti'].xcom_pull(key='data_from_mongo1', task_ids='extract_task')

    # Save data to the destination MongoDB (consider batch insert for large datasets)
    if data:
        collection.insert_many(data)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'mongo_data_transfer',
    default_args=default_args,
    description='A simple DAG to transfer data between MongoDBs',
    schedule_interval='@daily',
    start_date=datetime(2023, 9, 5),
    catchup=False,
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_from_mongo1,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_to_mongo2,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
extract_task >> load_task
