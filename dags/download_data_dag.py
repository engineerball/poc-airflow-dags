import json
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta

from utils.etl_utils import *

default_args = {
    "owner": "tk",
    "depends_on_past": False,
    "start_date": datetime(2022, 6, 11),
    "email": ["tk@aiqube.co"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def save_data_to_mongo():
    import pandas as pd
    transaction_data = pd.read_csv('/tmp/transaction_lean_customer_data.csv')
    user_data = pd.read_csv('/tmp/user_lean_customer_data.csv')
    product_data = pd.read_csv('/tmp/product_lean_customer_data.csv')
    
    return save_to_mongo("transaction", transaction_data.to_dict(orient="records")) and save_to_mongo("user", user_data.to_dict(orient="records")) and save_to_mongo("product", product_data.to_dict(orient="records")) is not None
    
    

def save_to_mongo(collection, docs) -> object:
    hook = MongoHook(conn_id='mongodb_local_con_id')
    return hook.insert_many(mongo_collection=collection, docs=docs, mongo_db='ecommerce') is not None
    

with DAG('DATA_TO_MONGODB', schedule_interval=timedelta(minutes=10),
        default_args=default_args,
        catchup=False) as dag:
    
    sample_base_filepath = "../data"
    sample_remote_filepath = ""


    with TaskGroup("extraction_group") as extraction_group:
        extract_users_task = PythonOperator(
            task_id="extract_users_task", python_callable=pull_user_data
        )

        extract_products_task = PythonOperator(
            task_id="extract_products_task", python_callable=pull_product_data
        )

        extract_transaction_task = PythonOperator(
            task_id="extract_transaction_task",
            python_callable=pull_transaction_data)

    save_data_to_mongo_op = PythonOperator(
        task_id="save_data_to_mongo_op",
        python_callable=save_data_to_mongo)

extraction_group >> save_data_to_mongo_op