from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.mssql_operator import MsSqlOperator 
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from utils.etl_utils import *



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 6, 20),
    "email": ["tk@aiqube.co"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}


with DAG(
    "MONGODB_ETL_MSSQL", default_args=default_args,
    schedule_interval=timedelta(hours=1)
) as dag:

    sample_base_filepath = "../data"
    sample_remote_filepath = ""

    with TaskGroup("extraction_group") as extraction_group:
        extract_users_task = PythonOperator(
            task_id="extract_users_task", python_callable=pull_mongo_user_data
        )

        extract_products_task = PythonOperator(
            task_id="extract_products_task", python_callable=pull_mongo_product_data
        )

        extract_transaction_task = PythonOperator(
            task_id="extract_transaction_task",
            python_callable=pull_mongo_transaction_data)

    create_table_platinum_customer_table = MsSqlOperator(
        mssql_conn_id='mssql_conn_id',
        task_id='create_table_platinum_customer_table',
        sql=create_table_query,
        # autocommit=True,
        database=f'{Variable.get("MSSQL_DB")}',
        dag=dag,
    )

    with TaskGroup("processing_group") as processing_group:
        generate_basket_analysis_task = PythonOperator(
            task_id="generate_basket_analysis_task",
            python_callable=get_basket_analysis_dataset,
        )

        generate_recommendation_engine_task = PythonOperator(
            task_id="generate_recommendation_engine_task",
            python_callable=get_recommendation_engine_dataset,
        )

        get_platinum_customer_task = PythonOperator(
            task_id="get_platinum_customer_task",
            python_callable=get_platinum_customer)


extraction_group >> create_table_platinum_customer_table >> processing_group
