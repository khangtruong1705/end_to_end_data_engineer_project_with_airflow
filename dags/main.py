from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from etl.extract_data import extract_data
from etl.transform_dim_customer import transform_dim_customers
from etl.transform_dim_date import transform_dim_date
from etl.transform_dim_location import transform_dim_location
from etl.transform_dim_payment import transform_dim_payment
from etl.transform_dim_product import transform_dim_product
from etl.transform_dim_seller import transform_dim_seller
from etl.transform_fact_order import transform_fact_order
from etl.load_data import load_data_to_datawarehouse


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='main',
    default_args=default_args,
    description='ETL process for project-e-commerce Data Warehouse',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    
    with TaskGroup("group_1_extract") as extract_group:
        extract_task = PythonOperator(
            task_id='extract_and_load_to_staging', 
            python_callable=extract_data,
        )
    with TaskGroup("group_2_transform") as transform_group:
        transform_dim_customers_task = PythonOperator(
            task_id='transform_dim_customers',
            python_callable=transform_dim_customers,
        )
        transform_dim_date_task = PythonOperator(
            task_id='transform_dim_date',
            python_callable=transform_dim_date,
        )
        transform_dim_location_task = PythonOperator(
            task_id='transform_dim_location',
            python_callable=transform_dim_location,
        )
        transform_dim_payments_task = PythonOperator(
            task_id='transform_dim_payment',
            python_callable=transform_dim_payment,
        )
        transform_dim_product_task = PythonOperator(
            task_id='transform_dim_product',
            python_callable=transform_dim_product,
        )
        transform_dim_seller_task = PythonOperator(
            task_id='transform_dim_seller',
            python_callable=transform_dim_seller,
        )
        transform_fact_order_task = PythonOperator(
            task_id='transform_fact_order',
            python_callable=transform_fact_order,
        )
        dims = [
            transform_dim_customers_task,
            transform_dim_date_task,
            transform_dim_location_task,
            transform_dim_payments_task,
            transform_dim_product_task,
            transform_dim_seller_task
        ]
        dims >> transform_fact_order_task
        
    with TaskGroup("group_3_load") as load_group:
        load_task = PythonOperator(
            task_id='load_data_to_datawarehouse', 
            python_callable=load_data_to_datawarehouse,
        )    

    extract_group >> transform_group >> load_group