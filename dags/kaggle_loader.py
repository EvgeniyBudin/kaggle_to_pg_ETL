import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

from kaggle_loader_utils.table_manager import TableManager
from kaggle_loader_utils.kaggle_manager import KaggleManager


with DAG(
    dag_id="from_kaggle_to_pg",
    start_date=days_ago(1),
    schedule="@once",
    catchup=False,
    params={
        "dataset_link": Param("aungpyaeap/supermarket-sales", type="string"),
        "dataset_file": Param("supermarket_sales - Sheet1.csv", type="string"),
    },
) as dag:
    start_task = EmptyOperator(task_id="start_task_id")
    load_task = PythonOperator(
        task_id="load_task_id",
        python_callable=KaggleManager().download,
        op_kwargs={"dataset_path": "{{ params.dataset_link }}", "file": "{{ params.dataset_file }}"},
    )
    insert_task = PythonOperator(
        task_id="insert_task_id",
        python_callable=TableManager().load_to_pg,
        op_kwargs={"path": "{{ params.dataset_file }}"},
    )
    end_task = EmptyOperator(task_id="end_task_id")

    start_task >> load_task >> insert_task >> end_task
