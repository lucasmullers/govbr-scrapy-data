from lazy_import import lazy_callable
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "Lucas Muller",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 1),
    "retry_delay": timedelta(seconds=30),
    "retries": 2,
}

ANP_EXTRACTOR = lazy_callable("tasks.etl_bronze.extract_anp_data.ANPDataExtractor")
# from tasks.etl_bronze.extract_anp_data import ANPDataExtractor


def prep_args_extract(data):
    return {
        "url": data["url"],
        "display_name": data["display_name"],
    }


def prep_args_expectations(data):
    return {
        "filename": data
    }


def extract_anp(dag, start, anp_extractor, finish):
    scrapy_files_from_anp = PythonOperator(
        task_id="scrapy_files_from_anp",
        python_callable=lazy_callable("tasks.etl_bronze.scrapy_anp_site.scrapy_files_from_anp"),
        dag=dag,
    )

    extract = PythonOperator.partial(
        task_id="scrapy_anp",
        python_callable=anp_extractor.upload_file_to_s3,
        max_active_tis_per_dag=1,
        dag=dag,
    ).expand(op_kwargs=scrapy_files_from_anp.output.map(prep_args_extract))

    list_files_to_run_expectations = PythonOperator(
        task_id="list_files_to_run_expectations",
        python_callable=lazy_callable("tasks.etl_bronze.list_recently_downloaded.get_recently_downloaded_files"),
        dag=dag,
    )

    run_expectations = PythonOperator.partial(
        task_id="run_great_expectations",
        python_callable=lazy_callable("tasks.etl_bronze.run_expectations_to_new_files.run_great_expectations_anp_data"),
        max_active_tis_per_dag=1,
        dag=dag,
    ).expand(op_kwargs=list_files_to_run_expectations.output.map(prep_args_expectations))

    return start >> scrapy_files_from_anp >> extract >> list_files_to_run_expectations >> run_expectations >> finish


with DAG(dag_id="EXTRACT-GOVBR-BRONZE",
         start_date=datetime(2023, 5, 1),
         schedule_interval="0 0 1 * *",
         catchup=False,
         max_active_runs=1,
         max_active_tasks=5,
         tags=["bronze", "anp"],
         default_args=default_args) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    anp_extractor = ANP_EXTRACTOR()

    extract_anp(dag, start, anp_extractor, finish)
