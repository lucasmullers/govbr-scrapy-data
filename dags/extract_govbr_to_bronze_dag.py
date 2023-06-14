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

INEP_EXTRACTOR = lazy_callable("tasks.etl_bronze.extract_inep_data.INEPDataExtractor")
DATASOURCES = ["censo-da-educacao-superior", "enem-por-escola", "censo-escolar", "enade"]  #, "enem",


def prep_args(data):
    return {
        "url": data["url"],
        "display_name": data["display_name"],
        "datasource": data["datasource"]
    }


with DAG(dag_id="EXTRACT-INEP-BRONZE",
         start_date=datetime(2023, 5, 1),
         schedule_interval="0 0 1 * *",
         catchup=False,
         max_active_runs=1,
         max_active_tasks=5,
         tags=["govbr", "bronze", "inep"],
         default_args=default_args) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    inep_extractor = INEP_EXTRACTOR()

    for datasource in DATASOURCES:
        scrapy_files_from_inep = PythonOperator(
            task_id=f"scrapy_files_from_inep_{datasource}",
            python_callable=lazy_callable("tasks.etl_bronze.scrapy_inep_site.scrapy_files_from_inep"),
            op_kwargs={"datasource": datasource}
        )

        extract = PythonOperator.partial(
            task_id=f"scrapy_{datasource}",
            python_callable=inep_extractor.upload_file_to_s3,
            max_active_tis_per_dag=1,
        ).expand(op_kwargs=scrapy_files_from_inep.output.map(prep_args))

        _ = start >> scrapy_files_from_inep >> extract >> finish
