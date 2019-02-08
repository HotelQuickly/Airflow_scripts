Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.operators import (
    SlackHQOperator
)

from airflow.macros import datetime

dag = DAG(
    dag_id='det_daily_top_hotels',
    schedule_interval='0 5 * * *',
    start_date=datetime(2018, 5, 24),
    catchup=True,
    default_args={
        'retries': 0
    })

dump_operator = BigQueryOperator(
    task_id='daily_hotel_dump',
    bql='SELECT * FROM hqdatawarehouse.ad_hoc_projects.det_daily_hotels',
    destination_dataset_table='hqdatawarehouse.ad_hoc_projects.det_check',
    write_disposition='WRITE_APPEND',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag
)

dump_operator
