Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

from airflow.operators import (
    SlackHQOperator,
)

from airflow.macros import datetime

dag = DAG(
    dag_id='ta_trivago_daily_report_data',
    schedule_interval=None,
    start_date=datetime(2017, 7, 12),
    catchup=False,
    default_args={
        'retries': 0
    })

latest_only_operator = LatestOnlyOperator(
    task_id='latest_show_rate_across_sources',
    dag=dag
)

dump_operator = BigQueryOperator(
    task_id='show_rate_across_sources',
    bql='SELECT * FROM hqdatawarehouse.analyst.ta_trivago_daily_report_data_view',
    destination_dataset_table='hqdatawarehouse.analyst.ta_trivago_daily_report_data',
    write_disposition='WRITE_TRUNCATE',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bigquery_conn_id='hqdatawarehouse_bigquery', use_legacy_sql=False,
    dag=dag
)

dump_operator << latest_only_operator
