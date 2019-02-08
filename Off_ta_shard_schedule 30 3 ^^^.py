Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.macros import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud.bigquery.schema import SchemaField

from airflow.operators import (
    SlackHQOperator
)


dag = DAG(
    dag_id='ta_shard',
    schedule_interval='30 3 * * *',
    start_date=datetime(2017, 10, 1),
    catchup=True,
    default_args={
        'retries': 0
    })


with open('./scripts/ta_click_cost/clickcost_shard.sql') as f:
    ta_shard = ' '.join(f.readlines())


ta_shard_operator = BigQueryOperator(
    task_id='bigquery_load_ta_shard',
    bql=ta_shard,
    destination_dataset_table='metasearch_tripadvisor_report_raw.click_cost_{{ ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

ta_shard_operator
