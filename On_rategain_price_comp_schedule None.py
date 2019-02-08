Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.macros import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.operators import SlackHQOperator

dag = DAG(
    dag_id='rategain_price_comp',
    schedule_interval=None,
    start_date=datetime(2018, 2, 27),
    catchup=True,
    default_args={
        'retries': 0
    })

with open('./scripts/rategain_price_comp/rategain_price_comp_sharded.sql') as f:
    price_comp = ' '.join(f.readlines())

price_comp_operator = BigQueryOperator(
    task_id='bigquery_load_price_comp',
    bql=price_comp,
    destination_dataset_table='rategain.daily_price_comp_{{ ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)
