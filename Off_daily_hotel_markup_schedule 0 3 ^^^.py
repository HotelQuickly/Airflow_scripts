Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.macros import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.operators import SlackHQOperator

dag = DAG(
    dag_id='daily_hotel_markup',
    schedule_interval='0 3 * * *',
    start_date=datetime(2018, 5, 2),
    catchup=True,
    default_args={
        'retries': 0
    })

with open('./scripts/markup/hotel_markup.sql') as f:
    daily_markup = ' '.join(f.readlines())

price_comp_operator = BigQueryOperator(
    task_id='bigquery_load_daily_markup',
    bql=daily_markup,
    destination_dataset_table='rategain.dynamic_markup_{{ tomorrow_ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)
