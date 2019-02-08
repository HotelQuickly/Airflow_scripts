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
    dag_id='daily_xsc_xsac_v2',
    schedule_interval=None, #'30 3 * * *',
    start_date=datetime(2017, 7, 12),
    catchup=True,
    default_args={
        'retries': 0
    })


with open('./scripts/daily_xsc_xsac/daily_tsc_tsac.sql') as f:
    daily_tsc_tsac = ' '.join(f.readlines())

with open('./scripts/daily_xsc_xsac/daily_isc_isac_per_source.sql') as f:
    daily_isc_isac_per_source = ' '.join(f.readlines())


daily_tsc_tsac_operator = BigQueryOperator(
    task_id='bigquery_load_daily_tsc_tsac',
    bql=daily_tsc_tsac,
    destination_dataset_table='analyst.daily_tsc_tsac_{{ ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)


daily_isc_isac_operator = BigQueryOperator(
    task_id='bigquery_load_daily_isc_isac',
    bql=daily_isc_isac_per_source,
    destination_dataset_table='analyst.daily_isc_isac_per_source_{{ ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)


daily_tsc_tsac_operator
daily_isc_isac_operator
