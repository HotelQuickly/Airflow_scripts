Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators import (
    SlackHQOperator,
)

from airflow.macros import datetime

dag = DAG(
    dag_id='win_show_rate',
    schedule_interval=None,
    start_date=datetime(2017, 7, 12),
    catchup=False,
    default_args={
        'retries': 0
    })

with open('./scripts/win_show_rate/win_rate_across_source.sql') as f:
    win_rate_across_source_sql = ' '.join(f.readlines())

with open('./scripts/win_show_rate/win_rate_across_sources.sql') as f:
    win_rate_across_sources_sql = ' '.join(f.readlines())

with open('./scripts/win_show_rate/show_rate_across_source.sql') as f:
    show_rate_across_source_sql = ' '.join(f.readlines())

with open('./scripts/win_show_rate/show_rate_across_sources.sql') as f:
    show_rate_across_sources_sql = ' '.join(f.readlines())

win_rate_across_source = BigQueryOperator(
    task_id='win_rate_across_source',
    bql=win_rate_across_source_sql,
    destination_dataset_table='hqdatawarehouse.analyst.win_rate_across_source_{{ ds_nodash }}',
    write_disposition='WRITE_TRUNCATE',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

win_rate_across_sources = BigQueryOperator(
    task_id='win_rate_across_sources',
    bql=win_rate_across_sources_sql,
    destination_dataset_table='hqdatawarehouse.analyst.win_rate_across_sources_{{ ds_nodash }}',
    write_disposition='WRITE_TRUNCATE',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

show_rate_across_source = BigQueryOperator(
    task_id='show_rate_across_source',
    bql=show_rate_across_source_sql,
    destination_dataset_table='hqdatawarehouse.analyst.show_rate_across_source_{{ ds_nodash }}',
    write_disposition='WRITE_TRUNCATE',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

show_rate_across_sources = BigQueryOperator(
    task_id='show_rate_across_sources',
    bql=show_rate_across_sources_sql,
    destination_dataset_table='hqdatawarehouse.analyst.show_rate_across_sources_{{ ds_nodash }}',
    write_disposition='WRITE_TRUNCATE',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)
