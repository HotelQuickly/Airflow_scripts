Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators import (
    SlackHQOperator,
    GCSUpload,
    GCSDelete,
    BigQueryLoad,
    MySQLJsonBigQueryDump
)

from airflow.macros import datetime

import os
import logging


def _local_cleanup(table_name, **context):
    file_path = context['task_instance'].xcom_pull(key='file_path',
                                                   task_ids='{}_dump'.format(table_name))
    os.remove(file_path)


def dump(dag, database_name, table_name, bigquery_dataset=None, bigquery_table_name=None):
    if not bigquery_dataset:
        bigquery_dataset = database_name
    if not bigquery_table_name:
        bigquery_table_name = table_name

    def _cleanup(**context):
        _local_cleanup(table_name, **context)

    latest_only_operator = LatestOnlyOperator(
        task_id='{}_latest_only'.format(table_name),
        dag=dag)

    dump_operator = MySQLJsonBigQueryDump(
        task_id='{}_dump'.format(table_name),
        pool='alice_database_bi_export',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        conn_id='alice_database',
        table_name=table_name,
        database_name=database_name,
        dag=dag)

    gcs_upload_operator = GCSUpload(
        task_id='{}_gcs_upload'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        bucket='hq-airflow-workspace',
        local_path_xcom_key='file_path',
        local_path_xcom_task_id='{}_dump'.format(table_name),
        remote_dir='',
        content_type='application/gzip',
        dag=dag)

    bq_load_operator = BigQueryLoad(
        task_id='{}_bq_load'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        dataset=bigquery_dataset,
        table=bigquery_table_name,
        schema_xcom_task_id='{}_dump'.format(table_name),
        schema_xcom_key='schema',
        gs_path_xcom_task_id='{}_gcs_upload'.format(table_name),
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    local_cleanup_operation = PythonOperator(
        task_id='{}_local_cleanup'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        python_callable=_cleanup,
        provide_context=True,
        dag=dag)

    gcp_cleanup_operation = GCSDelete(
        task_id='{}_gcp_cleanup'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        bucket='hq-airflow-workspace',
        remote_path_xcom_task_id='{}_dump'.format(table_name),
        remote_path_xcom_key='filename',
        dag=dag)

    local_cleanup_operation << gcs_upload_operator << dump_operator << latest_only_operator
    gcp_cleanup_operation << bq_load_operator << local_cleanup_operation

    return gcp_cleanup_operation


dag_1d = DAG(
    dag_id='hqlive_export_bigquery_1d',
    schedule_interval='30 0 * * *',
    start_date=datetime(2017, 10, 9),
    catchup=False,
    default_args={
        'retries': 0
    })

dag_10m = DAG(
    dag_id='hqlive_export_bigquery_10m',
    schedule_interval='2,22,42 * * * *',
    start_date=datetime(2017, 10, 9),
    catchup=False,
    default_args={
        'retries': 0
    })

dump(dag_1d, 'bi_export', 'credit_card')
dump(dag_1d, 'bi_export', 'campaign')
# dump(dag_1d, 'bi_export', 'device')
dump(dag_1d, 'bi_export', 'fx_rate')
dump(dag_1d, 'bi_export', 'hotel')
dump(dag_1d, 'bi_export', 'lst_city')
dump(dag_1d, 'bi_export', 'lst_country')
dump(dag_1d, 'bi_export', 'lst_currency')
dump(dag_1d, 'bi_export', 'lst_destination')
order_operator = dump(dag_10m, 'bi_export', 'order')
dump(dag_1d, 'bi_export', 'order_voucher_detail')
dump(dag_1d, 'bi_export', 'user')
dump(dag_1d, 'bi_export', 'user_vouchers')
dump(dag_1d, 'bi_export', 'voucher')
dump(dag_1d, 'bi_export', 'chargeback')


biqquery_all_orders_view_dump = BigQueryOperator(
    task_id='all_orders_view_dump',
    bql='SELECT * FROM `hqdatawarehouse.analyst.all_orders_view`',
    destination_dataset_table='hqdatawarehouse.analyst.all_orders',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag_10m)

biqquery_all_orders_view_dump << order_operator

dump_metasearch_cost_view_latest_only_operator = LatestOnlyOperator(
    task_id='dump_metasearch_cost_view_latest_only',
    dag=dag_1d)

dump_metasearch_cost_view_operator = BigQueryOperator(
    task_id='dump_metasearch_cost_view',
    bql='SELECT * FROM `hqdatawarehouse.analyst.metasearch_cost_view`',
    destination_dataset_table='hqdatawarehouse.analyst.metasearch_cost',
    write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    bigquery_conn_id='hqdatawarehouse_bigquery',
    dag=dag_1d)

dump_metasearch_cost_view_operator << dump_metasearch_cost_view_latest_only_operator
