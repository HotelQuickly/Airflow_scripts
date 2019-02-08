Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    SFTPTripadvisorListDir,
    SFTPTripadvisorDownload,
    GCSListDir,
    GCSUpload,
    BigQueryLoad,
    TriggerDagRunOperator
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime
import os
import logging
import pandas as pd
import csv
import numpy as np


def is_exist(needle, exist_list):
    for key in exist_list:
        if needle in key:
            return True
    return False


sftp_path_directory = '/home/hotelquickly/BMP'


def sanity_check(**context):
    sftp = context['task_instance'].xcom_pull(task_ids='sftp_listdir')
    gcs = context['task_instance'].xcom_pull(task_ids='gcs_listdir')

    date = context['execution_date']

    short_date = date.strftime('%Y-%m-%d')

    logging.info('execution_date: {}, ftp_date: {}'.format(date, short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    short_date = short_date.replace('-', '_')

    if not is_exist(short_date, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    if is_exist(short_date, gcs):
        logging.warning('File already exist in GCS: {}', gcs)

    for k in sftp:
        if short_date in k:
            return '{}/{}'.format(sftp_path_directory, k)
    raise RuntimeError('Not found date in SFTP: {}'.format(short_date))


def _parse_int(inp):
    if np.isnan(inp):
        return 0
    return int(inp)


def transform(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')
    tmp_file_path_gz = '%s.gz' % tmp_file_path

    date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')

    data = pd.read_csv(tmp_file_path, delimiter=',', quoting=csv.QUOTE_NONE)
    data = data[data['ds'] == date]
    data['clicks'] = data['clicks'].apply(_parse_int)
    data['bookings'] = data['bookings'].apply(_parse_int)
    data.to_csv(tmp_file_path_gz, sep=',', quoting=csv.QUOTE_NONE, index=False, compression='gzip')

    return tmp_file_path_gz


def generate_gcs_remote_path(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = tmp_file_path.split('/')[-1]
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    dates = short_date.split('-')
    return 'tripadvisor/BMP_BUCKET/{}/{}/{}'.format(dates[0], dates[1], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'bmp_bucket_{}'.format(short_date.replace('-', ''))


def cleanup(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(tmp_file_path)
    os.remove(tmp_file_path.replace('.gz', ''))


dag = DAG(
    dag_id='tripadvisor_bmp_bucket_report',
    schedule_interval='30 14 * * *',
    start_date=datetime(2017, 9, 3),
    catchup=True,
    default_args={
        'retries': 0
    })

sftp_listdir_operation = SFTPTripadvisorListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path=sftp_path_directory,
    startswith='Bmp_External_Data_By_Bucket-Hotelquickly-7-',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='tripadvisor/MBP_BUCKET_SUMMARY',
    startswith='Bmp_External_Data_By_Bucket-Hotelquickly-7-',
    dag=dag)

sanity_check_operator = PythonOperator(
    task_id='sanity_check',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=sanity_check,
    provide_context=True,
    dag=dag)

sftp_download_operator = SFTPTripadvisorDownload(
    task_id='sftp_download',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path_xcom_task_id='sanity_check',
    dag=dag)

transform_operator = PythonOperator(
    task_id='transform',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=transform,
    provide_context=True,
    dag=dag)

generate_gcs_remote_path_operator = PythonOperator(
    task_id='generate_gcs_remote_path',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=generate_gcs_remote_path,
    provide_context=True,
    dag=dag)

gcs_upload_operator = GCSUpload(
    task_id='gcs_upload',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    local_path_xcom_task_id='transform',
    remote_dir_xcom_task_id='generate_gcs_remote_path',
    content_type='application/gzip',
    dag=dag)

generate_bigquery_table_name_operator = PythonOperator(
    task_id='generate_bigquery_table_name',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=generate_bigquery_table_name,
    provide_context=True,
    dag=dag)

bigquery_load_operator = BigQueryLoad(
    task_id='bigquery_load',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    dataset='metasearch_tripadvisor_report_raw',
    table_xcom_task_id='generate_bigquery_table_name',
    schema=[SchemaField('provider', 'STRING'),
            SchemaField('silo', 'STRING'),
            SchemaField('bucket', 'STRING'),
            SchemaField('ds', 'DATE'),
            SchemaField('clicks', 'INTEGER'),
            SchemaField('bookings', 'INTEGER'),
            SchemaField('spend_usd', 'FLOAT'),
            SchemaField('gross_booking_value_usd', 'FLOAT'),
            SchemaField('cvr', 'FLOAT'),
            SchemaField('gbv_per_spend', 'FLOAT'),
            SchemaField('avg_cpc_usd', 'FLOAT'),
            SchemaField('avg_gross_booking_value_usd', 'FLOAT'),
            SchemaField('gbv_per_spend_target', 'FLOAT')],
    gs_path_xcom_task_id='gcs_upload',
    write_disposition='WRITE_TRUNCATE',
    field_delimiter=',',
    source_format='CSV',
    skip_leading_rows=1,
    dag=dag)

cleanup_operator = PythonOperator(
    task_id='cleanup',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=cleanup,
    provide_context=True,
    dag=dag)


def ta_trivago_daily_report_data_trigger(context, dro):
    dro.execution_date = context['execution_date']
    return dro


ta_trivago_daily_report_data_trigger_operator = TriggerDagRunOperator(
    task_id='trigger_ta_trivago_daily_report_data',
    trigger_dag_id='ta_trivago_daily_report_data',
    python_callable=ta_trivago_daily_report_data_trigger,
    dag=dag)


sanity_check_operator << sftp_listdir_operation
sanity_check_operator << gcs_listdir_operation

transform_operator << sftp_download_operator << sanity_check_operator

gcs_upload_operator << generate_gcs_remote_path_operator << transform_operator
bigquery_load_operator << generate_bigquery_table_name_operator << gcs_upload_operator
ta_trivago_daily_report_data_trigger_operator << bigquery_load_operator

cleanup_operator << gcs_upload_operator
