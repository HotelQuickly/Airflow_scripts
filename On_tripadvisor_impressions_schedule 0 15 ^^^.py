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
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime, ds_add, ds_format
import os
import logging
import pandas as pd
import numpy as np
import csv

sftp_path_directory = '/home/hotelquickly/Impressions'

PARENT_DAG = 'tripadvisor_impressions'
dag = DAG(
    dag_id = PARENT_DAG,
    schedule_interval='0 15 * * *',
    start_date=datetime(2018, 2, 10),
    catchup=True,
    default_args={
        'retries': 0
    })


def is_exist(needle, exist_list):
    for key in exist_list:
        if needle in key:
            return True
    return False


def sanity_check(**context):
    sftp = context['task_instance'].xcom_pull(task_ids='sftp_listdir')
    gcs = context['task_instance'].xcom_pull(task_ids='gcs_listdir')

    date = context['execution_date']

    short_date = date.strftime('%Y-%m-%d')
    short_date = ds_add(short_date, -1)

    logging.info('execution_date: {}, ftp_date: {}'.format(date, short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    short_date = short_date.replace('-', '_')

    if not is_exist(short_date, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    if is_exist(short_date, gcs):
        logging.warning('File already exist in GCS: {}', gcs)

    for k in sftp:
        if short_date in k:
            logging.info('{}'.format(k))
            return '{}/{}'.format(sftp_path_directory, k)
    raise RuntimeError('Not found date in SFTP: {}'.format(short_date))


def _parse_number(number):
    try:
        num = int(number)
    except ValueError:
        num = 0
    return num


def transform(**context):
    temp_file_path_gz = context['task_instance'].xcom_pull(task_ids='sftp_download')

    column_name = ['report_date', 'silo', 'provider_name', 'traffic_type', 'location_id', 'partner_id', 'clicks',
                   'costs', 'impressions', 'single_chevron_impressions']
    logging.info('temp file path: {}'.format(temp_file_path_gz))
    df = pd.read_csv(temp_file_path_gz, names=column_name, compression='gzip', delimiter=',', quoting=csv.QUOTE_NONE,
                     header=0)

    df['costs'] = df['costs'].apply(float)
    df['costs'] = df['costs']/100
    
    df['partner_id'] = df['partner_id'].apply(_parse_number)
    df['location_id'] = df['location_id'].apply(_parse_number)
    df['clicks'] = df['clicks'].apply(_parse_number)
    df['impressions'] = df['impressions'].apply(_parse_number)
    df['single_chevron_impressions'] = df['single_chevron_impressions'].apply(_parse_number)

    df.to_csv(temp_file_path_gz, sep=',', quoting=csv.QUOTE_NONE, index=False, compression='gzip')

    return temp_file_path_gz


def generate_gcs_remote_path(**context):
    temp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = temp_file_path.split('/')[-1]
    dates = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date').split('-')

    return 'tripadvisor/IMPRESSIONS/{}/{}/{}'.format(dates[0], dates[1], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    shorter_date = short_date.replace('-', '')
    return 'impressions_{}'.format(shorter_date)


def cleanup(**context):
    temp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(temp_file_path)


sftp_listdir_operation = SFTPTripadvisorListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path=sftp_path_directory,
    startswith='Hr_Impressions_V2_Zipped-Hotelquickly-0-',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='tripadvisor/IMPRESSIONS',
    startswith='Hr_Impressions_V2_Zipped-Hotelquickly-0-',
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
    schema=[SchemaField('report_date', 'DATE'),
            SchemaField('silo', 'STRING'),
            SchemaField('provider_name', 'STRING'),
            SchemaField('traffic_type', 'STRING'),
            SchemaField('location_id', 'INTEGER'),
            SchemaField('partner_id', 'INTEGER'),
            SchemaField('clicks', 'INTEGER'),
            SchemaField('costs', 'FLOAT'),
            SchemaField('impressions', 'INTEGER'),
            SchemaField('single_chevron_impressions', 'INTEGER')],
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

sftp_listdir_operation >> sanity_check_operator
gcs_listdir_operation >> sanity_check_operator

sanity_check_operator >> sftp_download_operator >> transform_operator

transform_operator >> generate_gcs_remote_path_operator >> gcs_upload_operator

gcs_upload_operator >> generate_bigquery_table_name_operator >> bigquery_load_operator

gcs_upload_operator >> cleanup_operator
