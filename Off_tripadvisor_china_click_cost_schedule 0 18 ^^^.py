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
    BigQueryLoad
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime, ds_add, ds_format
import pandas as pd
import numpy as np
import os
import logging
import csv

import math

sftp_path_directory = '/home/hotelquickly/ClickCostCNsite'

PARENT_DAG = 'tripadvisor_china_click_cost'

dag = DAG(
    dag_id=PARENT_DAG,
    schedule_interval='0 18 * * *',
    start_date=datetime(2018, 2, 27),
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
    short_date = short_date.replace('-', '_')

    logging.info('Execution date: {}, ftp date: {}'.format(date, short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    if not is_exist(short_date, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    if is_exist(short_date, gcs):
        logging.warning('File exists in gcs at this date: {}'. format(short_date))

    for k in sftp:
        if short_date in k:
            logging.info('File found: {}'.format(k))
            return '{}/{}'.format(sftp_path_directory, k)
    raise RuntimeError('Not found date in SFTP: {}'.format(short_date))


def _parse_number(number):
    try:
        num = int(number)
    except ValueError:
        num = 0
    return num


def is_nan(number):
    if math.isnan(number):
        return -1
    else: return int(number)


def transform(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')

    temp_file_path_zip = context['task_instance'].xcom_pull(task_ids='sftp_download')
    temp_file_path_gzip = 'ClickCost-V04-HotelQuickly-' + short_date + '.csv.gz'

    column_name = ['report_date', 'location_id', 'ta_location_id', 'provider_name', 'silo_name', 'bucket_name',
                   'revenue_share', 'cost_in_usd', 'clicks']

    df = pd.read_csv(temp_file_path_zip, names=column_name, compression='zip', delimiter=',', quoting=csv.QUOTE_NONE,
                     header=16)
    df = df[df['report_date'] == short_date.replace('_', '-')]

    df.drop(labels=['bucket_name', 'revenue_share'], axis=1, inplace=True)

    df['location_id'] = df['location_id'].apply(is_nan)
    df['ta_location_id'] = df['ta_location_id'].apply(is_nan)
    df['cost_in_usd'] = df['cost_in_usd'].apply(float)
    df['cost_in_usd'] = df['cost_in_usd'] / 100
    df['clicks'] = df['clicks'].apply(_parse_number)

    df.to_csv(temp_file_path_gzip, sep=',', quoting=csv.QUOTE_NONE, index=False, compression='gzip')

    return temp_file_path_gzip


def generate_gcs_remote_path(**context):
    temp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = temp_file_path.split('/')[-1]
    dates = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date').split('_')

    return 'tripadvisor/CHINA_CLICK_COST/{}/{}/{}'.format(dates[0], dates[1], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date').replace('_', '')
    return 'china_click_cost_{}'.format(short_date)


def cleanup(**context):
    temp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(temp_file_path)


sftp_listdir_operation = SFTPTripadvisorListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path=sftp_path_directory,
    startswith='ClickCost-V04-HotelQuickly-',
    dag=dag
)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='tripadvisor/CHINA_CLICK_COST',
    startswith='ClickCost-V04-HotelQuickly-',
    dag=dag
)

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
            SchemaField('location_id', 'INTEGER'),
            SchemaField('ta_location_id', 'INTEGER'),
            SchemaField('provider_name', 'STRING'),
            SchemaField('silo_name', 'STRING'),
            SchemaField('cost_in_usd', 'FLOAT'),
            SchemaField('clicks', 'INTEGER')],
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
