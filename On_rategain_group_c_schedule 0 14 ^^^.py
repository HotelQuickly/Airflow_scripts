Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    SFTPRategainListDir,
    SFTPRategainDownload,
    GCSListDir,
    GCSUpload,
    BigQueryLoad
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime, ds_add, ds_format
import os
import logging
import pandas as pd
import numpy as np
import csv
import datetime as dt
from datetime import timedelta

sftp_path_directory = '/hotelquickly'

PARENT_DAG = 'rategain_group_c'
dag = DAG(
    dag_id = PARENT_DAG,
    schedule_interval='0 14 * * *',
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
    
    short_date = context['execution_date'].strftime('%Y-%m-%d')
    short_date = ds_add(short_date, 1)
    
    checking_date = ds_format(short_date, '%Y-%m-%d', '%d_%B_%Y')
    
    logging.info('execution_date: {}, ftp_date: {}'.format(context['execution_date'], short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    if not is_exist(checking_date, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))
    
    for k in sftp:
        if checking_date in k:
            return '{}/{}'.format(sftp_path_directory, k)
    raise RuntimeError('Not found date in SFTP: {}'.format(checking_date))


def change_date_format(date_column):
    return ds_format(date_column, '%m/%d/%Y', '%Y-%m-%d')


def replace_string(string_name):
    return string_name.replace('"', '')

def transform(**context):
    temp_file_path_zip = context['task_instance'].xcom_pull(task_ids='sftp_download')
    temp_file_path = temp_file_path_zip.replace('.zip', '.csv.gz')

    column_name = ['website', 'country', 'city', 'checkin_date', 'LOS', 'occupancy', 'currency', 'hotel_name',
                   'room_type', 'board_type', 'availability', 'provider', 'rate', 'cache_page', 'hotel_id']

    df = pd.read_csv(temp_file_path_zip, names=column_name, compression='zip', delimiter=',', quoting=csv.QUOTE_MINIMAL,
                     header=0)

    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
                
    df.insert(0, 'report_date', short_date)
    
    df['checkin_date'] = df['checkin_date'].astype(str)
    df['checkin_date'] = df['checkin_date'].apply(replace_string)
    df['checkin_date'] = df['checkin_date'].apply(lambda x: ds_format(x, '%m/%d/%Y', '%Y-%m-%d'))
    df['checkin_date'] = df['checkin_date'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d'))
    
    df['LOS'] = df['LOS'].astype(str)
    df['LOS'] = df['LOS'].apply(replace_string)
    df['LOS'] = df['LOS'].astype(int)
    
    df['occupancy'] = df['occupancy'].astype(str)
    df['occupancy'] = df['occupancy'].apply(replace_string)
    df['occupancy'] = df['occupancy'].astype(int)
    
    df['rate'] = df['rate'].astype(str)
    df['rate'] = df['rate'].apply(replace_string)
    df['rate'] = df['rate'].astype(float)
    
    df['hotel_id'] = df['hotel_id'].astype(str)
    df['hotel_id'] = df['hotel_id'].apply(replace_string)
    df['hotel_id'] = df['hotel_id'].astype(int)

    df.to_csv(temp_file_path, sep=',', quoting=csv.QUOTE_MINIMAL, index=False, compression='gzip')
    
    return temp_file_path


def generate_gcs_remote_path(**context):
    temp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = temp_file_path.split('/')[-1]
    dates = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date').split('-')

    return 'group C/{}/{}/{}'.format(dates[0], dates[1], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'rategain_group_c_{}'.format(short_date.replace('-', ''))


def cleanup(**context):
    temp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(temp_file_path)


sftp_listdir_operation = SFTPRategainListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path=sftp_path_directory,
    startswith='Hotelquickly_C_',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='rategain-report',
    path='group C',
    startswith='Hotelquickly_C_',
    dag=dag)

sanity_check_operator = PythonOperator(
    task_id='sanity_check',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=sanity_check,
    provide_context=True,
    dag=dag)

sftp_download_operator = SFTPRategainDownload(
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
    bucket='rategain-report',
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
    dataset='rategain',
    table_xcom_task_id='generate_bigquery_table_name',
    schema=[SchemaField('report_date', 'DATE'),
            SchemaField('website', 'STRING'),
            SchemaField('country', 'STRING'),
            SchemaField('city', 'STRING'),
            SchemaField('checkin_date', 'DATE'),
            SchemaField('LOS', 'INTEGER'),
            SchemaField('occupancy', 'INTEGER'),
            SchemaField('currency', 'STRING'),
            SchemaField('hotel_name', 'STRING'),
            SchemaField('room_type', 'STRING'),
            SchemaField('board_type', 'STRING'),
            SchemaField('availability', 'STRING'),
            SchemaField('provider', 'STRING'),
            SchemaField('rate', 'FLOAT'),
            SchemaField('cache_page', 'STRING'),
            SchemaField('hotel_id', 'INTEGER')],
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
