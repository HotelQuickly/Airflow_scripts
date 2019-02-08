Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    SFTPTripadvisorListDir,
    SFTPTripadvisorDownload,
    SFTPTripadvisorUpload,
    GCSListDir,
    GCSUpload,
    BigQueryLoad
)

from google.cloud.bigquery.schema import SchemaField
from airflow.macros import datetime, ds_add, ds_format
from static_feed import tripadvisor_static_feed

import pandas as pd
import os
import logging
import csv

sftp_path_directory = '/home/hotelquickly'
static_feed_start = 'hotelquickly-ta-'
hotel_bucket_start = 'hotel_bucket_'

PARENT_DAG = 'tripadvisor_static_feed'
dag = DAG(
    dag_id=PARENT_DAG,
    schedule_interval='0 2 * * *',
    start_date=datetime(2018, 5, 20),
    catchup=True,
    default_args={
        'retries': 0
    }
)


def is_exist(needle, exist_list):
    for key in exist_list:
        if needle in key:
            return True
    return False


def sanity_check(**context):
    sftp = context['task_instance'].xcom_pull(task_ids='sftp_listdir')
    # gcs = context['task_instance'].xcom_pull(task_ids='gcs_listdir')

    checking_date = context['execution_date'].strftime('%Y-%m-%d')

    short_date = ds_add(checking_date, 1)

    # date = context['execution_date']
    #
    # short_date = date.strftime('%Y-%m-%d')
    # short_date = short_date.replace('-', '')

    logging.info('execution_date: {}, current_date: {}'.format(checking_date, short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    if not is_exist(static_feed_start, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    # if is_exist(actual_date, gcs):
    #     logging.warning('File already exist in GCS: {}', gcs)

    for k in sftp:
        if static_feed_start in k:
            logging.info('{}'.format(k))
            return '{}/{}'.format(sftp_path_directory, k)
    raise RuntimeError('Not found file in SFTP')


def bucket_sanity_check(**context):
    sftp = context['task_instance'].xcom_pull(task_ids='bucket_sftp_listdir')

    if not is_exist(hotel_bucket_start, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    # if is_exist(actual_date, gcs):
    #     logging.warning('File already exist in GCS: {}', gcs)

    for k in sftp:
        if hotel_bucket_start in k:
            logging.info('{}'.format(k))
            return '{}/{}'.format(sftp_path_directory, k)
    raise RuntimeError('Not found file in SFTP')


def transform(**context):
    temp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')
    # temp_hotel_bucket_path = context['task_instance'].xcom_pull(task_ids='bucket_sftp_download')
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')

    logging.info('temp file path: {}'.format(temp_file_path))
    old_static_file = pd.read_csv(temp_file_path, delimiter=',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')
    # hotel_bucket_file = pd.read_csv(temp_hotel_bucket_path, delimiter=',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')

    new_file = tripadvisor_static_feed.get_final_list(old_static_file)

    new_file.to_csv(temp_file_path, sep=',', quoting=csv.QUOTE_MINIMAL, index=False, encoding='utf-8')

    sftp_dict = {'temp_file_path': temp_file_path, 'short_date': short_date}

    return sftp_dict


def bigquery_transform(**context):
    bq_dict = context['task_instance'].xcom_pull(task_ids='transform')
    bq = pd.read_csv(bq_dict['temp_file_path'], delimiter=',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')

    bq.drop(labels=['Property Type', 'Number of Rooms', 'URL', 'Bids', 'B2V', '[latitude]', '[longitude]'], axis=1,
            inplace=True)
    bq.columns = ['hotel_id', 'hotel_name', 'hotel_address', 'hotel_city', 'hotel_state', 'hotel_country', 'postcode',
                  'bmp', 'dd_desktop_bid', 'dd_mobile_bid', 'us_desktop_bmp', 'us_mobile_bmp', 'au_desktop_bmp',
                  'au_mobile_bmp']
    bq['postcode'] = bq['postcode'].astype(str)
    bq['feed_date'] = bq_dict['short_date']

    cols = bq.columns.tolist()
    cols = [cols[-1]] + cols[:-1]
    bq = bq[cols]

    bq.to_csv(bq_dict['temp_file_path'], sep=',', quoting=csv.QUOTE_MINIMAL, index=False, encoding='utf-8')

    return bq_dict['temp_file_path']


def generate_gcs_remote_path(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'tripadvisor/tripadvisor-{}.csv'.format(short_date.replace('-', ''))


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'tripadvisor_{}'.format(short_date.replace('-', ''))


def cleanup(**context):
    temp_file_path_dict = context['task_instance'].xcom_pull(task_ids='transform')

    temp_file_path = temp_file_path_dict['temp_file_path']
    # temp_hotel_bucket_path = temp_file_path_dict['temp_hotel_bucket_path']
    os.remove(temp_file_path)
    # os.remove(temp_hotel_bucket_path)


sftp_listdir_operation = SFTPTripadvisorListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path=sftp_path_directory,
    startswith=static_feed_start,
    dag=dag)

# bucket_sftp_listdir_operation = SFTPTripadvisorListDir(
#     task_id='bucket_sftp_listdir',
#     on_failure_callback=SlackHQOperator.on_failure_general_callback,
#     path=sftp_path_directory,
#     startswith=hotel_bucket_start,
#     dag=dag)

# gcs_listdir_operation = GCSListDir(
#     task_id='gcs_listdir',
#     on_failure_callback=SlackHQOperator.on_failure_general_callback,
#     bucket='hq-metasearch-report',
#     path='tripadvisor/MBL_SUMMARY',
#     startswith='Mbl_Summary_All_Ips-Hotelquickly-7-',
#     dag=dag)

sanity_check_operator = PythonOperator(
    task_id='sanity_check',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=sanity_check,
    provide_context=True,
    dag=dag)

# bucket_sanity_check_operator = PythonOperator(
#     task_id='bucket_sanity_check',
#     on_failure_callback=SlackHQOperator.on_failure_general_callback,
#     python_callable=bucket_sanity_check,
#     provide_context=True,
#     dag=dag)

sftp_download_operator = SFTPTripadvisorDownload(
    task_id='sftp_download',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path_xcom_task_id='sanity_check',
    dag=dag)

# bucket_sftp_download_operator = SFTPTripadvisorDownload(
#     task_id='bucket_sftp_download',
#     on_failure_callback=SlackHQOperator.on_failure_general_callback,
#     path_xcom_task_id='bucket_sanity_check',
#     dag=dag)

transform_operator = PythonOperator(
    task_id='transform',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=transform,
    provide_context=True,
    dag=dag)

bigquery_transform_operator = PythonOperator(
    task_id='bigquery_transform',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=bigquery_transform,
    provide_context=True,
    dag=dag)

sftp_upload_operator = SFTPTripadvisorUpload(
    task_id='sftp_upload',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path_xcom_task_id='transform',
    file_path='/home/hotelquickly/',
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
    bucket='affiliate-hotel-data',
    local_path_xcom_task_id='bigquery_transform',
    remote_dir_xcom_task_id='generate_gcs_remote_path',
    content_type='text/csv',
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
    dataset='static_feed',
    table_xcom_task_id='generate_bigquery_table_name',
    schema=[SchemaField('feed_date', 'DATE'),
            SchemaField('hotel_id', 'INTEGER'),
            SchemaField('hotel_name', 'STRING'),
            SchemaField('hotel_address', 'STRING'),
            SchemaField('hotel_city', 'STRING'),
            SchemaField('hotel_state', 'STRING'),
            SchemaField('hotel_country', 'STRING'),
            SchemaField('postcode', 'STRING'),
            SchemaField('bmp', 'STRING'),
            SchemaField('dd_desktop_bid', 'INTEGER'),
            SchemaField('dd_mobile_bid', 'STRING'),
            SchemaField('us_desktop_bmp', 'STRING'),
            SchemaField('us_mobile_bmp', 'STRING'),
            SchemaField('au_desktop_bmp', 'STRING'),
            SchemaField('au_mobile_bmp', 'STRING')],
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

sftp_listdir_operation >> sanity_check_operator >> sftp_download_operator
# sftp_download_operator >> bucket_sftp_listdir_operation
# bucket_sftp_listdir_operation >> bucket_sanity_check_operator >> bucket_sftp_download_operator
# bucket_sftp_download_operator >> transform_operator
sftp_download_operator >> transform_operator
transform_operator >> sftp_upload_operator
sftp_upload_operator >> bigquery_transform_operator
bigquery_transform_operator >> generate_gcs_remote_path_operator >> gcs_upload_operator
gcs_upload_operator >> generate_bigquery_table_name_operator >> bigquery_load_operator
bigquery_load_operator >> cleanup_operator
