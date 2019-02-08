Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    SFTPTripadvisorListDir,
    SFTPTripadvisorDownload,
    S3ListDir,
    S3Upload,
    GCSListDir,
    GCSUpload,
    BigQueryLoad
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime, timedelta
import os
import logging
import pandas as pd
import numpy as np
import csv


def is_exist(needle, exist_list):
    for key in exist_list:
        if needle in key:
            return True
    return False


sftp_path_directory = '/home/hotelquickly/MBL'


def sanity_check(**context):
    sftp = context['task_instance'].xcom_pull(task_ids='sftp_listdir')
    gcs = context['task_instance'].xcom_pull(task_ids='gcs_listdir')
    s3 = context['task_instance'].xcom_pull(task_ids='s3_listdir')

    date = context['execution_date']

    yesterday_date = date - timedelta(days=1)

    short_date = yesterday_date.strftime('%Y-%m-%d')

    logging.info('execution_date: {}, ftp_date: {}'.format(date, short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    short_date = short_date.replace('-', '_')

    if not is_exist(short_date, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    if is_exist(short_date, s3):
        logging.warning('File already exist in S3: {}', s3)

    if is_exist(short_date, gcs):
        logging.warning('File already exist in GCS: {}', gcs)

    for k in sftp:
        if short_date in k:
            return '{}/{}'.format(sftp_path_directory, k)
    raise RuntimeError('Not found date in SFTP: {}'.format(short_date))


def parse_date(date_column):
    # 2017-05-23 -04:00
    return date_column[0:10]


def parse_win_loss(v):
    if v:
        return 1
    return 0


def transform(**context):
    tmp_file_path_gz = context['task_instance'].xcom_pull(task_ids='sftp_download')

    data = pd.read_csv(tmp_file_path_gz, compression='gzip', delimiter=',', quoting=csv.QUOTE_NONE)
    data['start_date'] = data['start_date'].apply(parse_date)
    data['end_date'] = data['end_date'].apply(parse_date)
    if data['ta_location_id'].dtype != np.int:
        data['ta_location_id'] = data['ta_location_id'].apply(int)
    if data['partner_location_id'].dtype != np.int:
        data['partner_location_id'] = data['partner_location_id'].apply(int)
    if data['adr'].dtype != np.float:
        data['adr'] = data['adr'].apply(float)
    if data['adr_lowest_delta'].dtype != np.float:
        data['adr_lowest_delta'] = data['adr_lowest_delta'].apply(float)
    if data['median_meta_pricing'].dtype != np.float:
        data['median_meta_pricing'] = data['median_meta_pricing'].apply(float)
    if data['win_loss'].dtype == np.bool:
        data['win_loss'] = data['win_loss'].apply(parse_win_loss)

    data.to_csv(tmp_file_path_gz, sep=',', quoting=csv.QUOTE_NONE, index=False, compression='gzip')

    return tmp_file_path_gz


def generate_gcs_remote_path(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = tmp_file_path.split('/')[-1]
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    dates = short_date.split('-')
    return 'tripadvisor/MEET_BEAT_LOSE_WITH_INSTANT_BOOK/{}/{}/{}'.format(
        dates[0], dates[1], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'meet_beat_loss_with_instant_book_{}'.format(short_date.replace('-', ''))


def cleanup(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(tmp_file_path)


dag = DAG(
    dag_id='tripadvisor_meet_beat_loss_with_instant_book_report',
    schedule_interval='0 14 * * *',
    start_date=datetime(2017, 7, 30),
    catchup=True,
    default_args={
        'retries': 0
    })

sftp_listdir_operation = SFTPTripadvisorListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path=sftp_path_directory,
    startswith='Meet_Beat_Lose_With_Instant_Book_Zipped-Hotelquickly-Usd-Desktop_Meta-7-',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='tripadvisor/MBL_SUMMARY',
    startswith='Meet_Beat_Lose_With_Instant_Book_Zipped-Hotelquickly-Usd-Desktop_Meta-7-',
    dag=dag)

s3_listdir_operation = S3ListDir(
    task_id='s3_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-data-extracts',
    path='metasearchSFTP/tripadvisor/MEET_BEAT_LOSE_WITH_INSTANT_BOOK/Meet_Beat_Lose_With_Instant_Book_Zipped-Hotelquickly-Usd-Desktop_Meta-7-',  # noqa
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

s3_upload_operator = S3Upload(
    task_id='s3_upload',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-data-extracts',
    local_path_xcom_task_id='transform',
    remote_dir='metasearchSFTP/tripadvisor/MEET_BEAT_LOSE_WITH_INSTANT_BOOK',
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
    schema=[SchemaField('start_date', 'DATE'),
            SchemaField('end_date', 'DATE'),
            SchemaField('commerce_country', 'STRING'),
            SchemaField('ta_location_id', 'INTEGER'),
            SchemaField('partner_location_id', 'INTEGER'),
            SchemaField('average_meta_rank', 'INTEGER'),
            SchemaField('check_in', 'DATE'),
            SchemaField('check_out', 'DATE'),
            SchemaField('average_num_guests', 'INTEGER'),
            SchemaField('adr', 'FLOAT'),
            SchemaField('adr_lowest_delta', 'FLOAT'),
            SchemaField('currency', 'STRING'),
            SchemaField('comp_within_one_dollar', 'INTEGER'),
            SchemaField('comp_one_dollar_cheaper', 'INTEGER'),
            SchemaField('comp_one_dollar_more', 'INTEGER'),
            SchemaField('comp_within_five_percent', 'INTEGER'),
            SchemaField('comp_five_percent_cheaper', 'INTEGER'),
            SchemaField('comp_five_percent_more', 'INTEGER'),
            SchemaField('median_meta_pricing', 'FLOAT'),
            SchemaField('not_available', 'INTEGER'),
            SchemaField('impression_indexed', 'FLOAT'),
            SchemaField('win_loss', 'INTEGER'),
            SchemaField('instant_book_status', 'INTEGER'),
            SchemaField('comp_within_one_percent', 'INTEGER'),
            SchemaField('comp_one_percent_cheaper', 'INTEGER'),
            SchemaField('comp_one_percent_more', 'INTEGER'),
            SchemaField('mbl', 'STRING')],
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

sanity_check_operator << sftp_listdir_operation
sanity_check_operator << gcs_listdir_operation
sanity_check_operator << s3_listdir_operation

transform_operator << sftp_download_operator << sanity_check_operator

gcs_upload_operator << generate_gcs_remote_path_operator << transform_operator
bigquery_load_operator << generate_bigquery_table_name_operator << gcs_upload_operator
s3_upload_operator << transform_operator

cleanup_operator << gcs_upload_operator
cleanup_operator << s3_upload_operator
