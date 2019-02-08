Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    SFTPTrivagoListDir,
    SFTPTrivagoDownload,
    S3ListDir,
    S3Upload,
    GCSListDir,
    GCSUpload,
    BigQueryLoad
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime
import os
import logging
import gzip
import pandas as pd


def is_exist(needle, exist_list):
    for key in exist_list:
        if needle in key:
            return True
    return False


def sanity_check(**context):
    sftp = context['task_instance'].xcom_pull(task_ids='sftp_listdir')
    gcs = context['task_instance'].xcom_pull(task_ids='gcs_listdir')
    s3 = context['task_instance'].xcom_pull(task_ids='s3_listdir')

    date = context['execution_date']

    short_date = date.strftime('%Y%m%d')

    logging.info('execution_date: {}, ftp_date: {}'.format(date, short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    if not is_exist(short_date, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    if is_exist(short_date, s3):
        logging.warning('File already exist in S3: {}', s3)

    if is_exist(short_date, gcs):
        logging.warning('File already exist in GCS: {}', gcs)

    for k in sftp:
        if short_date in k:
            return '/infiles/{}'.format(k)
    raise RuntimeError('Not found date in SFTP: {}'.format(short_date))


def transform(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')
    file_name = tmp_file_path.split('/')[-1]  # 1856_POS_report_20170817.csv.gz
    date = file_name[16:24]
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]

    with gzip.open(tmp_file_path, 'r') as fin:
        data = pd.read_csv(fin, delimiter=';', quotechar='"')
        data.insert(0, 'date', date)
        data.to_csv(tmp_file_path, sep=';', index=False, compression='gzip')

    return tmp_file_path


def generate_gcs_remote_path(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = tmp_file_path.split('/')[-1]
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'trivago/POS_report/{}/{}/{}'.format(short_date[0:4], short_date[4:6], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'pos_report_{}'.format(short_date)


def cleanup(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(tmp_file_path)


dag = DAG(
    dag_id='trivago_pos_report_v2',
    schedule_interval='30 8 * * *',
    start_date=datetime(2017, 8, 18),
    catchup=True,
    default_args={
        'retries': 0
    })

sftp_listdir_operation = SFTPTrivagoListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path='/infiles',
    startswith='1856_POS_report_',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='trivago/POS_report',
    startswith='1856_POS_report_',
    dag=dag)

s3_listdir_operation = S3ListDir(
    task_id='s3_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-data-extracts',
    path='metasearchSFTP/trivago/POS_REPORT/1856_POS_report_',
    dag=dag)

sanity_check_operator = PythonOperator(
    task_id='sanity_check',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=sanity_check,
    provide_context=True,
    dag=dag)

sftp_download_operator = SFTPTrivagoDownload(
    task_id='sftp_download',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path_xcom_task_id='sanity_check',
    dag=dag)

add_date_col_operator = PythonOperator(
    task_id='transform',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=transform,
    provide_context=True,
    dag=dag)

s3_upload_operator = S3Upload(
    task_id='s3_upload',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-data-extracts',
    local_path_xcom_task_id='transform',
    remote_dir='metasearchSFTP/trivago/POS_REPORT',
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
    dataset='metasearch_trivago_report_raw',
    table_xcom_task_id='generate_bigquery_table_name',
    schema=[SchemaField('date', 'DATETIME'),
            SchemaField('pos', 'STRING'),
            SchemaField('hotel_impr', 'INTEGER'),
            SchemaField('clicks', 'INTEGER'),
            SchemaField('cost', 'FLOAT'),
            SchemaField('avg_cpc', 'FLOAT'),
            SchemaField('top_pos_share', 'FLOAT'),
            SchemaField('impr_share', 'FLOAT'),
            SchemaField('outbid_ratio', 'FLOAT'),
            SchemaField('beat', 'FLOAT'),
            SchemaField('meet', 'FLOAT'),
            SchemaField('lose', 'FLOAT'),
            SchemaField('unavailability', 'FLOAT'),
            SchemaField('max_potential', 'INTEGER'),
            SchemaField('bookings', 'INTEGER'),
            SchemaField('booking_rate', 'FLOAT'),
            SchemaField('cpa', 'FLOAT'),
            SchemaField('gross_rev', 'FLOAT'),
            SchemaField('abv', 'FLOAT'),
            SchemaField('net_rev', 'FLOAT'),
            SchemaField('net_rev_click', 'FLOAT'),
            SchemaField('profit', 'FLOAT'),
            SchemaField('roas', 'FLOAT'),
            SchemaField('margin', 'FLOAT'),
            SchemaField('lps', 'FLOAT'),
            SchemaField('cpc_impact', 'FLOAT')],
    gs_path_xcom_task_id='gcs_upload',
    source_format='CSV',
    field_delimiter=';',
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

add_date_col_operator << sftp_download_operator << sanity_check_operator

s3_upload_operator << add_date_col_operator
gcs_upload_operator << generate_gcs_remote_path_operator << add_date_col_operator
bigquery_load_operator << generate_bigquery_table_name_operator << gcs_upload_operator

cleanup_operator << s3_upload_operator
cleanup_operator << gcs_upload_operator
