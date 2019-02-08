Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    SFTPTrivagoListDir,
    SFTPTrivagoDownload,
    GCSListDir,
    GCSUpload,
    BigQueryLoad
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime
import os
import logging
import pandas as pd


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

    logging.info('execution_date: {}, ftp_date: {}'.format(date, short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    short_date = short_date.replace('-', '')

    if not is_exist(short_date, sftp):
        raise RuntimeError('Not found file in SFTP: {}'.format(sftp))

    if is_exist(short_date, gcs):
        logging.warning('File already exist in GCS: {}', gcs)

    for k in sftp:
        if short_date in k:
            if 'v2' in k:
                return '/infiles/{}'.format(k)
    raise RuntimeError('Not found date in SFTP: {}'.format(short_date))


def _parse_date(inp):
    date_str = str(inp)
    return date_str[0:4] + '-' + date_str[4:6] + '-' + date_str[6:8]


def transform(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')
    tmp_file_path_gz = tmp_file_path + '.gz'

    date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')

    data = pd.read_csv(
        tmp_file_path,
        delimiter=';')
    data = data[data['Date'] == int(date.replace('-', ''))]
    data['Date'] = data['Date'].apply(_parse_date)
        
    data.to_csv(
        tmp_file_path_gz,
        sep=';',
        index=False,
        compression='gzip',
        encoding='utf-8')

    return tmp_file_path_gz


def generate_gcs_remote_path(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = tmp_file_path.split('/')[-1]
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    dates = short_date.split('-')
    return 'trivago/teb_performance_report/{}/{}/{}'.format(dates[0], dates[1], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'teb_performance_report_{}'.format(short_date.replace('-', ''))


def cleanup(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')
    tmp_file_path_gz = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(tmp_file_path)
    os.remove(tmp_file_path_gz)


dag = DAG(
    dag_id='trivago_teb_performance_report_v3',
    schedule_interval='40 5 * * *',
    start_date=datetime(2017, 10, 30),
    catchup=True,
    default_args={
        'retries': 0
    })

sftp_listdir_operation = SFTPTrivagoListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path='/infiles',
    startswith='hotelquickly_teb_performance_',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='trivago/teb_performance_report',
    startswith='hotelquickly_teb_performance_',
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
    dataset='metasearch_trivago_report_raw',
    table_xcom_task_id='generate_bigquery_table_name',
    schema=[SchemaField('date', 'DATE'),
            SchemaField('trylocale', 'STRING'),
            SchemaField('clicksexpress', 'INTEGER'),
            SchemaField('clickdefault', 'INTEGER'),
            SchemaField('bookingsdefault', 'INTEGER'),
            SchemaField('bookingsexpressviaexpressapi', 'INTEGER'),
            SchemaField('bookingsexpressviaexitpath', 'INTEGER'),
            SchemaField('bookingsexpress', 'INTEGER'),
            SchemaField('clicksviaexitpath', 'INTEGER')],
    gs_path_xcom_task_id='gcs_upload',
    source_format='CSV',
    field_delimiter=';',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

cleanup_operator = PythonOperator(
    task_id='cleanup',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=cleanup,
    provide_context=True,
    dag=dag)

sanity_check_operator << sftp_listdir_operation
sanity_check_operator << gcs_listdir_operation

transform_operator << sftp_download_operator << sanity_check_operator
gcs_upload_operator << generate_gcs_remote_path_operator << transform_operator
# noinspection PyStatementEffect
bigquery_load_operator << generate_bigquery_table_name_operator << gcs_upload_operator

cleanup_operator << gcs_upload_operator
