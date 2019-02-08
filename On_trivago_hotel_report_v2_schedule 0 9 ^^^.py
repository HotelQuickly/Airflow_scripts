Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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
            return '/infiles/{}'.format(k)
    raise RuntimeError('Not found date in SFTP: {}'.format(short_date))


def _parse_bookings(bookings):
    try:
        num = int(bookings)
    except ValueError:
        num = 0
    return num


def _parse_stars_rating(stars):
    try:
        num = int(stars)
    except ValueError:
        num = -1
    return num


def transform(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')
    file_name = tmp_file_path.split('/')[-1]  # 1856_Hotel_report_20171001.csv.gz
    date = file_name[18:26]
    date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]

    data = pd.read_csv(
        tmp_file_path,
        compression='gzip',
        delimiter=';',
        quotechar='"')
    data['Bookings'] = data['Bookings'].apply(_parse_bookings)
    data['Stars'] = data['Stars'].apply(_parse_stars_rating)
    data['Rating'] = data['Rating'].apply(_parse_stars_rating)
    data.insert(0, 'date', date)
    data.to_csv(
        tmp_file_path,
        sep=';',
        index=False,
        compression='gzip',
        encoding='utf-8')

    return tmp_file_path


def generate_gcs_remote_path(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    file_name = tmp_file_path.split('/')[-1]
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    dates = short_date.split('-')
    return 'trivago/Hotel_report/{}/{}/{}'.format(dates[0], dates[1], file_name)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'hotel_report_{}'.format(short_date.replace('-', ''))


def cleanup(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='transform')
    os.remove(tmp_file_path)


dag = DAG(
    dag_id='trivago_hotel_report_v2',
    schedule_interval='0 9 * * *',
    start_date=datetime(2017, 3, 27),
    catchup=True,
    default_args={
        'retries': 0
    })

sftp_listdir_operation = SFTPTrivagoListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path='/infiles',
    startswith='1856_Hotel_report_',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='trivago/Hotel_report',
    startswith='1856_Hotel_report_',
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
            SchemaField('hotel_name', 'STRING'),
            SchemaField('partner_ref', 'STRING'),
            SchemaField('pos', 'STRING'),
            SchemaField('bid_cpc', 'FLOAT'),
            SchemaField('status', 'STRING'),
            SchemaField('opportunity_cpc', 'FLOAT'),
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
            SchemaField('booking_amount_per_click_index', 'STRING'),
            SchemaField('net_rev', 'FLOAT'),
            SchemaField('net_rev_click', 'FLOAT'),
            SchemaField('profit', 'FLOAT'),
            SchemaField('roas', 'FLOAT'),
            SchemaField('margin', 'FLOAT'),
            SchemaField('last_pushed', 'STRING'),
            SchemaField('region', 'STRING'),
            SchemaField('country', 'STRING'),
            SchemaField('city', 'STRING'),
            SchemaField('stars', 'INTEGER'),
            SchemaField('rating', 'INTEGER'),
            SchemaField('trivago_id', 'STRING'),
            SchemaField('trivago_url', 'STRING')],
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

dump_metasearch_cost_view_operator = BigQueryOperator(
    task_id='dump_metasearch_cost_view',
    bql='SELECT * FROM `hqdatawarehouse.analyst.metasearch_cost_view`',
    destination_dataset_table='hqdatawarehouse.analyst.metasearch_cost',
    write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    bigquery_conn_id='hqdatawarehouse_bigquery',
    dag=dag
)

sanity_check_operator << sftp_listdir_operation
sanity_check_operator << gcs_listdir_operation

transform_operator << sftp_download_operator << sanity_check_operator
gcs_upload_operator << generate_gcs_remote_path_operator << transform_operator
bigquery_load_operator << generate_bigquery_table_name_operator << gcs_upload_operator

dump_metasearch_cost_view_operator << bigquery_load_operator

cleanup_operator << gcs_upload_operator
