Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    SFTPTripadvisorListDir,
    SFTPTripadvisorDownload,
    GCSListDir,
    GCSListUpload,
    BigQueryBulkLoad
)

from airflow.macros import ds_add, ds_format, datetime
import os
import csv
import pandas as pd
from google.cloud.bigquery.schema import SchemaField
import logging

sftp_path_directory = '/home/hotelquickly/ClickCost'
PARENT_DAG = 'tripadvisor_click_cost'
dag = DAG(
    dag_id = PARENT_DAG,
    schedule_interval='0 18 * * 0',
    start_date=datetime(2018, 1, 7),
    catchup=True,
    default_args={
        'retries': 0
    })


def sanity_check(**context):
    def is_exist(needle, exist_list):
        for key in exist_list:
            if needle in key:
                return True
        return False

    sftp = context['task_instance'].xcom_pull(task_ids='sftp_listdir')
    short_date = context['task_instance'].xcom_pull(task_ids='get_start_date')
    context['task_instance'].xcom_push(key='short_date', value=short_date)

    if len(sftp) == 0:
        raise RuntimeError('Not found file in SFTP for date: {} in {}'.format(short_date, sftp))
    else:
        return '{}/{}'.format(sftp_path_directory, sftp[0])


def transform(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')

    weekly_columns = ["#", "date", "locationid", "talocationid", "provider_name", "silo_name",
                      "cost in USD", "clicks"]
    weekly_columns_to_drop = ['#']
    daily_columns = ["#", "date", "locationid", "talocationid", "provider_name", "silo_name", "bucket_name",
                     "Revenue Share", "cost in USD", "clicks", "##"]
    daily_columns_to_drop = ['#', 'bucket_name', 'Revenue Share', '##']
    try:
        report = pd.read_csv(tmp_file_path,
                             delimiter=',',
                             names=weekly_columns,
                             engine='python')
        report.drop(labels=weekly_columns_to_drop, axis=1, inplace=True)
    except BaseException as e:
        report = pd.read_csv(tmp_file_path,
                             delimiter=',',
                             names=daily_columns,
                             engine='python')
        report.drop(labels=daily_columns_to_drop, axis=1, inplace=True)

    report = report.loc[17:]
    file_prefix = '/tmp/ClickCost-V04-HotelQuickly-'
    files_dict = dict()
    dates = report['date'].unique()
    for dt in dates:
        data = report.loc[report['date'] == dt]
        file = "{}{}.csv".format(file_prefix, dt)
        data.to_csv(file, sep=',', quoting=csv.QUOTE_NONE, index=False, compression='gzip')
        logging.info("Saving file: {} with rows: {}".format(file, data.shape[0]))
        files_dict[dt] = {'file_path': file}
    context['task_instance'].xcom_push(key='files_dict', value=files_dict)
    context['task_instance'].xcom_push(key='dates', value=dates)
    return files_dict


def generate_gcs_remote_path_and_bq_table_name(**context):
    bucket = 'hq-metasearch-report'
    dates = context['task_instance'].xcom_pull(key='dates', task_ids='transform')
    file_paths = context['task_instance'].xcom_pull(key='files_dict', task_ids='transform')
    for dt in dates:
        dts = dt.split('-')
        file_name = file_paths[dt]['file_path'].split('/')[-1]
        remote_path = 'tripadvisor/CLICK_COST/{}/{}/{}'.format(dts[0], dts[1], file_name)
        bq_table_name = 'hotel_click_cost_{}'.format(dt.replace('-', ''))
        file_paths[dt]['remote_path'] = remote_path
        file_paths[dt]['bq_table_name'] = bq_table_name
        file_paths[dt]['gs_path'] = 'gs://{}/{}'.format(bucket, remote_path)
    context['task_instance'].xcom_push(key='files_dict', value=file_paths)


def cleanup(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='sftp_download')
    os.remove(tmp_file_path)
    file_paths = context['task_instance'].xcom_pull(key='files_dict', task_ids='transform')
    for dt, file in file_paths.items():
        local_file = file['file_path']
        logging.info('Removing file: {} for date: {}'.format(local_file, dt))
        os.remove(local_file)


def get_start_date(**context):
    short_date = ds_format(ds_add(context['execution_date'].strftime('%Y-%m-%d'), -7), '%Y-%m-%d', '%Y_%m_%d')
    logging.info('TA Report stars with date: {}'.format(short_date))
    return short_date


startswith_operator = PythonOperator(
    task_id='get_start_date',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=get_start_date,
    provide_context=True,
    dag=dag)

sftp_listdir_operation = SFTPTripadvisorListDir(
    task_id='sftp_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    path=sftp_path_directory,
    xcom_task_id='get_start_date',
    startswith='ClickCost-V04-HotelQuickly-',
    dag=dag)

gcs_listdir_operation = GCSListDir(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='hq-metasearch-report',
    path='tripadvisor/CLICK_COST',
    startswith='ClickCost-V04-HotelQuickly',
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


gen_bq_table_name_and_remote_path_operator = PythonOperator(
    task_id='generate_bq_table_name_and_remote_path',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=generate_gcs_remote_path_and_bq_table_name,
    provide_context=True,
    dag=dag)

gcs_upload_operator = GCSListUpload(
        task_id='gcs_upload',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        bucket='hq-metasearch-report',
        files_list_xcom_task_id='generate_bq_table_name_and_remote_path',
        dates_xcom_task_id='transform',
        content_type='application/gzip',
        dag=dag)


bigquery_load_operator = BigQueryBulkLoad(
    task_id='bigquery_load',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    dataset='metasearch_tripadvisor_report_raw',
    schema=[SchemaField('date', 'DATE'),
            SchemaField('locationid', 'INTEGER'),
            SchemaField('talocationid', 'INTEGER'),
            SchemaField('provider_name', 'STRING'),
            SchemaField('silo_name', 'STRING'),
            SchemaField('cost_in_USD', 'INTEGER'),
            SchemaField('clicks', 'INTEGER')],
    gs_path_xcom_task_id='generate_bq_table_name_and_remote_path',
    dates_xcom_task_id='transform',
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

startswith_operator >> sftp_listdir_operation >> gcs_listdir_operation >> sanity_check_operator
sanity_check_operator >> sftp_download_operator >> transform_operator
transform_operator >> gen_bq_table_name_and_remote_path_operator
gen_bq_table_name_and_remote_path_operator >> gcs_upload_operator
gcs_upload_operator >> bigquery_load_operator >> cleanup_operator
