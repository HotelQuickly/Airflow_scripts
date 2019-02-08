Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    MySQLCsvDump,
    FTPHotelscombinedUpload
)

import pandas as pd
from csv import QUOTE_MINIMAL
from airflow.macros import datetime, timedelta

import os

def generate_daily_query(**context):
    date = context['execution_date']
    short_date = date.strftime('%Y-%m-%d')
    context['task_instance'].xcom_push(
        key='remote_path', value='/hqk/HQK-daily-{}.csv'.format(short_date))
    return 'WHERE _DATE=\'{}\''.format(short_date)


def generate_monthly_query(**context):
    date = context['execution_date']
    start_of_curr_month, start_of_next_month = _get_start_of_current_and_next_month(date)
    start_of_curr_month_str = start_of_curr_month.strftime('%Y-%m-%d')
    start_of_next_month_str = start_of_next_month.strftime('%Y-%m-%d')

    context['task_instance'].xcom_push(
        key='remote_path', value='/hqk/HQK-monthly-{}.csv'.format(start_of_curr_month_str))
    return 'WHERE _DATE >= \'{}\' AND _DATE < \'{}\''.format(
        start_of_curr_month_str, start_of_next_month_str)


def _get_start_of_current_and_next_month(date):
    start_of_curr_month = date.date().replace(day=1)
    next_month = (start_of_curr_month + timedelta(days=40))
    start_of_next_month = next_month.replace(day=1)
    return start_of_curr_month, start_of_next_month


def transform(**context):
    local_path = context['task_instance'].xcom_pull(task_ids='dump', key='file_path')
    arr = ['ProviderCode','BookingID','ConversionID','BookedOn','CheckIn','CheckOut','TransactionType','TransactedOn','ReportingCurrencyCode','GrossBookingValueReportingCurrency','CommissionCollectedReportingCurrency','CommissionDueReportingCurrency','ClientCurrencyCode','GrossBookingValueClientCurrency','PropertyID','NumberOfRooms','IsNonRefundable','CancellationType','ClientIPAddress','ClientCountryCode']
    if os.stat(local_path).st_size == 0:
        local_path_file = open(local_path, "w")
        local_path_file.write(','.join(arr))
        local_path_file.close()

    data = pd.read_csv(local_path)
    if '_DATE' in data.columns:
        data = data.drop('_DATE', axis=1)
    data.replace({'null': ''}, inplace=True)
    data['BookingID'] = data['BookingID'].str.split(",", 1, expand=True)
    data = data[arr]
    data.to_csv(local_path, sep=',', quoting=QUOTE_MINIMAL, quotechar='"', index=False)
    return local_path


def cleanup(**context):
    local_path = context['task_instance'].xcom_pull(task_ids='dump', key='file_path')
    #os.remove(local_path)


def run(dag, generate_date_operator):
    dump_operator = MySQLCsvDump(
        task_id='dump',
        pool='alice_database',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        conn_id='alice_database',
        table_name='hotelscombined_egress_report',
        database_name='affiliate_export',
        where_xcom_task_id='generate_query',
        dag=dag)

    transform_operator = PythonOperator(
        task_id='transform',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        provide_context=True,
        python_callable=transform,
        dag=dag
    )

    ftp_upload_operator = FTPHotelscombinedUpload(
        task_id='upload',
        local_path_xcom_task_id='transform',
        remote_path_xcom_task_id='generate_query',
        remote_path_xcom_key_id='remote_path',
        dag=dag)

    cleanup_operator = PythonOperator(
        task_id='cleanup',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        provide_context=True,
        python_callable=cleanup,
        dag=dag
    )

    ftp_upload_operator << transform_operator << dump_operator << generate_date_operator
    cleanup_operator << ftp_upload_operator

    return dag


dag_daily = DAG(
    dag_id='hotelscombined_daily_egress_report',
    schedule_interval='30 2 * * *',
    start_date=datetime(2017, 10, 1),
    catchup=False,
    default_args={
        'retries': 0
    })

generate_daily_date_operator = PythonOperator(
    task_id='generate_query',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    provide_context=True,
    python_callable=generate_daily_query,
    dag=dag_daily
)

dag_monthly = DAG(
    dag_id='hotelscombined_monthly_egress_report_v2',
    schedule_interval='10 3 1 * *',
    start_date=datetime(2017, 10, 1),
    catchup=True,
    default_args={
        'retries': 0
    })

generate_monthly_date_operator = PythonOperator(
    task_id='generate_query',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    provide_context=True,
    python_callable=generate_monthly_query,
    dag=dag_monthly
)

run(dag_daily, generate_daily_date_operator)
run(dag_monthly, generate_monthly_date_operator)
