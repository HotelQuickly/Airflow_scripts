Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    GCSUpload,
    MySQLCsvDump,
    SlackHQOperator
)

import pandas as pd
import csv
import os
from airflow.macros import datetime, timedelta


def _get_start_of_current_and_next_month(date):
    start_of_curr_month = date.date().replace(day=1)
    next_month = (start_of_curr_month + timedelta(days=40))
    start_of_next_month = next_month.replace(day=1)
    return start_of_curr_month, start_of_next_month


def generate_monthly_query():
    return 'WHERE date(m01_order_datetime_gmt0) >= \'2017-01-01\''


def transform(**context):
    date = context['execution_date']
    start_of_curr_month, start_of_next_month = _get_start_of_current_and_next_month(date)
    start_of_curr_month_str = start_of_curr_month.strftime('%Y-%m-%d')
    start_of_next_month_str = start_of_next_month.strftime('%Y-%m-%d')

    keys = ['p1', 'p2', 'p3']
    report_dict = {key: None for key in keys}
    for key in report_dict:
        local_path = context['task_instance'].xcom_pull(task_ids='dump_{}'.format(key), key='file_path')
        data = pd.read_csv(local_path)
        data.to_csv(local_path, index=False)
        report_dict[key] = data

    return report_dict['p1']


def cleanup(**context):
    local_path = context['task_instance'].xcom_pull(task_ids='dump_p1', key='file_path')
    #os.remove(local_path)


def generate_gcs_remote_path(**context):
    return 'query_p1.csv'


def run(dag_name, generate_date_operator):
    part = ['p1', 'p2', 'p3']
    for p in part:
        dump_operator = MySQLCsvDump(
            task_id='dump_{}'.format(p),
            pool='alice_database',
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            conn_id='alice_database',
            table_name='order_accounting_booking_report_{}'.format(p),
            database_name='finance_accounting_export',
            where_xcom_task_id='generate_query',
            dag=dag_name
        )

    transform_operator = PythonOperator(
        task_id='transform',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        provide_context=True,
        python_callable=transform,
        dag=dag_name
    )

    generate_gcs_remote_path_operator = PythonOperator(
        task_id='generate_gcs_remote_path',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        python_callable=generate_gcs_remote_path,
        provide_context=True,
        dag=dag)

    gcs_upload_operator = GCSUpload(
        task_id='gcs_upload',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        bucket='finance-report',
        local_path_xcom_task_id='transform',
        remote_dir_xcom_task_id='generate_gcs_remote_path',
        content_type='text/csv',
        dag=dag)

    cleanup_operator = PythonOperator(
        task_id='cleanup',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        provide_context=True,
        python_callable=cleanup,
        dag=dag_name
    )

    generate_date_operator >> dump_operator >> transform_operator >> generate_gcs_remote_path_operator >> gcs_upload_operator
    gcs_upload_operator >> cleanup_operator

    return dag_name


dag = DAG(
    dag_id='finance_report',
    schedule_interval='0 4 1 * *',
    start_date=datetime(2018, 7, 1),
    catchup=True,
    default_args={
        'retries': 0
    }
)


generate_date_operator = PythonOperator(
    task_id='generate_query',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    provide_context=True,
    python_callable=generate_monthly_query,
    dag=dag
)


run(dag, generate_date_operator)







#
#
# query_operator = PythonOperator(
#     task_id='get_monthly_query',
#     on_failure_callback=SlackHQOperator.on_failure_general_callback,
#     python_callable=generate_monthly_query,
#     provide_context=True,
#     dag=dag)
#

#
#
# query_operator >> generate_gcs_remote_path_operator >> gcs_upload_operator
