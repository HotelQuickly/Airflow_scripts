Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    GCSListDirDelimiter,
    BigQueryLoad,
    TriggerDagRunOperator
)

from google.cloud.bigquery.schema import SchemaField

from airflow.macros import datetime, timedelta
import logging


def is_exist(needle, exist_list):
    for key in exist_list:
        if needle in key:
            return True
    return False


def sanity_check(**context):
    gcs = context['task_instance'].xcom_pull(task_ids='gcs_listdir')

    date = context['execution_date']
    tomorrow_date = date + timedelta(days=1)

    short_date = date.strftime('%Y-%m-%d')
    tomorrow_short_date = tomorrow_date.strftime('%Y-%m-%d')

    logging.info('execution_date: {}, next_gcs_date: {}'.format(date, tomorrow_short_date))

    context['task_instance'].xcom_push(key='short_date', value=short_date)

    if not is_exist(tomorrow_short_date, gcs):
        raise RuntimeError('Directory is not exist in GCS: {}'.format(gcs))

    return 'gs://inventory-system_hotel-inventory-metric/{}/*'.format(short_date)


def generate_bigquery_table_name(**context):
    short_date = context['task_instance'].xcom_pull(task_ids='sanity_check', key='short_date')
    return 'search_{}'.format(short_date.replace('-', ''))


dag = DAG(
    dag_id='inventory_search_metric_v2',
    schedule_interval='30 0 * * *',
    start_date=datetime(2017, 7, 12),
    catchup=True,
    default_args={
        'retries': 0
    })

gcs_listdir_operation = GCSListDirDelimiter(
    task_id='gcs_listdir',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    bucket='inventory-system_hotel-inventory-metric',
    delimiter='/',
    dag=dag)

sanity_check_operator = PythonOperator(
    task_id='sanity_check',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=sanity_check,
    provide_context=True,
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
    dataset='inventory_raw',
    table_xcom_task_id='generate_bigquery_table_name',
    schema=[SchemaField('measured_datetime', 'TIMESTAMP'),
            SchemaField('hq_hotel_id', 'INTEGER'),
            SchemaField('consumer_code', 'STRING'),
            SchemaField('user_country', 'STRING'),
            SchemaField('device_type', 'STRING'),
            SchemaField('days_in_advances', 'INTEGER'),
            SchemaField('nights', 'INTEGER'),
            SchemaField('cop_HQ01', 'FLOAT'),
            SchemaField('cop_AGODAPARSER01', 'FLOAT'),
            SchemaField('cop_EXPEDIA01', 'FLOAT'),
            SchemaField('cop_DOTW01', 'FLOAT'),
            SchemaField('cop_GTA01', 'FLOAT'),
            SchemaField('cop_HOTELBEDS01', 'FLOAT'),
            SchemaField('cop_SITEMINDER01', 'FLOAT'),
            SchemaField('cop_ZUMATA01', 'FLOAT'),
            SchemaField('cop_GTA02', 'FLOAT'),
            SchemaField('cop_GTA03', 'FLOAT'),
            SchemaField('cgp_HQ01', 'FLOAT'),
            SchemaField('cgp_AGODAPARSER01', 'FLOAT'),
            SchemaField('cgp_EXPEDIA01', 'FLOAT'),
            SchemaField('cgp_DOTW01', 'FLOAT'),
            SchemaField('cgp_GTA01', 'FLOAT'),
            SchemaField('cgp_HOTELBEDS01', 'FLOAT'),
            SchemaField('cgp_SITEMINDER01', 'FLOAT'),
            SchemaField('cgp_ZUMATA01', 'FLOAT'),
            SchemaField('cgp_GTA02', 'FLOAT'),
            SchemaField('cgp_GTA03', 'FLOAT'),
            SchemaField('lookup_HQ01', 'BOOLEAN'),
            SchemaField('lookup_AGODAPARSER01', 'BOOLEAN'),
            SchemaField('lookup_EXPEDIA01', 'BOOLEAN'),
            SchemaField('lookup_DOTW01', 'BOOLEAN'),
            SchemaField('lookup_GTA01', 'BOOLEAN'),
            SchemaField('lookup_HOTELBEDS01', 'BOOLEAN'),
            SchemaField('lookup_SITEMINDER01', 'BOOLEAN'),
            SchemaField('lookup_ZUMATA01', 'BOOLEAN'),
            SchemaField('lookup_GTA02', 'BOOLEAN'),
            SchemaField('lookup_GTA03', 'BOOLEAN')],
    gs_path_xcom_task_id='sanity_check',
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_TRUNCATE',
    dag=dag)


def win_show_rate_trigger(context, dro):
    dro.execution_date = context['execution_date']
    return dro


def rategain_price_comp_trigger(context, dro):
    dro.execution_date = context['execution_date']
    return dro


def dataflow_search_trigger(context, dro):
    dro.execution_date = context['execution_date']
    return dro


win_show_rate_trigger_operator = TriggerDagRunOperator(
    task_id='trigger_win_show_rate',
    trigger_dag_id='win_show_rate',
    python_callable=win_show_rate_trigger,
    dag=dag)

rategain_price_comp_trigger_operator = TriggerDagRunOperator(
    task_id = 'trigger_rategain_price_comp',
    trigger_dag_id='rategain_price_comp',
    python_callable=rategain_price_comp_trigger,
    dag=dag
)

dataflow_search_trigger_operator = TriggerDagRunOperator(
    task_id='trigger_dataflow_search',
    trigger_dag_id='dataflow_search',
    python_callable=dataflow_search_trigger,
    dag=dag
)

generate_bigquery_table_name_operator << sanity_check_operator << gcs_listdir_operation
win_show_rate_trigger_operator << bigquery_load_operator << generate_bigquery_table_name_operator
rategain_price_comp_trigger_operator << bigquery_load_operator
dataflow_search_trigger_operator << bigquery_load_operator
