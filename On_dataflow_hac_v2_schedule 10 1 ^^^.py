Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators import (
    SlackHQOperator,
    DataflowRun
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.macros import datetime


def gen_cmd(partner):
    def _gen_cmd(context):
        return 'docker-compose --project-name dataflow_{0}_hac_{{}} ' \
               'run --rm app msiab {0} {1} dataflow'.format(partner, context['ds'])
    return _gen_cmd


def gen_cleanup_cmd(partner):
    return 'docker-compose --project-name dataflow_{}_hac_{{}} ' \
           'down --rmi local'.format(partner)


dag = DAG(
    dag_id='dataflow_hac_v2',
    schedule_interval='10 1 * * *',
    start_date=datetime(2017, 5, 19),
    catchup=True,
    default_args={
        'retries': 1
    })


def run_hac(partner):
    return DataflowRun(
        task_id='{}_hac'.format(partner),
        pool='dataflow',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        cmd=gen_cmd(partner),
        cleanup_cmd=gen_cleanup_cmd(partner),
        dag=dag)


biqquery_log_booking_view_dump = BigQueryOperator(
    task_id='log_booking_view_dump',
    bql='SELECT * FROM `hqdatawarehouse.metasearch.log_booking_view`',
    destination_dataset_table='hqdatawarehouse.metasearch.log_booking',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

biqquery_log_availability_view_dump = BigQueryOperator(
    task_id='log_availability_view_view_dump',
    bql='SELECT * FROM `hqdatawarehouse.metasearch.log_availability_view`',
    destination_dataset_table='hqdatawarehouse.metasearch.log_availability',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)


biqquery_log_availability_view_dump << run_hac('hotelscombined')
biqquery_log_availability_view_dump << run_hac('kayak')
biqquery_log_availability_view_dump << run_hac('skyscanner')
biqquery_log_availability_view_dump << run_hac('tripadvisor')
biqquery_log_availability_view_dump << run_hac('trivago')
biqquery_log_availability_view_dump << run_hac('hotellook')
biqquery_log_availability_view_dump << run_hac('hotelquickly-website')
biqquery_log_availability_view_dump << run_hac('gogobot')
biqquery_log_booking_view_dump << biqquery_log_availability_view_dump
