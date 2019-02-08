Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators import (
    SlackHQOperator,
    DataflowRun
)

from airflow.macros import datetime


def gen_cmd(partner):
    return 'docker-compose --project-name dataflow_{0}_hac_{{}} ' \
           'run --rm app_1_10 msiab {0} dataflow'.format(partner)


def gen_cleanup_cmd(partner):
    return 'docker-compose --project-name dataflow_{}_hac_{{}} ' \
           'down --rmi local'.format(partner)


dag = DAG(
    dag_id='dataflow_hac_legacy',
    schedule_interval='10 1 * * *',
    start_date=datetime(2017, 5, 19),
    catchup=False,
    default_args={
        'retries': 0
    })


def run_hac(partner):
    DataflowRun(
        task_id='{}_hac'.format(partner),
        pool='dataflow',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        cmd=gen_cmd(partner),
        cleanup_cmd=gen_cleanup_cmd(partner),
        dag=dag)


run_hac('hotelscombined')
run_hac('kayak')
run_hac('skyscanner')
run_hac('tripadvisor')
run_hac('trivago')
