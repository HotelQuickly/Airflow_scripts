Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    SlackHQOperator,
    DataflowRun,
    TriggerDagRunOperator
)

from airflow.macros import datetime
import logging


dag = DAG(
    dag_id='dataflow_search',
    schedule_interval=None, #'0 2 * * *',
    start_date=datetime(2017, 7, 12),
    catchup=True,
    default_args={
        'retries': 0
    })


def gen_cmd(**context):
    date = context['execution_date']
    short_date = date.strftime('%Y-%m-%d')

    cmd = 'docker-compose --project-name dataflow_xsac_{{}} run --rm app xsac {} dataflow' \
        .format(short_date)
    cleanup_cmd = 'docker-compose --project-name dataflow_xsac_{} down --rmi local'

    context['task_instance'].xcom_push(key='cmd', value=cmd)
    context['task_instance'].xcom_push(key='cleanup_cmd', value=cleanup_cmd)


def xsc_xsac_trigger(context, dro):
    dro.execution_date = context['execution_date']
    return dro

gen_cmd_operator = PythonOperator(
    task_id='gen_cmd',
    provide_context=True,
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    python_callable=gen_cmd,
    dag=dag)

xsac_operation = DataflowRun(
    task_id='xsac',
    pool='dataflow',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    cmd_xcom_task_id='gen_cmd',
    cleanup_cmd_xcom_task_id='gen_cmd',
    dag=dag)

xsc_xsac_trigger_operator = TriggerDagRunOperator(
    task_id='trigger_xsc_xsac',
    trigger_dag_id='daily_xsc_xsac_v2',
    python_callable=xsc_xsac_trigger,
    dag=dag
)

xsc_xsac_trigger_operator << xsac_operation << gen_cmd_operator
