Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.macros import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.operators import SlackHQOperator

dag = DAG(
    dag_id='bidding_avg_cop',
    schedule_interval='0 4 * * *',
    start_date=datetime(2018, 5, 2),
    catchup=True,
    default_args={
        'retries': 0
    })

with open('./scripts/bidding/minimum_cop_15.sql') as f:
    cop_15 = ' '.join(f.readlines())

with open('./scripts/bidding/minimum_cop_30.sql') as f:
    cop_30 = ' '.join(f.readlines())

with open('./scripts/bidding/minimum_cop_15_bins.sql') as f:
    cop_15_bins = ' '.join(f.readlines())

with open('./scripts/facebook_min_cgp_daily/min_price.sql') as f:
    min_price = ' '.join(f.readlines())

cop_15_operator = BigQueryOperator(
    task_id='bigquery_load_cop_15',
    bql=cop_15,
    destination_dataset_table='inventory.avg_cop_15_{{ tomorrow_ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

cop_30_operator = BigQueryOperator(
    task_id='bigquery_load_cop_30',
    bql=cop_30,
    destination_dataset_table='inventory.avg_cop_30_{{ tomorrow_ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

cop_15bins_operator = BigQueryOperator(
    task_id='bigquery_load_cop_15_bins',
    bql=cop_15_bins,
    destination_dataset_table='inventory.avg_cop_15_bins_{{ tomorrow_ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

min_price_operator = BigQueryOperator(
    task_id='bigquery_load_min_price',
    bql=min_price,
    destination_dataset_table='inventory.min_cgp_{{ ds_nodash }}',
    on_failure_callback=SlackHQOperator.on_failure_general_callback,
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='hqdatawarehouse_bigquery',
    use_legacy_sql=False,
    dag=dag)

cop_15_operator
cop_30_operator
cop_15bins_operator
min_price_operator
