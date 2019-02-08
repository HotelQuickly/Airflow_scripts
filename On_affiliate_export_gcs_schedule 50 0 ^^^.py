Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> import pandas as pd
import csv
import os
import sys
import logging

from airflow import DAG
from airflow.macros import datetime, ds_add, ds_format
from airflow.operators import (
    GCSUpload,
    MySQLCsvDump,
    S3Upload,
    SlackHQOperator,
    BigQueryLoad
)

from google.cloud.bigquery.schema import SchemaField
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from static_feed import trivago_static_feed, skyscanner_static_feed, hotelscombined_static_feed, kayak_static_feed

self_path = os.path.dirname(os.path.abspath("__file__"))

s3_affiliate_paths = {
    'hotel_data': 'affiliates/partners/unified/hotel_data.csv',
    'hotel_info': 'affiliates/partners/unified/hotel_info.csv',
    'hotel_photo': 'affiliates/partners/unified/hotel_photo.csv',
    'hotel_room': 'affiliates/partners/unified/hotel_room.csv',

    'airpay_hotel_amenities': 'affiliates/partners/airpay/airpay_hotel_amenities.csv',
    'airpay_hotel_amenities_info': 'affiliates/partners/airpay/airpay_hotel_amenities_info.csv',
    'airpay_hotel_data': 'affiliates/partners/airpay/airpay_hotel_data.csv',
    'airpay_hotel_photo': 'affiliates/partners/airpay/airpay_hotel_photo.csv',

    'gogobot_hotel_data': 'affiliates/partners/gogobot/gogobot_hotel_data.csv',

    'hotellook_hotel_photo': 'affiliates/partners/hotellook/hotellook_hotel_photo.csv',

    'hotelscombined_hotel_data': 'hotelscombined/hotel_data.csv',

    'kayak_hotel_data': 'affiliates/partners/kayak/kayak_hotel_data.csv',
    'kayak_direct_hotel': 'affiliates/partners/kayak/kayak_direct_hotel_data.csv',

    'momondo_hotel_amenities': 'affiliates/partners/momondo/momondo_hotel_amenities.csv',
    'momondo_hotel_amenities_info': 'affiliates/partners/momondo/momondo_hotel_amenities_info.csv',
    'momondo_hotel_data': 'affiliates/partners/momondo/momondo_hotel_data.csv',
    'momondo_hotel_image': 'affiliates/partners/momondo/momondo_hotel_image.csv',

    'skyscanner_hotel_data': 'affiliates/partners/skyscanner/skyscanner_hotel_data.csv',

    'tripadvisor_hotel_data': 'affiliates/partners/tripadvisor/tripadvisor_hotel_data.csv',

    'trivago_hotel_data': 'affiliates/partners/trivago/trivago_hotel_data.csv'
}


def _local_cleanup(table_name, **context):
    file_path = context['task_instance'].xcom_pull(key='file_path',
                                                   task_ids='{}_dump'.format(table_name))
    os.remove(file_path)


def dag_folder_cleanup(table_name):
    for item in os.listdir(self_path):
        if item.endswith(table_name + '.csv'):
            os.remove(item)


def python_dump(table_name, file_path=''):
    def _cleanup(**context):
        dag_folder_cleanup(table_name)

    def get_static_feed(**context):
        mse = table_name.split('_')[0]
        if mse == 'trivago':
            static_feed_df = trivago_static_feed.get_trivago_static_feed()
        elif mse == 'skyscanner':
            static_feed_df = skyscanner_static_feed.get_final_list()
        elif mse == 'hotelscombined':
            static_feed_df = hotelscombined_static_feed.get_hc_static_feed()
        elif mse == 'kayak':
            static_feed_df = kayak_static_feed.get_kayak_static_feed()
        static_feed_file = self_path + table_name + '.csv'
        static_feed_df.to_csv(static_feed_file, sep=',', quoting=csv.QUOTE_MINIMAL, index=False, encoding='utf-8')
        return static_feed_file

    def bigquery_transform(**context):
        logging.info('execution_date: {}'.format(context['execution_date']))
        checking_date = context['execution_date'].strftime('%Y-%m-%d')

        short_date = ds_add(checking_date, 1)
        context['task_instance'].xcom_push(key='short_date', value=short_date)

        static_feed_file = context['task_instance'].xcom_pull(task_ids='get_{}_static_feed'.
                                                              format(table_name.split('_')[0]))
        bq = pd.read_csv(static_feed_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')

        bq.drop(labels=['address2', 'country_id', 'longitude', 'latitude', 'phone', 'fax', 'web', 'email', 'city_id',
                        'currency'], axis=1,
                inplace=True)
        bq.columns = ['hotel_name', 'hotel_address', 'hotel_city', 'hotel_country', 'hotel_id', 'hotel_state',
                      'postcode']
        bq['postcode'] = bq['postcode'].astype(str)
        bq['feed_date'] = short_date

        cols = bq.columns.tolist()
        cols = [cols[-1]] + cols[:-1] #Make date the first column
        cols = [cols[0]] + [cols[5]] + cols[1:5] + cols[6:] #Make hotel_id the second column
        bq = bq[cols]

        bq.to_csv(static_feed_file, sep=',', quoting=csv.QUOTE_MINIMAL, index=False, encoding='utf-8')

        return static_feed_file

    def generate_gcs_remote_path(**context):
        short_date = context['task_instance'].xcom_pull(task_ids='{}_bigquery_transform'.
                                                        format(table_name.split('_')[0]), key='short_date')
        return 'trivago/trivago-{}.csv'.format(short_date.replace('-', ''))

    def generate_bigquery_table_name(**context):
        short_date = context['task_instance'].xcom_pull(task_ids='{}_bigquery_transform'.
                                                        format(table_name.split('_')[0]), key='short_date')
        return 'trivago_{}'.format(short_date.replace('-', ''))

    latest_only_operator = LatestOnlyOperator(
        task_id='{}_latest_only'.format(table_name),
        dag=dag)

    get_static_feed_operator = PythonOperator(
        task_id='get_{}_static_feed'.format(table_name.split('_')[0]),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        python_callable=get_static_feed,
        provide_context=True,
        dag=dag)

    if table_name.split('_')[0] == 'trivago':
        bigquery_transform_operator = PythonOperator(
            task_id='{}_bigquery_transform'.format(table_name.split('_')[0]),
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            python_callable=bigquery_transform,
            provide_context=True,
            dag=dag)

        generate_gcs_remote_path_operator = PythonOperator(
            task_id='generate_{}_gcs_remote_path'.format(table_name.split('_')[0]),
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            python_callable=generate_gcs_remote_path,
            provide_context=True,
            dag=dag)

        trivago_gcs_upload_operator = GCSUpload(
            task_id='{}_gcs_upload'.format(table_name.split('_')[0]),
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            bucket='affiliate-hotel-data',
            local_path_xcom_task_id='{}_bigquery_transform'.format(table_name.split('_')[0]),
            remote_dir_xcom_task_id='generate_{}_gcs_remote_path'.format(table_name.split('_')[0]),
            content_type='text/csv',
            dag=dag)

        generate_bigquery_table_name_operator = PythonOperator(
            task_id='generate_{}_bigquery_table_name'.format(table_name.split('_')[0]),
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            python_callable=generate_bigquery_table_name,
            provide_context=True,
            dag=dag)

        bigquery_load_operator = BigQueryLoad(
            task_id='{}_bigquery_load'.format(table_name.split('_')[0]),
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            dataset='static_feed',
            table_xcom_task_id='generate_{}_bigquery_table_name'.format(table_name.split('_')[0]),
            schema=[SchemaField('feed_date', 'DATE'),
                    SchemaField('hotel_id', 'INTEGER'),
                    SchemaField('hotel_name', 'STRING'),
                    SchemaField('hotel_address', 'STRING'),
                    SchemaField('hotel_city', 'STRING'),
                    SchemaField('hotel_country', 'STRING'),
                    SchemaField('hotel_state', 'STRING'),
                    SchemaField('postcode', 'STRING')],
            gs_path_xcom_task_id='{}_gcs_upload'.format(table_name.split('_')[0]),
            write_disposition='WRITE_TRUNCATE',
            field_delimiter=',',
            source_format='CSV',
            skip_leading_rows=1,
            dag=dag)

    gcs_upload_operator = GCSUpload(
        task_id='{}_gcs_upload'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        bucket='affiliate-hotel-data',
        local_path_xcom_task_id='get_{}_static_feed'.format(table_name.split('_')[0]),
        remote_path=file_path,
        content_type='text/csv',
        dag=dag)

    if table_name in s3_affiliate_paths:
        s3_upload_operator = S3Upload(
            task_id='{}_s3_upload'.format(table_name),
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            bucket='hq-data-extracts',
            local_path_xcom_task_id='get_{}_static_feed'.format(table_name.split('_')[0]),
            remote_path=s3_affiliate_paths[table_name],
            dag=dag)

    local_cleanup_operation = PythonOperator(
        task_id='{}_cleanup'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        python_callable=_cleanup,
        provide_context=True,
        dag=dag)

    gcs_upload_operator << get_static_feed_operator << latest_only_operator
    if table_name in s3_affiliate_paths:
        s3_upload_operator << get_static_feed_operator
        if table_name.split('_')[0] == 'trivago':
            bigquery_transform_operator << s3_upload_operator
            trivago_gcs_upload_operator << generate_gcs_remote_path_operator << bigquery_transform_operator
            bigquery_load_operator << generate_bigquery_table_name_operator << trivago_gcs_upload_operator
            local_cleanup_operation << bigquery_load_operator
        local_cleanup_operation << s3_upload_operator
    local_cleanup_operation << gcs_upload_operator


def mysql_dump(database_name, table_name, file_path=''):
    def _cleanup(**context):
        _local_cleanup(table_name, **context)

    latest_only_operator = LatestOnlyOperator(
        task_id='{}_latest_only'.format(table_name),
        dag=dag)

    dump_operator = MySQLCsvDump(
        conn_id='alice_database',
        table_name=table_name,
        database_name=database_name,
        header=False,
        batch_size=100000,
        task_id='{}_dump'.format(table_name),
        pool='alice_database',
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        dag=dag)

    gcs_upload_operator = GCSUpload(
        task_id='{}_gcs_upload'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        bucket='affiliate-hotel-data',
        local_path_xcom_key='file_path',
        local_path_xcom_task_id='{}_dump'.format(table_name),
        remote_path=file_path,
        content_type='text/csv',
        dag=dag)

    if table_name in s3_affiliate_paths:
        s3_upload_operator = S3Upload(
            task_id='{}_s3_upload'.format(table_name),
            on_failure_callback=SlackHQOperator.on_failure_general_callback,
            bucket='hq-data-extracts',
            local_path_xcom_key='file_path',
            local_path_xcom_task_id='{}_dump'.format(table_name),
            remote_path=s3_affiliate_paths[table_name],
            dag=dag)

    local_cleanup_operation = PythonOperator(
        task_id='{}_local_cleanup'.format(table_name),
        on_failure_callback=SlackHQOperator.on_failure_general_callback,
        python_callable=_cleanup,
        provide_context=True,
        dag=dag)

    gcs_upload_operator << dump_operator << latest_only_operator
    if table_name in s3_affiliate_paths:
        s3_upload_operator << dump_operator
        local_cleanup_operation << s3_upload_operator
    local_cleanup_operation << gcs_upload_operator


dag = DAG(
    dag_id='affiliate_export_gcs',
    schedule_interval='50 0 * * *',
    start_date=datetime(2017, 10, 16),
    catchup=False,
    default_args={
        'retires': 0
    }
)

mysql_dump('affiliate_export', 'mapping_works_data', 'mapping_works/hotel_data.csv')

# unified
mysql_dump('affiliate_export', 'hotel_data', 'unified/hotel_data.csv')
mysql_dump('affiliate_export', 'hotel_info', 'unified/hotel_info.csv')
mysql_dump('affiliate_export', 'hotel_photo', 'unified/hotel_photo.csv')
mysql_dump('affiliate_export', 'hotel_room', 'unified/hotel_room.csv')

# airpay
mysql_dump('affiliate_export', 'airpay_hotel_amenities', 'airpay/hotel_amenities.csv')
mysql_dump('affiliate_export', 'airpay_hotel_amenities_info', 'airpay/hotel_amenities_info.csv')
mysql_dump('affiliate_export', 'airpay_hotel_data', 'airpay/hotel_data.csv')
mysql_dump('affiliate_export', 'airpay_hotel_photo', 'airpay/hotel_photo.csv')

# gogobot
mysql_dump('affiliate_export', 'gogobot_hotel_data', 'gogobot/hotel_data.csv')

# hotellook
mysql_dump('affiliate_export', 'hotellook_hotel_photo', 'hotellook/hotel_photo.csv')

# hotelscombined
# mysql_dump('affiliate_export', 'hotelscombined_hotel_data', 'hotelscombined/hotel_data.csv')
python_dump('hotelscombined_hotel_data', 'hotelscombined/hotel_data.csv')

# kayak
# mysql_dump('affiliate_export', 'kayak_hotel_data', 'kayak/hotel_data.csv')
python_dump('kayak_hotel_data', 'kayak/hotel_data.csv')
mysql_dump('affiliate_export', 'kayak_direct_hotel', 'kayak/direct_hotel_data.csv')

# momondo
mysql_dump('affiliate_export', 'momondo_hotel_amenities', 'momondo/hotel_amenities.csv')
mysql_dump('affiliate_export', 'momondo_hotel_amenities_info', 'momondo/hotel_amenities_info.csv')
mysql_dump('affiliate_export', 'momondo_hotel_data', 'momondo/hotel_data.csv')
mysql_dump('affiliate_export', 'momondo_hotel_image', 'momondo/hotel_image.csv')

# skyscanner
# mysql_dump('affiliate_export', 'skyscanner_hotel_data', 'skyscanner/hotel_data.csv')
python_dump('skyscanner_hotel_data', 'skyscanner/hotel_data.csv')

# tripadvisor
# mysql_dump('affiliate_export', 'tripadvisor_hotel_data', 'tripadvisor/hotel_data.csv')

# trivago
# mysql_dump('affiliate_export', 'trivago_hotel_data', 'trivago/hotel_data.csv')
python_dump('trivago_hotel_data', 'trivago/hotel_data.csv')
