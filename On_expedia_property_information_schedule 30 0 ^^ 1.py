Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import MySqlHook, HttpHook
from sqlalchemy.engine.url import make_url
import pymysql.cursors
from datetime import datetime
import pandas as pd
import logging
import os


class PropertyDescriptionTable(object):
    def __init__(self):
        self._id = None
        self._language_code = None
        self._property_description = None

    @property
    def id(self):
        return self._id

    @property
    def language_code(self):
        return self._language_code

    @property
    def property_description(self):
        return self._property_description

    @id.setter
    def id(self, id):
        self._id = id

    @language_code.setter
    def language_code(self, language_code):
        self._language_code = language_code

    @property_description.setter
    def property_description(self, property_description):
        self._property_description = property_description


def get_list_of_expedia_language_codes(**context):
    url = 'PropertyDescriptionList{}.zip'
    urls = []
    urls.append(url.format(''))  # en_US

    for lang in ['ar_SA', 'cs_CZ', 'de_DE', 'es_ES', 'es_MX',
                 'fr_FR', 'fr_CA', 'hu_HU', 'in_ID', 'it_IT',
                 'ja_JP', 'ko_KR', 'ms_MY', 'nl_NL', 'no_NO',
                 'pl_PL', 'pt_BR', 'pt_PT', 'ru_RU', 'sv_SE',
                 'sk_SK', 'th_TH', 'tr_TR', 'uk_UA', 'vi_VN',
                 'zh_TW', 'zh_CN']:
        urls.append(url.format('_' + lang))
    return urls


def request_expedia_information(**context):
    urls = context['task_instance'].xcom_pull(
        task_ids='get_list_of_expedia_language_codes')
    dests = []

    temp_folder_name = 'tmp'
    if not os.path.exists(temp_folder_name):
        os.makedirs(temp_folder_name)

    for url in urls:
        api_hook = HttpHook(http_conn_id='expedia_static_url', method='GET')
        dest = os.path.join(temp_folder_name, url)
        logging.info(url)
        logging.info(dest)
        resp = api_hook.run(endpoint=url, extra_options={'timeout': 120})
        logging.info(resp.status_code)
        try:
            logging.info("Download property description file: {}".format(dest))
            with open(dest, 'wb') as fd:
                for chunk in resp.iter_content(chunk_size=128):
                    fd.write(chunk)
            dests.append(dest)
        except:
            logging.error('Cannot download property description file: {}'.format(dest))
            continue

    return dests


def update_hotel_data(row, conn):
    try:
        int(row['EANHotelID'])
    except:
        logging.warn('Missing hotel id for property description')
        return

    try:
        property_desc = PropertyDescriptionTable()
        property_desc.id = row['EANHotelID']
        property_desc.language_code = row['LanguageCode']
        property_desc.property_description = row['PropertyDescription'].encode('utf-8')

        if len(property_desc.property_description) == 0:
            return

        data = {
            'hotel_id': property_desc.id,
            'language_code': property_desc.language_code,
            'property_description': property_desc.property_description
        }

        cur = conn.cursor()
        query = """
        INSERT INTO expedia.lst_property_description
        (hotel_id, language_code, property_description, ins_dt, ins_process_id)
        VALUES (%(hotel_id)s, %(language_code)s, %(property_description)s, NOW(), 'airflow-expedia')
        ON DUPLICATE KEY UPDATE
        property_description=%(property_description)s, upd_process_id='airflow-expedia'
        """

        cur.execute(query, data)
        cur.close()
        conn.commit()
    except UnicodeEncodeError:
        logging.warn('Error unicode encode at hotel id: {}.'.format(row['EANHotelID']))
        return



def save_to_expedia_property_description(**context):
    file_paths = context['task_instance'].xcom_pull(
        task_ids='request_expedia_information')
    mysql_hook = MySqlHook(mysql_conn_id='expedia_database')
    mysql_uri = make_url(mysql_hook.get_uri())

    conn = pymysql.connect(host=mysql_uri.host,
                           user=mysql_uri.username,
                           password=mysql_uri.password,
                           db=mysql_uri.database,
                           charset='utf8', use_unicode=True)
    with conn:
        for file_path in file_paths:
            logging.info(file_path)
            data = pd.read_csv(file_path, sep='|', encoding='utf-8')
            data.apply(lambda x: update_hotel_data(x, conn), axis=1)

            os.remove(file_path)


dag = DAG(
    dag_id='expedia_property_information',
    schedule_interval='30 0 * * 1',
    start_date=datetime(2017, 9, 18),
    catchup=True,
    default_args={
        'retries': 0
    })

get_list_of_expedia_language_codes_operator = PythonOperator(
    task_id='get_list_of_expedia_language_codes',
    python_callable=get_list_of_expedia_language_codes,
    provide_context=True,
    dag=dag
)

request_expedia_information_operator = PythonOperator(
    task_id='request_expedia_information',
    python_callable=request_expedia_information,
    provide_context=True,
    dag=dag
)

save_to_expedia_property_description_operator = PythonOperator(
    task_id='save_to_expedia_lst_property_description_table',
    python_callable=save_to_expedia_property_description,
    provide_context=True,
    dag=dag
)

get_list_of_expedia_language_codes_operator >> request_expedia_information_operator

request_expedia_information_operator >> save_to_expedia_property_description_operator
