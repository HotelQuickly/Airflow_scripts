Python 3.6.7rc1 (v3.6.7rc1:311101f7b6, Sep 26 2018, 16:33:24) [MSC v.1900 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.http_hook import HttpHook
from contextlib import closing
from sqlalchemy.engine.url import make_url
import pymysql.cursors
from airflow.macros import datetime
import pandas as pd
import time
import hashlib

import csv
import uuid
import logging
import json
import os


class HotelTable(object):
    def __init__(self):
        self._id = None
        self._name = None
        self._address1 = None
        self._address2 = None
        self._city = None
        self._state_province = None
        self._postal_code = None
        self._country_code = None
        self._latitude = None
        self._longitude = None
        self._airport_code = None
        self._property_category = None
        self._currency_code = None
        self._star_rating = None
        self._location = None
        self._checkin_time = None
        self._checkout_time = None

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def address1(self):
        return self._address1

    @property
    def address2(self):
        return self._address2

    @property
    def city(self):
        return self._city

    @property
    def state_province(self):
        return self._state_province

    @property
    def postal_code(self):
        return self._postal_code

    @property
    def country_code(self):
        return self._country_code

    @property
    def latitude(self):
        return self._latitude

    @property
    def longitude(self):
        return self._longitude

    @property
    def airport_code(self):
        return self._airport_code

    @property
    def property_category(self):
        return self._property_category

    @property
    def currency_code(self):
        return self._currency_code

    @property
    def star_rating(self):
        return self._star_rating

    @property
    def location(self):
        return self._location

    @property
    def checkin_time(self):
        return self._checkin_time

    @property
    def checkout_time(self):
        return self._checkout_time

    @id.setter
    def id(self, id):
        self._id = id

    @name.setter
    def name(self, name):
        self._name = name

    @address1.setter
    def address1(self, address):
        self._address1 = address

    @address2.setter
    def address2(self, address):
        self._address2 = address

    @city.setter
    def city(self, city):
        self._city = city

    @state_province.setter
    def state_province(self, state_province):
        self._state_province = state_province

    @postal_code.setter
    def postal_code(self, postal_code):
        self._postal_code = postal_code

    @country_code.setter
    def country_code(self, country_code):
        self._country_code = country_code

    @latitude.setter
    def latitude(self, latitude):
        self._latitude = latitude

    @longitude.setter
    def longitude(self, longitude):
        self._longitude = longitude

    @airport_code.setter
    def airport_code(self, airport_code):
        self._airport_code = airport_code

    @property_category.setter
    def property_category(self, property_category):
        self._property_category = property_category

    @currency_code.setter
    def currency_code(self, currency_code):
        self._currency_code = currency_code

    @star_rating.setter
    def star_rating(self, star_rating):
        self._star_rating = star_rating

    @location.setter
    def location(self, location):
        self._location = location

    @checkin_time.setter
    def checkin_time(self, checkin_time):
        self._checkin_time = checkin_time

    @checkout_time.setter
    def checkout_time(self, checkout_time):
        self._checkout_time = checkout_time


def signature(apiKey, secretKey):
    currTime = str(int(time.time()))
    cid = apiKey + secretKey + currTime
    h = hashlib.md5(cid.encode('utf-8')).hexdigest()
    return h


def get_paired_loaded_hq_hotels():
    query = """
    select
    ex.source_code as expedia_id
    from ihub.hotel_ref as hq
    left join ihub.hotel_ref as ex
    on ex.hotel_agg_id = hq.hotel_agg_id
    and ex.source_system_code = 'EXPEDIA01'
    and ex.hotel_agg_id > -1
    and ex.del_flag =0
    left join hotel_source_system_rel as hl
    on hl.hotel_agg_id = ex.hotel_agg_id
    and hl.source_system_code = ex.source_system_code
    and hl.del_flag = 0
    where hl.inventory_load_enabled_flag = 1
    and hq.source_system_code = 'HQ01'
    and hq.hotel_agg_id > -1
    and hq.del_flag = 0
    group by hq.source_code
    """
    mysql_hook = MySqlHook(mysql_conn_id='ihub_database')
    data = mysql_hook.get_pandas_df(sql=query)
    return data


def request_expedia_information(expedia_hotel_id):
    api_hook = HttpHook(
        http_conn_id='expedia_hotel_information_api', method='GET')
    apiKey = "cq6aggemwh8vtkpe7ub93k2e"
    secretKey = "qSKBwn6M"
    cid = "440823"
    minorRev = "30"
    apiExperience = "PARTNER_MOBILE_APP"
    supplierType = "E"
    logging.info('hotel id: {}'.format(expedia_hotel_id))
    p = {"cid": cid, "apiKey": apiKey, "sig": signature(apiKey, secretKey), "minorRev": minorRev,
         "apiExperience": apiExperience, "supplierType": supplierType, "hotelId": expedia_hotel_id}
    resp = api_hook.run('', data=p, extra_options={'timeout': 120})
    resp = json.loads(resp.content.decode('latin-1'))

    if '@hotelId' not in resp['HotelInformationResponse']:
        logging.warn('Cannot request hotelid: {}'.format(expedia_hotel_id))

        logging.warn('message: {}'.format(
            resp['HotelInformationResponse']['EanWsError']['presentationMessage']))
        return None
    hotel_id = resp['HotelInformationResponse']['@hotelId']

    hotel = HotelTable()
    hotel.id = hotel_id
    hotel.name = resp['HotelInformationResponse']['HotelSummary']['name']
    hotel.address1 = resp['HotelInformationResponse']['HotelSummary']['address1']
    if 'address2' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.address2 = resp['HotelInformationResponse']['HotelSummary']['address2']
    hotel.city = resp['HotelInformationResponse']['HotelSummary']['city']
    if 'stateProvinceCode' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.state_province = resp['HotelInformationResponse']['HotelSummary']['stateProvinceCode']
    if 'postalCode' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.postal_code = resp['HotelInformationResponse']['HotelSummary']['postalCode']
    if 'countryCode' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.country_code = resp['HotelInformationResponse']['HotelSummary']['countryCode']
    if 'latitude' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.latitude = resp['HotelInformationResponse']['HotelSummary']['latitude']
    if 'longitude' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.longitude = resp['HotelInformationResponse']['HotelSummary']['longitude']
    if 'airportCode' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.airport_code = resp['HotelInformationResponse']['HotelSummary']['airportCode']
    if 'propertyCategory' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.property_category = resp['HotelInformationResponse']['HotelSummary']['propertyCategory']
    if 'rateCurrencyCode' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.currency_code = resp['HotelInformationResponse']['HotelSummary']['rateCurrencyCode']
    if 'hotelRating' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.star_rating = resp['HotelInformationResponse']['HotelSummary']['hotelRating']
    if 'locationDescription' in resp['HotelInformationResponse']['HotelSummary']:
        hotel.location = resp['HotelInformationResponse']['HotelSummary']['locationDescription']
    if 'checkInTime' in resp['HotelInformationResponse']['HotelDetails']:
        hotel.checkin_time = resp['HotelInformationResponse']['HotelDetails']['checkInTime']
    if 'checkOutTime' in resp['HotelInformationResponse']['HotelDetails']:
        hotel.checkout_time = resp['HotelInformationResponse']['HotelDetails']['checkOutTime']
    return hotel


def local_cleanup(**context):
    file_path = context['task_instance'].xcom_pull(key='file_path',
                                                   task_ids='download_expedia_hotel_information')
    os.remove(file_path)


def download_expedia_hotel_information(**context):
    expedia_hotel_ids = get_paired_loaded_hq_hotels()
    date = context['execution_date']
    affix = '{}_{}'.format(date.strftime('%Y_%m_%d'), str(uuid.uuid4())[:8])
    file_path = 'tmp/expedia_{}.csv'.format(affix)

    with closing(open(file_path, "w", encoding='utf-8')) as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow([
            'id',
            'name',
            'address1',
            'address2',
            'city',
            'state_province',
            'postal_code',
            'country',
            'latitude',
            'longitude',
            'airport_code',
            'property_category',
            'property_currency',
            'star_rating',
            'location',
            'checkin_time',
            'checkout_time'
        ])
        for i, df in expedia_hotel_ids.iterrows():
            hotel = request_expedia_information(df['expedia_id'])
            if hotel is not None:
                writer.writerow([
                    hotel.id,
                    hotel.name,
                    hotel.address1,
                    hotel.address2,
                    hotel.city,
                    hotel.state_province,
                    hotel.postal_code,
                    hotel.country_code,
                    hotel.latitude,
                    hotel.longitude,
                    hotel.airport_code,
                    hotel.property_category,
                    hotel.currency_code,
                    hotel.star_rating,
                    hotel.location,
                    hotel.checkin_time,
                    hotel.checkout_time
                ])

    context['task_instance'].xcom_push(key='file_path', value=file_path)


def _update_hotel_table(hotel, cur):
    logging.info('updating hotel id: {}'.format(hotel['id']))
    data = {
        'id': hotel['id'],
        'name': hotel['name'],
        'address1': hotel['address1'],
        'address2': hotel['address2'],
        'city': hotel['city'],
        'state_province': hotel['state_province'],
        'postal_code': hotel['postal_code'],
        'country': hotel['country'],
        'latitude': hotel['latitude'],
        'longitude': hotel['longitude'],
        'airport_code': hotel['airport_code'],
        'property_category': hotel['property_category'],
        'property_currency': hotel['property_currency'],
        'star_rating': hotel['star_rating'],
        'location': hotel['location'],
        'checkin_time': hotel['checkin_time'],
        'checkout_time': hotel['checkout_time']
    }
    for key in data:
        if pd.isnull(data[key]):
            data[key] = None

    query = """
                UPDATE expedia.hotel
                SET
                name=%(name)s,
                address1=%(address1)s,
                address2=%(address2)s,
                city=%(city)s,
                state_province=%(state_province)s,
                postal_code=%(postal_code)s,
                country=%(country)s,
                latitude=%(latitude)s,
                longitude=%(longitude)s,
                airport_code=%(airport_code)s,
                property_category=%(property_category)s,
                property_currency=%(property_currency)s,
                star_rating=%(star_rating)s,
                location=%(location)s,
                checkin_time=%(checkin_time)s,
                checkout_time=%(checkout_time)s,
                upd_process_id='airflow-expedia',
                upd_dt=NOW()
                WHERE id = %(id)s
            """
    cur.execute(query, data)


def update_expedia_hotel_table(**context):
    file_path = context['task_instance'].xcom_pull(key='file_path',
                                                   task_ids='download_expedia_hotel_information')
    mysql_hook = MySqlHook(mysql_conn_id='expedia_database')
    mysql_uri = make_url(mysql_hook.get_uri())

    conn = pymysql.connect(host=mysql_uri.host,
                           user=mysql_uri.username,
                           password=mysql_uri.password,
                           db=mysql_uri.database,
                           charset='utf8', use_unicode=True)
    logging.info('reading: {}'.format(file_path))
    hotel_chunks = pd.read_csv(file_path, chunksize=10000, sep='|')
    with conn:
        for chunk in hotel_chunks:
            cur = conn.cursor()
            chunk.apply(lambda x: _update_hotel_table(x, cur), axis=1)
            cur.close()
            conn.commit()


dag = DAG(
    dag_id='expedia_hotel_information',
    schedule_interval='30 0 * * 1',
    start_date=datetime(2017, 9, 18),
    catchup=True,
    default_args={
        'retries': 0
    })

expedia_download_hotel_information_operator = PythonOperator(
    task_id='download_expedia_hotel_information',
    python_callable=download_expedia_hotel_information,
    provide_context=True,
    dag=dag
)

update_expedia_hotel_table_operator = PythonOperator(
    task_id='update_expedia_hotel_table',
    python_callable=update_expedia_hotel_table,
    provide_context=True,
    dag=dag
)

clean_up_local_file_operator = PythonOperator(
    task_id='clean_up_local_file',
    python_callable=local_cleanup,
    provide_context=True,
    dag=dag
)

expedia_download_hotel_information_operator >> update_expedia_hotel_table_operator >> clean_up_local_file_operator
