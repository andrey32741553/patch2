import json
from datetime import datetime, timedelta
import configparser
import logging
from ftplib import FTP
from pathlib import Path
import os
import hashlib
import uuid

import pytz
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.providers.apache.hive.operators.hive import HiveCliHook
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.exceptions import AirflowException

from fastavro import writer

import xml.etree.ElementTree as ET


def get_parameters(ini_file: str) -> dict:
    """ Функция чтения config файла """
    config = configparser.ConfigParser()
    with open(ini_file, "r", encoding="utf-8") as fp:
        config.read_file(fp)
    params = {}
    for section, proxy in config.items():
        ini_section = config.items(section)
        common_keys = set(params.keys()) & {p[0] for p in ini_section}
        assert len(common_keys) == 0, "Duplicated keys: {}".format(common_keys)
        params.update(dict(config.items(section)))
    return params


today = datetime.now(pytz.timezone('Europe/Moscow'))
one_day_delta = timedelta(days=1)
week_delta = timedelta(weeks=1)
yesterday = (today - one_day_delta).strftime('%y%m%d')
last_week = (today - week_delta).strftime('%y%m%d')

dag_folder = Variable.get("dag_folder")
job_name = "KHD"
import_files_path = os.path.join(dag_folder, "import_files")
parameters = get_parameters(os.path.join(import_files_path, "config", "config.ini"))
table_names = ['pensionregistry', 'pensionreceiver']

default_args = {
    'owner': 'airflow',
    'start_date': datetime.strptime('2021-02-16 00:00:00', '%Y-%m-%d %H:%M:%S'),
    'depends_on_past': False,
    'provide_context': True
}


def get_path(table_name=None):
    """ Функция, возвращающая пути к файлам """
    paths = {'data_avro': "/opt/temp/load_pension.avro",
             'schema_avro': os.path.join(import_files_path, "avsc_files", f"{table_name}.avsc"),
             'compute_delta': os.path.join(import_files_path, "sql_files", table_name, "compute_delta.sql"),
             'compute_mirror': os.path.join(import_files_path, "sql_files", table_name, "compute_mirror.sql")}
    return paths


def read_file(filename: str) -> str:
    with open(filename, 'r') as f:
        data = f.read()
    return data.replace('\r\n', '\n').replace("\n", " ")


def read_avsc(filename: str) -> dict:
    with open(filename, 'r') as f:
        data = json.load(f)
    return data


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def compute_delta(ods_schema_name: str, table: str, params: dict) -> str:
    delta = read_file(get_path(table)['compute_delta'])
    delta = delta.replace("${insert_date}", datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S'))\
        .replace("${dws_job}", params['job_id']) \
        .replace("${ods_schema_name}", ods_schema_name) \
        .replace("${date}", params['job_date'])
    return delta


def compute_mirror(ods_schema_name: str, table: str, params: dict) -> str:
    mirror = read_file(get_path(table)['compute_mirror'])
    mirror = mirror.replace("${dws_job}", params['job_id']) \
        .replace("${insert_date}", datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')) \
        .replace("${ods_schema_name}", ods_schema_name) \
        .replace("${insert_date_delta}", params['job_date'])
    return mirror


def generating_data_dict(cl, table_name: str, path: str, params: dict, salt: str, creation_date) -> dict:
    dict_from_parsing = dict(vd=cl.findtext('vd'), po=int(cl.findtext('po')), gn=str(cl.findtext('gn')),
                             nm_f=cl.findtext('nm/f'), nm_i=cl.findtext('nm/i'),
                             nm_o=cl.findtext('nm/o'),
                             dc=cl.findtext('dc'), sm=float(cl.findtext('sm')), ad=cl.findtext('ad'),
                             pm=int(cl.findtext('pm')), d=cl.findtext('d'))

    other_vars_dict = dict(hash=hashlib.sha512((salt + dict_from_parsing['gn']).encode('utf-8'))
                           .hexdigest(),
                           hash_dc=hashlib.sha512((salt + dict_from_parsing['dc']).encode('utf-8'))
                           .hexdigest(),
                           table_name=table_name, date=params['job_date'], system_id='khd_pension',
                           hash_table=md5(Path(path)), snapshot=2, deleted_flag=False,
                           creation_date=(datetime.strptime(creation_date, '%y%m%d')).strftime('%Y-%m-%d'))

    dict_from_parsing.update(other_vars_dict)
    return dict_from_parsing


def avro_creating(data_list):
    avsc_path = get_path(table_names[0])['schema_avro']
    avsc = read_avsc(avsc_path)

    for data_dict in data_list:
        records = [
            {'vd': data_dict['vd'], 'po': data_dict['po'], 'sm': data_dict['sm'], 'pm': data_dict['pm'],
             'd': data_dict['d'], 'hash': data_dict['hash'], 'hash_dc': data_dict['hash_dc'],
             'table_name': data_dict['table_name'], 'hash_table': data_dict['hash_table'],
             'snapshot': data_dict['snapshot'], 'creation_date': data_dict['creation_date'],
             'deleted_flag': data_dict['deleted_flag'], 'system_id': data_dict['system_id'],
             'date': data_dict['date']}
        ]

        with open('/tmp/load_pension.avro', 'a+b') as out:
            writer(out, avsc, records)

    logging.info(f"Successfully saved data to file /tmp/load_pension.avro")
    return "Success"


def data_to_vertica(data_list):
    cur = VerticaHook(parameters['vertica_conn_id']).get_cursor()
    vertica_schema_name = parameters['vertica_schema_name']

    for data_dict in data_list:
        if data_dict['gn'] == 'None':
            data_dict['gn'] = ''
            data_dict['hash'] = ''
        data = (data_dict['gn'], data_dict['nm_f'], data_dict['nm_i'], data_dict['nm_o'], data_dict['dc'],
                data_dict['ad'], data_dict['hash'], data_dict['table_name'], data_dict['hash_table'],
                data_dict['hash_dc'], data_dict['snapshot'], data_dict['creation_date'],
                data_dict['deleted_flag'], data_dict['date'], data_dict['system_id'])

        cur.execute(
            f'INSERT INTO {vertica_schema_name}.pensionreceiver_dsrc VALUES ('
            '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            data, use_prepared_statements=False)

    cur.execute("COMMIT")

    logging.info(f"Successfully saved data to db vertica")
    return "Success"


def start_stage(**kwargs):
    ti = kwargs['ti']
    parameters['job_id'] = str(uuid.uuid4())
    parameters['job_date'] = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d')
    ti.xcom_push(key='params', value=parameters)
    return parameters


def uploading_file_stage():
    """ Соединение с ftp и скачивание файла xml в локальную папку """
    try:
        ftp = FTP(parameters['ftp_server_id'])
        ftp.login(user=Variable.get('authorization'), passwd=Variable.get('password'))
        ftp.cwd(parameters['file_location_on_ftp'])
        for file in ftp.nlst():
            if Path(file).suffix == '.xml':
                prefix = (str(Path(file).name)).split('_')[0]
                creation_date = (str(Path(file).name)).split('_')[2]
                if (prefix == 'SZN' or prefix == 'PFR') and creation_date == yesterday:
                    path = os.path.join('/tmp', file)
                    with open(path, 'wb') as f:
                        ftp.retrbinary('RETR ' + file, f.write)
                else:
                    raise AirflowException("There're no new xml files for uploading")
        ftp.quit()
        return "Success"
    except Exception as ex:
        logging.warning(ex)
        logging.warning(f"Failed to connect ftp: {parameters['ftp_server_id']}")


def xml_parsing_stage(**kwargs):
    """ Task для парсинга xml файлов """

    ti = kwargs['ti']
    parameters = ti.xcom_pull(key='params')

    # Начало парсинга файлов xml по дате создания с созданием списка словарей с данными
    # Файлы недельной давности удаляются
    data_list = []
    salt = Variable.get('secret')
    count_new_files = 0

    for filename in os.listdir('/tmp'):
        dir_path = '/tmp'
        path = os.path.join(dir_path, filename)
        table_name = filename.replace('.xml', '_raw')
        parser = ET.XMLParser()
        prefix = (str(Path(filename).name)).split('_')[0]  # Временная переменная
        # if Path(filename).suffix == '.xml':
        if Path(filename).suffix == '.xml' and prefix == 'SZN':
            file_creation_date = filename.split('_')[2]

            if file_creation_date == last_week:
                os.remove(path)
            elif file_creation_date == yesterday:
                tree = ET.parse(path, parser)
                root = tree.getroot()
                count_new_files += 1

                for cl in root.findall('cl'):
                    data_dict = generating_data_dict(cl, table_name, path, parameters, salt, file_creation_date)
                    data_list.append(data_dict)

    if count_new_files == 0:
        raise AirflowException("There're no new uploaded files for parsing in the folder")
    else:
        avro_creating(data_list)
        data_to_vertica(data_list)
        return "Data successfully saved"


def hdfs_stage(**kwargs):
    """ Перенос avro файла в hdfs """
    ti = kwargs['ti']
    parameters = ti.xcom_pull(key='params')
    date = parameters['job_date']
    load_to_hdfs = WebHDFSHook(webhdfs_conn_id='hdfs').load_file(
        source='/tmp/load_pension.avro',
        destination=os.path.join(parameters['hdfs_path'], f'date={date}', 'load_pension.avro'),
        overwrite=True,
    )

    # Удаление файла из временной папки после переноса в hdfs
    if os.path.isfile('/tmp/load_pension.avro'):
        os.remove('/tmp/load_pension.avro')
    return load_to_hdfs


def delta_stage(*args, **kwargs):
    """ Создание таблиц delta в бд hive и vertica """
    ti = kwargs['ti']
    params = ti.xcom_pull(key='params')
    table_name = args[0]
    if table_name == 'pensionregistry':
        hook = HiveCliHook(parameters['hive_conn_id'])
        ods_schema_name = parameters['hive_schema_name']
        delta = compute_delta(ods_schema_name, table_name, params)
        hook.run_cli(delta)
    elif table_name == 'pensionreceiver':
        cur = VerticaHook(parameters['vertica_conn_id']).get_cursor()
        ods_schema_name = parameters['vertica_schema_name']
        delta = compute_delta(ods_schema_name, table_name, params)
        cur.execute(delta)
        cur.execute("COMMIT")
    return "Success"


def mirror_stage(*args, **kwargs):
    """ Создание зеркала для pensionreceiver в vertica """
    ti = kwargs['ti']
    params = ti.xcom_pull(key='params')
    table_name = args[0]
    if table_name == 'pensionregistry':
        hook = HiveCliHook(parameters['hive_conn_id'])
        ods_schema_name = parameters['hive_schema_name']
        mirror = compute_mirror(ods_schema_name, table_name, params)
        hook.run_cli(mirror)
    elif table_name == 'pensionreceiver':
        cur = VerticaHook(parameters['vertica_conn_id']).get_cursor()
        ods_schema_name = parameters['vertica_schema_name']
        mirror = compute_mirror(ods_schema_name, table_name, params)
        cur.execute(mirror)
        cur.execute("COMMIT")
    return "Success"


with DAG(dag_id=job_name,
         default_args=default_args,
         concurrency=4,
         catchup=False,
         schedule_interval=None
         ) as dag:

    start_stage = PythonOperator(
        task_id='start_stage',
        python_callable=start_stage,
        dag=dag)

    stage_ftp_conn = PythonOperator(
        task_id='stage_ftp_connection',
        python_callable=uploading_file_stage,
        dag=dag)

    stage_xml_parsing = PythonOperator(
        task_id='stage_parsing_xml',
        python_callable=xml_parsing_stage,
        dag=dag)

    stage_avro_to_hdfs = PythonOperator(
        task_id='stage_upload_to_hdfs_avro',
        python_callable=hdfs_stage,
        dag=dag)

    stage_delta = PythonOperator(
        task_id=f'stage_delta_for_{table_names[0]}',
        python_callable=delta_stage,
        op_args=[table_names[0]],
        dag=dag)

    stage_mirror = PythonOperator(
        task_id=f'stage_mirror_for_{table_names[0]}',
        python_callable=mirror_stage,
        op_args=[table_names[0]],
        dag=dag)

    start_stage >> stage_ftp_conn >> stage_xml_parsing >> stage_avro_to_hdfs >> stage_delta >> \
        stage_mirror

    stage_delta = PythonOperator(
        task_id=f'stage_delta_for_{table_names[1]}',
        python_callable=delta_stage,
        op_args=[table_names[1]],
        dag=dag)

    stage_mirror = PythonOperator(
        task_id=f'stage_mirror_for_{table_names[1]}',
        python_callable=mirror_stage,
        op_args=[table_names[1]],
        dag=dag)

    start_stage >> stage_ftp_conn >> stage_xml_parsing >> stage_delta >> stage_mirror
