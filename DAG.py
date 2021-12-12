import json
from datetime import datetime, timedelta
import configparser
import logging
from ftplib import FTP
from pathlib import Path
import tempfile
import os
import hashlib

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.providers.apache.hive.operators.hive import HiveCliHook
from airflow.contrib.hooks.vertica_hook import VerticaHook

from fastavro import writer

import xml.etree.ElementTree as ET


def get_parameters(ini_file: str) -> dict:
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


def read_file(filename: str) -> str:
    with open(filename, 'r') as f:
        data = f.read()
    return data.replace('\r\n', '\n').replace("\n", " ")


def read_avsc(filename: str) -> dict:
    with open(filename, 'r') as f:
        data = json.load(f)
    return data


parameters = {}
job_name = "xml_to_avro"
import_files_path = "/opt/airflow/dags/load_pension/import_files"
config_file_full_path = "/opt/airflow/dags/load_pension/import_files/config/", "config.ini"
default_args = {
    'owner': 'airflow',
    'start_date': datetime.strptime('2021-02-16 00:00:00', '%Y-%m-%d %H:%M:%S'),
    'depends_on_past': False,
    'provide_context': True
}
table_names = ['pensionregistry', 'pensionreceiver']


def get_path(table_name):
    paths = {'data_avro': "/opt/temp/load_pension.avro",
             'schema_avro': f"/opt/airflow/dags/load_pension/import_files/avsc_files/{table_name}.avsc",
             'compute_delta': os.path.join(import_files_path, "hql_files", table_name, "compute_delta.hql"),
             'compute_mirror': os.path.join(import_files_path, "hql_files", table_name, "compute_mirror.hql"),
             'truncate_src': os.path.join(import_files_path, "hql_files", table_name, "truncate_src.hql"),
             'dsrc': os.path.join(import_files_path, "hql_files", table_name, f"{table_name}_dsrc.hql")}
    return paths


def compute_delta(ods_schema_name, table, parameters):
    delta = read_file(get_path(table)['compute_delta'])
    delta.replace("${dws_job}", parameters['job_id']) \
        .replace("${insert_date}", str((datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d'))) \
        .replace("${ods_schema_name}", ods_schema_name) \
        .replace("${date}", parameters['job_date'])
    return delta


def compute_mirror(ods_schema_name, table, parameters):
    mirror = read_file(get_path(table)['compute_mirror'])
    mirror.replace("${dws_job}", parameters['job_id']) \
        .replace("${insert_date}", str((datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d'))) \
        .replace("${ods_schema_name}", ods_schema_name) \
        .replace("${insert_date_delta}", parameters['job_date'])
    return mirror


def start_stage(**kwargs):
    ti = kwargs['ti']
    parameters['job_id'] = str(xcom_pull_job_id(**kwargs))
    parameters['job_date'] = str((datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d'))
    ti.xcom_push(key='params', value=parameters)
    return parameters


def clear_src():
    for table in table_names:
        if table == 'pensionregistry':
            hook = HiveCliHook('hive')
            truncate = read_file(get_path(table)['truncate_src'])
            truncate.replace("${ods_schema_name}", 'default')
            hook.run_cli(truncate)
        elif table == 'pensionreceiver':
            cur = VerticaHook('vertica').get_cursor()
            truncate = read_file(get_path(table)['truncate_src'])
            sql = truncate.replace("${ods_schema_name}", 'public')
            cur.execute(sql)
    return "Success"


def xml_to_avro_stage(**kwargs):
    ti = kwargs['ti']
    parameters = ti.xcom_pull(key='params')
    ftp = FTP("ftp_address")
    ftp.login(user='user', passwd='passwd')
    ftp.cwd('/srv/airflow/dags/load_pension')
    for file in ftp.nlst():
        if Path(file).suffix == '.xml':
            temp_directory = tempfile.TemporaryDirectory()
            path = Path(temp_directory.name) / file
            print(path)
            with open(path, 'wb') as f:
                ftp.retrbinary('RETR ' + file, f.write)

    cur = VerticaHook('vertica').get_cursor()
    parser = ET.XMLParser()
    tree = ET.parse(path, parser)
    root = tree.getroot()
    avsc_path = get_path(table_names[0])['schema_avro']
    avsc = read_avsc(avsc_path)
    list_for_avro = []

    for cl in root.findall('cl'):
        payment_case_num = cl.findtext('vd')
        ops_delivery_num = int(cl.findtext('po'))
        insurance_num = cl.findtext('gn')
        lastname = cl.findtext('nm/f')
        firstname = cl.findtext('nm/i')
        patronymic = cl.findtext('nm/o')
        identifying_document = cl.findtext('dc')
        delivery_sum_in_coins = int(cl.findtext('sm'))
        address = cl.findtext('ad')
        department_num = int(cl.findtext('pm'))
        delivery_date = cl.findtext('d')

        salt = 'salt_pension'
        hash = hashlib.sha512((salt + str(insurance_num)).encode('utf-8')).hexdigest()
        hash_dc = hashlib.sha512((salt + identifying_document).encode('utf-8')).hexdigest()
        hash_table = hashlib.sha512((salt + avsc['name']).encode('utf-8')).hexdigest()

        records = [
            {'vd': payment_case_num, 'po': ops_delivery_num,
             'sm': delivery_sum_in_coins, 'pm': department_num, 'd': delivery_date,
             'hash': hash, 'hash_dc': hash_dc,
             'table_name': avsc['name'], 'hash_table': hash_table,
             'snapshot': 1, 'creation_date': str((datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d')),
             'deleted_flag': False, 'system_id': 'khd_pension', 'date': parameters['job_date']}
        ]

        list_for_avro.append(records)

        dsrc = read_file(get_path(table_names[1])['dsrc'])

        sql = dsrc.replace("${ods_schema_name}", 'public') \
            .replace("${gn}", str(insurance_num)).replace("${nm/f}", lastname).replace("${nm/i}", firstname)\
            .replace("${nm/o}", patronymic).replace("${dc}", identifying_document).replace("${ad}", address)\
            .replace("${hash}", hash).replace("${table_name}", avsc['name']).replace("${hash_table}", hash_table) \
            .replace("${hash_dc}", hash_dc)\
            .replace("${creation_date}", str((datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d')))\
            .replace("${deleted_flag}", 'False').replace("${date}", parameters['job_date'])\
            .replace("${system_id}", 'khd_pension')

        cur.execute(sql)

    with open("/tmp/load_pension.avro", 'wb') as out:
        writer(out, avsc, list_for_avro)
    temp_directory.cleanup()
    ftp.quit()
    logging.info(f" Success save data to file /tmp/load_pension.avro")

    return "Data saved in avro format."


def hdfs_stage():
    load_to_hdfs = WebHDFSHook(webhdfs_conn_id='hdfs').load_file(
        source="/tmp/load_pension.avro",
        destination='/user/user/pensionregistry/load_pension.avro',
        overwrite=True,
    )
    return load_to_hdfs


def delta_stage(**kwargs):
    ti = kwargs['ti']
    parameters = ti.xcom_pull(key='params')
    for table in table_names:
        if table == 'pensionregistry':
            hook = HiveCliHook('hive')
            ods_schema_name = 'default'
            delta = compute_delta(ods_schema_name, table, parameters)
            hook.run_cli(delta)
        elif table == 'pensionreceiver':
            cur = VerticaHook('vertica').get_cursor()
            ods_schema_name = 'public'
            delta = compute_delta(ods_schema_name, table, parameters)
            cur.execute(delta)
    return "Success"


def mirror_stage(**kwargs):
    ti = kwargs['ti']
    parameters = ti.xcom_pull(key='params')
    for table in table_names:
        if table == 'pensionregistry':
            hook = HiveCliHook('hive')
            ods_schema_name = 'default'
            mirror = compute_mirror(ods_schema_name, table, parameters)
            hook.run_cli(mirror)
        elif table == 'pensionreceiver':
            cur = VerticaHook('vertica').get_cursor()
            ods_schema_name = 'public'
            mirror = compute_mirror(ods_schema_name, table, parameters)
            cur.execute(mirror)
    return "Success"


with DAG(dag_id=job_name,
         default_args=default_args,
         concurrency=4,
         catchup=False,
         schedule_interval=None
         ) as dag:

    stage_clear_src = PythonOperator(
        task_id=f'stage_clear_src',
        python_callable=clear_src,
        dag=dag)

    stage_xml_to_avro = PythonOperator(
        task_id=f'stage_parsing_xml',
        python_callable=xml_to_avro_stage,
        dag=dag)

    stage_hdfs = PythonOperator(
        task_id=f'stage_upload_to_hdfs_avro',
        python_callable=hdfs_stage,
        dag=dag)

    stage_delta = PythonOperator(
        task_id=f'stage_delta',
        python_callable=delta_stage,
        dag=dag)

    stage_mirror = PythonOperator(
        task_id=f'stage_mirror',
        python_callable=mirror_stage,
        dag=dag)

    stage_clear_src >> stage_xml_to_avro >> stage_hdfs >> stage_delta >> stage_mirror
