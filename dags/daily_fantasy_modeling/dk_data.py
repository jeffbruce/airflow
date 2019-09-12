# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
from datetime import datetime, timedelta
import os
import re
import requests

# make constants searchable, sensitive information stored in constants
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from constants import SOURCE_DATA, BUCKET_LOC, LOCAL_DEST

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

def get_dk_filename():
    r = requests.get(SOURCE_DATA, allow_redirects=True)

    # process raw data to figure out file name
    content = str(r.content)
    content_lines = content.split('\\n')
    next_line = False
    for line in content_lines:
        if next_line == True:
            tokens = line.split(';')
            week = tokens[0]
            year = tokens[1]
            next_line = False
        if line.find('Week;Year;GID;Name;Pos;Team;h/a;Oppt;DK points;DK salary') != -1:
            next_line = True

    filename = str(year) + '-w' + week.zfill(2) + '.html'

    return filename

def source_to_local():
    r = requests.get(SOURCE_DATA, allow_redirects=True)
    filename = get_dk_filename()
    full_filename = os.path.join(LOCAL_DEST, 'raw', filename)
    
    with open(full_filename, 'wb') as f:
        f.write(r.content)

def local_to_gs():
    filename = get_dk_filename()
    full_filename = os.path.join(LOCAL_DEST, 'raw', filename)
    os.system(' '.join(['gsutil rm', BUCKET_LOC+filename]))
    os.system(' '.join(['gsutil cp', full_filename, BUCKET_LOC]))

def process_local():
    # recreate the draft-kings.csv file
    return 0

dag = DAG(
    dag_id='dk_data', 
    description='Download and Process DraftKings Data',
    default_args=default_args,
    schedule_interval='0 14 * * 3')

source_to_local = PythonOperator(
    task_id='source_to_local', 
    python_callable=source_to_local, 
    dag=dag)

local_to_gs = PythonOperator(
    task_id='local_to_gs', 
    python_callable=local_to_gs, 
    dag=dag)

# process_local = PythonOperator(
#     task_id='process_local', 
#     python_callable=process_local, 
#     dag=dag)

# setting dependencies
# source_to_local.set_downstream([process_local, local_to_gs])
source_to_local.set_downstream(local_to_gs)
