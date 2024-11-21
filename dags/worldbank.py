import sys, os, warnings
sys.path.append('/home/diquest/kmi_airflow/utils')

from config import *
from python_library import *

######################
## 기록
######################
##. 

######################
## DAG 정의
######################

init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}

init_dag = DAG(
    dag_id = 'worldbank_data_collector',
    default_args = init_args,
    schedule_interval = '@once'
    # schedule_interval = '0 1 * * *'
)

task_start = DummyOperator(task_id='start', dag=init_dag)
task_end = DummyOperator(task_id='end', dag=init_dag)

######################
## Function 정의
######################
def insert_to_dataframe(result_dataframe, table_name):
    url = sqlalchemy.engine.URL.create(
        drivername = 'mysql',
        username = MDB_USERNAME,
        password = MDB_PASSWORD,
        host = MDB_HOST,
        port = MDB_PORT,
        database = 'kmi_dw_db'
    )

    engine = create_engine(url)
    conn = engine.connect()
    result_dataframe.to_sql(name = table_name, con=engine, if_exists='append', index=False)
    conn.close()

##. WORLDBANK 국가와 국제기관 세계 연간 물동량 (fct_world_bank_cargo_volume)
def func1():
    print('--' * 10, '(start) kosis_population_metrics', '--' * 10)


    print('--' * 10, '(end) kosis_population_metrics', '--' * 10)