import sys, os, warnings
sys.path.append('/home/diquest/kmi_airflow/utils')

from config import *
from python_library import *

######################
## 기록
######################
##. 본 데이터는 6개월에 1번씩 append 형식으로 수집 함(1월 1일, 7월 1일, 새벽 1시)
##. 2024-11-11
# - 수집 기간 형식 변경 : 1960 ~ 1972 -> 최근 100년
# - 증분 조건 : DB에 있는 데이터의 집계일자와 수집할 데이터의 집계일자의 최대값이 같은면 Pass 아니면 적재


######################
## DAG 정의
######################

init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}

init_dag = DAG(
    dag_id = 'kosis_data_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 * * *'
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

##. KOSIS 연간 인구지표 (fct_kosis_population_metrics_2)
def func1():
    print('--' * 10, '(start) kosis_population_metrics', '--' * 10)
    url = 'https://kosis.kr/openapi/Param/statisticsParameterData.do'
    
    payloads = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "itmId" : "T10+",
        "objL1" : "ALL",
        "objL2" : "ALL",
        "format" : "json",
        "jsonVD" : "Y",
        "prdSe" : "Y",
        "newEstPrdCnt" : "150", # 최근 150년
        "orgId" : "101",
        "tblId" : "DT_1BPA002",
    }
    
    response = requests.get(url, params=payloads)
    response_json = response.json()
    response_df = pd.DataFrame(response_json)

    def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    response_df['CREATE_DTM'] = def_timestamp
    response_df['UPDATE_DTM'] = def_timestamp
    
    #. 최신 데이터 비교 후 적재
    def_table_name = 'fct_kosis_population_metrics_2'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn, dtype='object')
    def_conn.close()

    if response_df['LST_CHN_DE'].max() == def_origin_df['LST_CHN_DE'].max():
        print("No Update Data")

    else:
        insert_to_dataframe(response_df, def_table_name)        

    print('--' * 10, '(end) kosis_population_metrics', '--' * 10)
    
##. KOSIS 연간 시나리오별 추계인구 (fct_kosis_population_scenario_2)
def func2():
    print('--' * 10, '(start) kosis_population_scenario', '--' * 10)
    url = 'https://kosis.kr/openapi/Param/statisticsParameterData.do'
    
    payloads = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "itmId" : "T10+",
        "objL1" : "ALL",
        "objL2" : "ALL",
        "format" : "json",
        "jsonVD" : "Y",
        "prdSe" : "Y",
        "newEstPrdCnt" : "150", # 최근 150년
        "orgId" : "101",
        "tblId" : "DT_1BPA401",
    }
    
    response = requests.get(url, params=payloads)
    response_json = response.json()
    response_df = pd.DataFrame(response_json)

    def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    response_df['CREATE_DTM'] = def_timestamp
    response_df['UPDATE_DTM'] = def_timestamp
    
    #. 최신 데이터 비교 후 적재
    def_table_name = 'fct_kosis_population_secenario_2'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn, dtype='object')
    def_conn.close()

    if response_df['LST_CHN_DE'].max() == def_origin_df['LST_CHN_DE'].max():
        print("No Update Data")

    else:
        insert_to_dataframe(response_df, def_table_name)

    print('--' * 10, '(end) kosis_population_scenario', '--' * 10)
    
##. KOSIS 연간 1인단 양곡 소비량 (fct_kosis_grain_consumption_2)
def func3():
    print('--' * 10, '(start) kosis_grain_consumption', '--' * 10)
    url = 'https://kosis.kr/openapi/Param/statisticsParameterData.do'
    
    payloads = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "itmId" : "T10+T20+T30+",
        "objL1" : "ALL",
        "objL2" : "ALL",
        "format" : "json",
        "jsonVD" : "Y",
        "prdSe" : "Y",
        "newEstPrdCnt" : "100", # 최근 150년
        "orgId" : "101",
        "tblId" : "DT_1ED0001",
    }
    
    response = requests.get(url, params=payloads)
    response_json = response.json()
    response_df = pd.DataFrame(response_json)

    def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    response_df['CREATE_DTM'] = def_timestamp
    response_df['UPDATE_DTM'] = def_timestamp
    
    #. 최신 데이터 비교 후 적재
    def_table_name = 'fct_kosis_grain_consumption_2'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn, dtype='object')
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        insert_to_dataframe(response_df, def_table_name)

    print('--' * 10, '(end) kosis_grain_consumption', '--' * 10)

######################
## task 정의
######################

task1 = PythonOperator(
    task_id = 'task1',
    python_callable = func1,
    dag = init_dag
)

task2 = PythonOperator(
    task_id = 'task2',
    python_callable = func2,
    dag = init_dag
)

task3 = PythonOperator(
    task_id = 'task3',
    python_callable = func3,
    dag = init_dag
)

task_start >> task1 >> task2 >> task3 >> task_end