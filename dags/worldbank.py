import sys, os, warnings
#sys.path.append('/home/diquest/kmi_airflow/utils')
sys.path.append('/opt/airflow/dags/utils')
from config import *
from python_library import *

######################
## 기록
######################
##. 본 데이터는 1개월에 1번씩 append 형식으로 수집 함
# - 데이터 1) 국가와 국제기관 세계 연간 물동량 데이터
# - 1개월에 1번씩 수집하되, 증분 데이터 없으면 Pass

##. 2024-11-12
# - 최초 작성

######################
## DAG 정의
######################
local_tz = pendulum.timezone("Asia/Seoul")

init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 12, tzinfo=local_tz),
    'retries': 1
}

init_dag = DAG(
    dag_id = 'worldbank_data_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 1 * *',
    catchup=False
)

task_start = DummyOperator(task_id='start', dag=init_dag)
task_end = DummyOperator(task_id='end', dag=init_dag)

######################
## Function 정의
######################
def processing_dataframe(input_df):
    def_df = input_df.copy()

    def_indicator = pd.json_normalize(def_df['indicator'])
    def_country = pd.json_normalize(def_df['country'])

    def_indicator.columns = ['indicator_id', 'indicator_value']
    def_country.columns = ['country_id', 'country_value']

    def_result_df = pd.concat([def_df, def_indicator, def_country], axis=1)
    def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def_result_df['CREATE_DTM'], def_result_df['UPDATE_DTM'] = def_timestamp, def_timestamp

    def_result_df = def_result_df[['indicator_id', 'indicator_value', 'country_id', 'country_value', 'date', 'value', 'CREATE_DTM', 'UPDATE_DTM']]
    def_result_df.columns = def_result_df.columns.str.upper()
    
    return def_result_df

##. WORLDBANK 국가와 국제기관 세계 연간 물동량 (fct_world_bank_cargo_volume)
def func1():
    print('--' * 10, '(start) world_bank_cargo_volume', '--' * 10)
    url = 'https://api.worldbank.org/v2/country/all/indicator/IS.SHP.GOOD.TU'
    per_page = 10000
    payloads = {
        "format" : "json",
        "page" : "1",
        "per_page" : per_page
    }

    response = requests.get(url, params = payloads)
    response_json = response.json()
    def_result_df = pd.DataFrame()

    if response_json[0]['page'] < response_json[0]['pages']:
        for i in range(1, response_json[0]['pages']+1):
            payloads = {
                "format" : "json",
                "page" : i,
                "per_page" : per_page
            }
            response = requests.get(url, params = payloads)
            response_json = response.json()
            print(response_json[0])
            response_df = pd.DataFrame(response_json[1])
            def_result_df = pd.concat([def_result_df, response_df]).reset_index(drop=True)

    else:
        def_result_df = pd.DataFrame(response_json[1])

    final_df = processing_dataframe(def_result_df)

    def_table_name = 'fct_world_bank_cargo_volume'
    def_check_column = 'DATE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_conn.close()

    if final_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        drop_table(def_table_name)
        def_create_commend = f"""
            CREATE TABLE `{def_table_name}` (
                `INDICATOR_ID` VARCHAR(30) NOT NULL COMMENT '시리즈 코드' COLLATE 'utf8_general_ci',
                `INDICATOR_VALUE` VARCHAR(100) NULL DEFAULT NULL COMMENT '시리즈 명' COLLATE 'utf8_general_ci',
                `COUNTRY_ID` CHAR(4) NOT NULL COMMENT '국가 코드' COLLATE 'utf8_general_ci',
                `COUNTRY_VALUE` VARCHAR(60) NULL DEFAULT NULL COMMENT '국가 명' COLLATE 'utf8_general_ci',
                `DATE` CHAR(4) NOT NULL COMMENT '결과 값 시점(년)' COLLATE 'utf8_general_ci',
                `VALUE` DOUBLE(15,2) NULL DEFAULT NULL COMMENT '결과 값',
                `CREATE_DTM` DATETIME NOT NULL DEFAULT current_timestamp() COMMENT '데이터 생성 일자 ',
                `UPDATE_DTM` DATETIME NOT NULL DEFAULT current_timestamp() COMMENT '데이터 업데이트 일자 '
            )
            COMMENT='* WORLD BANK 국가와 국제기관 세계 연간 물동량 데이터\n* Author. ischoi \n* Created. 2024.11.12'
            COLLATE='utf8_general_ci'
            ENGINE=InnoDB
            ;
        """
        create_table(def_create_commend)
        insert_to_dwdb(final_df, def_table_name)

    print('--' * 10, '(end) world_bank_cargo_volume', '--' * 10)

######################
## task 정의
######################

worldbank_data = PythonOperator(
    task_id = 'worldbank_data',
    python_callable = func1,
    dag = init_dag
)

task_start >> worldbank_data >> task_end



