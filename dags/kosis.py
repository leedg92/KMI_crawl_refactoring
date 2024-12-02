import sys, os, warnings
sys.path.append('/home/diquest/kmi_airflow/utils')

from config import *
from python_library import *
from kosis_config import *

######################
## 기록
######################
##. 본 데이터는 1개월에 1번씩 append 형식으로 수집 함
# - 1개월에 1번씩 수집하되, 증분 데이터 없으면 Pass
# - 데이터 1) 연간 인구지표 : 매 2년 5월에 업데이트
# - 데이터 2) 연간 시나리오별 추계인구 : 매 2년 5월에 업데이트
# - 데이터 3) 연간 1인당 양곡 소비량  : 매년 1월에 업데이트
# - 데이터 4) 시도별 경제활동별 지역내 총생산 : 매년 12월에 업데이트
# - 데이터 5) 시도/산업별 광공업생산지수(2020=100) : 매달 말일 업데이트
# => 광업제조업 > 광업제조업동향조사 > 생산, 출하, 재고 > 시도/산업별 광공업생산지수(2020=100)
# - 데이터 6) 축종별 시도별 가구수 및 마리수 : 업데이트 사항 없음(2017년도 이전의 과거 데이터)
# => 농림 > 가축동향조사 > 소 > 이력제 자료 대체 이전(~2017.2/4) > 축종별 시도별 가구수 및 마리수 (분기 1983 1/4 ~2017 2/4)
# - 데이터 7) 시도/산업별 총괄(사업체수, 종사자수, 매출액) : 매년 2월에 업데이트
# => 도소매서비스 > 서비스업조사 > 조사기반 > 8차 & 9차 & 10차 > 시도/산업별 총괄
# - 데이터 8) 업종별 기업경기실사지수 (건설업 전망 및 실적)
# => 경제일반경기 > 기업경기조사 > 업종별 기업경기조사 > 업종별 기업경기실사지수

##. 2024-11-11
# - 수집 기간 형식 변경 : 1960 ~ 1972 -> 최근 100년
# - 증분 조건 : DB에 있는 데이터의 집계일자와 수집할 데이터의 집계일자의 최대값이 같은면 Pass 아니면 적재

##. 2024-11-18
# - 데이터 4, 5, 6 추가

##. 2024--11-21
# - 데이터 7, 8 추가
# - 코드 최적화 (kosis_config 추가)

##. 2024-12-02
# - read_sql 구문 변경(dtype 삭제)

######################
## DAG 정의
######################
local_tz = pendulum.timezone("Asia/Seoul")

init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11, tzinfo=local_tz)
}

init_dag = DAG(
    dag_id = 'kosis_data_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 1 * *'
)

task_start = DummyOperator(task_id='start', dag=init_dag)
task_end = DummyOperator(task_id='end', dag=init_dag)

######################
## Common Function 정의
######################
def kosis_api_body(body_type, itmId, objL1, objL2, newEstPrdCnt, prdSe, startPrdDe, endPrdDe, orgId, tblId):
    url = KOSIS_API_URL

    if body_type == 1:
        def_json_body = {
            "method": "getList",
            "apiKey": KOSIS_API_KEY,
            "itmId" : itmId,
            "objL1" : objL1,
            "objL2" : objL2,
            "format" : "json",
            "jsonVD" : "Y",
            "prdSe" : prdSe,
            "newEstPrdCnt" : newEstPrdCnt, # 최근 150년
            "orgId" : orgId,
            "tblId" : tblId
        }

    else:
        def_json_body = {
            "method" : "getList",
            "apiKey" : KOSIS_API_KEY,
            "itmId" : itmId,
            "objL1" : objL1,
            "objL2" : objL2,
            "format" : "json",
            "jsonVD" : "Y",
            "prdSe" : prdSe,
            "startPrdDe" : startPrdDe,
            "endPrdDe" : endPrdDe,
            "orgId" : orgId,
            "tblId" : tblId
        }

    def_response = requests.get(url, params=def_json_body)
    def_response_df = pd.DataFrame(def_response.json())
    def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def_response_df['CREATE_DTM'] = def_timestamp
    def_response_df['UPDATE_DTM'] = def_timestamp

    return def_response_df

######################
## Function 정의
######################
##. KOSIS 연간 인구지표 (fct_kosis_population_metrics)
def func1():
    print('--' * 10, '(start) kosis_population_metrics', '--' * 10)
    response_df = kosis_api_body(body_type = 1,
                                 itmId = 'T10+',
                                 objL1 = 'ALL',
                                 objL2 = 'ALL',
                                 prdSe = 'Y',
                                 newEstPrdCnt = 150,
                                 startPrdDe = None,
                                 endPrdDe = None,
                                 orgId = 101,
                                 tblId = 'DT_1BPA002')
    
    #. 최신 데이터 비교 후 적재
    def_table_name = 'fct_kosis_population_metrics'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_POPULATION_METRICS 
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        response_df = response_df[insert_cols]        
        insert_to_dwdb(response_df, def_table_name)        

    print('--' * 10, '(end) kosis_population_metrics', '--' * 10)
    
##. KOSIS 연간 시나리오별 추계인구 (fct_kosis_population_scenario)
def func2():
    print('--' * 10, '(start) kosis_population_scenario', '--' * 10)
    #. 최신 3년 데이터만 우선 탐색
    response_df = kosis_api_body(body_type = 1,
                                 itmId = 'T10+',
                                 objL1 = 'ALL',
                                 objL2 = 'ALL',
                                 prdSe = 'Y',
                                 newEstPrdCnt = 3,
                                 startPrdDe = None,
                                 endPrdDe = None,
                                 orgId = 101,
                                 tblId = 'DT_1BPA401')
  
    #. 최신 데이터 비교 후 적재
    def_table_name = 'fct_kosis_population_secenario'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        response_df = kosis_api_body(body_type = 1,
                                     itmId = 'T10+',
                                     objL1 = 'ALL',
                                     objL2 = 'ALL',
                                     prdSe = 'Y',
                                     newEstPrdCnt = 150,
                                     startPrdDe = None,
                                     endPrdDe = None,
                                     orgId = 101,
                                     tblId = 'DT_1BPA401')
        
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_POPULATION_SECENARIO
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        response_df = response_df[insert_cols]        
        insert_to_dwdb(response_df, def_table_name)

    print('--' * 10, '(end) kosis_population_scenario', '--' * 10)
    
##. KOSIS 연간 1인단 양곡 소비량 (fct_kosis_grain_consumption)
def func3():
    print('--' * 10, '(start) kosis_grain_consumption', '--' * 10)

    response_df = kosis_api_body(body_type = 1,
                                 itmId = 'T10+T20+T30+',
                                 objL1 = 'ALL',
                                 objL2 = 'ALL',
                                 prdSe = 'Y',
                                 newEstPrdCnt = 150,
                                 startPrdDe = None,
                                 endPrdDe = None,
                                 orgId = 101,
                                 tblId = 'DT_1ED0001')
    
    #. 최신 데이터 비교 후 적재
    def_table_name = 'fct_kosis_grain_consumption'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_GRAIN_CONSUMPTION
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        response_df = response_df[insert_cols]
        insert_to_dwdb(response_df, def_table_name)

    print('--' * 10, '(end) kosis_grain_consumption', '--' * 10)

##. KOSIS 시도별 경제활동별 지역내총생산 (fct_kosis_region_income)
#- 데이터 건수가 API 최대 제한 건수를 넘어서, 10년 단위로 수집  
def func4():
    print('--' * 10, '(start) kosis_region_income', '--' * 10)

    #. 최신 데이터 비교 후 적재(최근 3년 기준)
    def_end_timestamp = int(datetime.datetime.now().strftime("%Y"))
    def_start_timestamp = def_end_timestamp - 3
    
    response_df = kosis_api_body(body_type = 2,
                                 itmId = 'T1+T2+T3+',
                                 objL1 = 'ALL',
                                 objL2 = 'ALL',
                                 prdSe = 'Y',
                                 newEstPrdCnt = None,
                                 startPrdDe = def_start_timestamp,
                                 endPrdDe = def_end_timestamp,
                                 orgId = 101,
                                 tblId = 'DT_1C81')
    
    def_table_name = 'fct_kosis_region_income'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()
    
    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        ##. 데이터 수집
        collect_start_year = 1985
        collect_end_year = int(datetime.datetime.now().strftime("%Y"))
        
        result_df = pd.DataFrame()
        
        for apply_year in range(1985, collect_end_year, 10):
            start_year = apply_year
            end_year = apply_year + 10

            if end_year > collect_end_year:
                end_year = collect_end_year

            response_df = kosis_api_body(body_type = 2,
                                         itmId = 'T1+T2+T3+',
                                         objL1 = 'ALL',
                                         objL2 = 'ALL',
                                         prdSe = 'Y',
                                         newEstPrdCnt = None,
                                         startPrdDe = start_year,
                                         endPrdDe = end_year,
                                         orgId = 101,
                                         tblId = 'DT_1C81')
            result_df = pd.concat([result_df, response_df]).reset_index(drop=True)
    
        def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_df['CREATE_DTM'] = def_timestamp
        result_df['UPDATE_DTM'] = def_timestamp

        ##. 데이터 적재
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_REGION_INCOME
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        result_df = result_df[insert_cols]
        insert_to_dwdb(result_df, def_table_name)

    print('--' * 10, '(end) kosis_region_income', '--' * 10)

##. KOSIS 시도별 경제활동별 지역내총생산 (fct_kosis_product_index)
#- 데이터 건수가 API 최대 제한 건수를 넘어서, 10년 단위로 수집
def func5():
    print('--' * 10, '(start) kosis_product_index', '--' * 10)
    #. 최신 데이터 비교 (최근 3년)
    def_end_timestamp = int(datetime.datetime.now().strftime("%Y%m")) #YYYYMM
    def_start_timestamp = def_end_timestamp - 300

    response_df = kosis_api_body(body_type = 2,
                                 itmId = 'T10+T11+T12+',
                                 objL1 = 'ALL',
                                 objL2 = 'C13+C14+C205+',
                                 prdSe = 'Y',
                                 newEstPrdCnt = None,
                                 startPrdDe = def_start_timestamp,
                                 endPrdDe = def_end_timestamp,
                                 orgId = 101,
                                 tblId = 'DT_1F02001')    

    def_table_name = 'fct_kosis_product_index'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        ##. 데이터 수집
        collect_start_date = 197501
        current_date = int(datetime.datetime.now().strftime("%Y%m"))
        result_df = pd.DataFrame()
        for apply_date in range(collect_start_date, current_date, 1000):
            start_date = apply_date # YYYY01
            end_date = apply_date + 1011 # YYYY + YY(Y+10)Y12

            if end_date > current_date:
                end_date = current_date

            print(start_date, end_date)
            response_df = kosis_api_body(body_type = 2,
                                         itmId = 'T10+T11+T12+',
                                         objL1 = 'ALL',
                                         objL2 = 'C13+C14+C205+',
                                         prdSe = 'Y',
                                         newEstPrdCnt = None,
                                         startPrdDe = start_date,
                                         endPrdDe = end_date,
                                         orgId = 101,
                                         tblId = 'DT_1F02001')
            result_df = pd.concat([result_df, response_df]).reset_index(drop=True)

        def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_df['CREATE_DTM'] = def_timestamp
        result_df['UPDATE_DTM'] = def_timestamp
        
        ##. 데이터 적재
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_PRODUCT_INDEX
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        result_df = result_df[insert_cols]
        insert_to_dwdb(result_df, def_table_name)

    print('--' * 10, '(end) kosis_product_index', '--' * 10)

##. KOSIS 축종별 시도별 가구수 및 마리수 (fct_kosis_cattle_statistics)
def func6():
    print('--' * 10, '(start) kosis_cattle_statistics', '--' * 10)
    response_df = kosis_api_body(body_type = 2,
                                 itmId = 'T02+T04+T06+',
                                 objL1 = 'ALL',
                                 objL2 = None,
                                 prdSe = 'Q',
                                 newEstPrdCnt = None,
                                 startPrdDe = 198301,
                                 endPrdDe = 201702,
                                 orgId = 101,
                                 tblId = 'DT_1EO099')
    
    def_table_name = 'fct_kosis_cattle_statistics'
    def_check_column = 'LST_CHN_DE'
    
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_CATTLE_STATISTICS
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        response_df = response_df[insert_cols]
        insert_to_dwdb(response_df, def_table_name)
    
    print('--' * 10, '(end) kosis_cattle_statistics', '--' * 10)

##. KOSIS 시도/산업별 총괄 : 외식업 / 식품유통업 산업현황 (fct_kosis_service_industry)
def func7():
    print('--' * 10, '(start) kosis_service_industry', '--' * 10)
    #. 데이터 증분 체크는 최신 데이터로 진행
    def_end_timestamp = int(datetime.datetime.now().strftime("%Y"))
    response_df = kosis_api_body(body_type = 2,
                                 itmId = 'T01+T02+T03+',
                                 objL1 = 'ALL',
                                 objL2 = '463+561+',
                                 prdSe = 'Y',
                                 newEstPrdCnt = None,
                                 startPrdDe = 2020,
                                 endPrdDe = def_end_timestamp,
                                 orgId = 101,
                                 tblId = 'DT_2KB9001')
    
    def_table_name = 'fct_kosis_service_industry'
    def_check_column = 'LST_CHN_DE'
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        #. 데이터 수집 : 1997 ~ 2006
        response_df_1 = kosis_api_body(body_type = 2,
                                       itmId = 'T10+T20+T30+',
                                       objL1 = 'ALL',
                                       objL2 = 'G513+H552+',
                                       prdSe = 'Y',
                                       newEstPrdCnt = None,
                                       startPrdDe = 1997,
                                       endPrdDe = 2006,
                                       orgId = 101,
                                       tblId = 'DT_1KB7001')
        #. 데이터 수집 : 2006 ~ 2016
        response_df_2 = kosis_api_body(body_type = 2,
                                       itmId = 'T01+T02+T05+',
                                       objL1 = 'ALL',
                                       objL2 = 'G463+I561+',
                                       prdSe = 'Y',
                                       newEstPrdCnt = None,
                                       startPrdDe = 2006,
                                       endPrdDe = 2016,
                                       orgId = 101,
                                       tblId = 'DT_1KB6001')
        #. 데이터 수집 : 2006 ~ 2016
        response_df_3 = kosis_api_body(body_type = 2,
                                       itmId = 'T01+T02+T05+',
                                       objL1 = 'ALL',
                                       objL2 = '463+561+',
                                       prdSe = 'Y',
                                       newEstPrdCnt = None,
                                       startPrdDe = 2016,
                                       endPrdDe = 2020,
                                       orgId = 101,
                                       tblId = 'DT_1KB9001')

        result_df = pd.concat([response_df_1, response_df_2, response_df_3, response_df])
        def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_df['CREATE_DTM'] = def_timestamp
        result_df['UPDATE_DTM'] = def_timestamp
        #. 중복제거
        result_df = result_df.drop_duplicates().reset_index(drop=True)
        
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_SERVICE_INDUSTRY
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        result_df = result_df[insert_cols]
        insert_to_dwdb(result_df, def_table_name)

    print('--' * 10, '(end) kosis_service_industry', '--' * 10)
    
##. KOSIS 업종별 기업경기실사지수 (실적 및 전망) : 건설업 (fct_kosis_construction_bsi)
def func8():
    print('--' * 10, '(start) kosis_construction_bsi', '--' * 10)
    #. 데이터 증분 체크는 최신 데이터로 진행
    def_end_timestamp = datetime.datetime.now().strftime("%Y%m")
    response_df = kosis_api_body(body_type = 2,
                                 itmId = '13103134566999+',
                                 objL1 = '13102134566BSI_CD.BA+13102134566BSI_CD.BB+13102134566BSI_CD.BE+13102134566BSI_CD.BO+13102134566BSI_CD.BJ+',
                                 objL2 = '13102134566BUSINESS_TYPE_CD.F4100+',
                                 prdSe = 'M',
                                 newEstPrdCnt = None,
                                 startPrdDe = 200909,
                                 endPrdDe = def_end_timestamp,
                                 orgId = 301,
                                 tblId = 'DT_512Y008')

    def_table_name = 'fct_kosis_construction_bsi'
    def_check_column = 'LST_CHN_DE'

    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"SELECT DISTINCT({def_check_column}) FROM {def_table_name}", con=def_conn)
    def_origin_cols = pd.read_sql(f"SELECT * FROM {def_table_name} LIMIT 5", con=def_conn)
    def_conn.close()

    if response_df[def_check_column].max() == def_origin_df[def_check_column].max():
        print("No Update Data")

    else:
        response_df_1 = kosis_api_body(body_type = 2,
                                       itmId = '13103134491999+',
                                       objL1 = '13102134491BSI_CD.AA+13102134491BSI_CD.AB+13102134491BSI_CD.AE+13102134491BSI_CD.AO+13102134491BSI_CD.AJ+',
                                       objL2 = '13102134491BUSINESS_TYPE_CD.F4100+',
                                       prdSe = 'M',
                                       newEstPrdCnt = None,
                                       startPrdDe = 200908,
                                       endPrdDe = def_end_timestamp,
                                       orgId = 301,
                                       tblId = 'DT_512Y007')
        #. 실적, 전망 순서
        result_df = pd.concat([response_df_1, response_df]).reset_index(drop=True)
        def_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_df['CREATE_DTM'] = def_timestamp
        result_df['UPDATE_DTM'] = def_timestamp
        
        drop_table(def_table_name)
        def_create_commend = CREATE_FCT_KOSIS_CONSTRUCTION_BSI
        create_table(def_create_commend)
        insert_cols = def_origin_cols.columns
        result_df = result_df[insert_cols]
        insert_to_dwdb(result_df, def_table_name)        
    
    print('--' * 10, '(end) kosis_construction_bsi', '--' * 10)
    

######################
## task 정의
######################

task1 = PythonOperator(
    task_id = 'fct_kosis_population_metrics',
    python_callable = func1,
    dag = init_dag
)

task2 = PythonOperator(
    task_id = 'fct_kosis_population_secenario',
    python_callable = func2,
    dag = init_dag
)

task3 = PythonOperator(
    task_id = 'fct_kosis_grain_consumption',
    python_callable = func3,
    dag = init_dag
)

task4 = PythonOperator(
    task_id = 'fct_kosis_region_income',
    python_callable = func4,
    dag = init_dag
)

task5 = PythonOperator(
    task_id = 'fct_kosis_product_index',
    python_callable = func5,
    dag = init_dag
)

task6 = PythonOperator(
    task_id = 'fct_kosis_cattle_statistics',
    python_callable = func6,
    dag = init_dag
)

task7 = PythonOperator(
    task_id = 'fct_kosis_service_industry',
    python_callable = func7,
    dag = init_dag
)

task8 = PythonOperator(
    task_id = 'fct_kosis_construction_bsi',
    python_callable = func8,
    dag = init_dag
)

task_start >> task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task_end
#. task6 : 일회성 수집으로 스케쥴에서 제외

