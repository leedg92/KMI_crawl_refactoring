import pendulum
from datetime import datetime, timedelta
import sys, os, warnings

###########################서버용 설정 시작###########################
sys.path.append('/opt/airflow/dags/utils')

from config import *
from python_library import *
from api_account import *
###########################서버용 설정 끝#############################

###########################로컬용 설정 시작###########################
#from pathlib import Path

# 상대 경로로 utils 디렉토리 추가
#dag_folder = os.path.dirname(os.path.abspath(__file__))
#project_root = os.path.dirname(dag_folder)
#utils_path = os.path.join(project_root, 'utils')

#if utils_path not in sys.path:
    #sys.path.append(utils_path)

#from utils.config import *
#from utils.python_library import *
###########################로컬용 설정 끝###########################



######################
## 기록
######################
##. [2024-11-18 이동근] 리펙토링 시작(공통 함수 분리, 상수 정의, 함수 정의)
##. [2024-11-19 이동근] DB 적재 구현
##. [2024-11-20 이동근] Airflow 적재 구현
##. [2024-11-21 이동근] 최신데이  로직 제거, 공통 설정 호출 함수 제거(무의미함)
##----------------ECOS 데이터 수집 프로세스---------------
##. 
##  1. 공통 설정 및 코드 조회
##     - get_ecos_codes(): DB에서 수집할 통계 코드 목록 조회
##
##  2. 날짜 범위 설정
##     - get_year_month(): 현재 연월(YYYYMM) 반환
##     - get_date_range(): 기간 유형(월/분기/연)별 시작/종료 날짜 계산
##
##  3. API 호출
##     - create_ecos_url(): API 호출 URL 생성
##     - collect_ecos_data(): 실제 API 호출 및 데이터 수집
##
##  4. 데이터 검증 및 처리
##     - insert_to_dataframe(): 수집된 데이터를 DB에 저장
##
##  5. 수집 실행
##     - collect_ecos_interest_rate_data(): 금리 데이터 수집 실행
##
##----------------ECOS 데이터 수집 프로세스 끝----------------


######################
## DAG 정의
######################
KST = pendulum.timezone("Asia/Seoul")

init_args = {
    'owner' : OWNER_NAME,
    'start_date': pendulum.datetime(2024, 11, 18, tz=KST),
    'retries': 1,  # 실패 시 재시도 횟수
}

init_dag = DAG(
    dag_id = 'ecos_data_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 1 * *',
    catchup=False
)

task_start = DummyOperator(task_id='start', dag=init_dag)
task_end = DummyOperator(task_id='end', dag=init_dag)



######################
## 상수 정의
######################
ECOS_CONFIG = {
    'API_URL': 'http://ecos.bok.or.kr/api/StatisticSearch',
    'API_KEY': ECOS_API_KEY,  # API 키를 여기에 입력해주세요
    'OUTPUT_FORMAT': 'json',
    'LANG': 'kr',
    'COLLECT_AT_ONCE_CNT': '100'
}



######################
## 함수 정의
######################
#. 수집할 통계 코드 조회
def get_ecos_codes(category, period):
    print(f"\n[DEBUG] Fetching codes for category: {category}, period: {period}")
    
    conn = maria_kmi_dw_db_connection()
    query = """
        SELECT 
            LAST_CATEGORY_CD,
            CATEGORY_NM,
            COLLECTION_CYCLE,
            FULL_PATH
        FROM view_ecos_path_view
        WHERE SUBSTRING_INDEX(FULL_PATH, '|', 1) = %s
        AND COLLECTION_CYCLE = %s
    """
    print(f"\n[DEBUG] Executing query: {query}")
    print(f"[DEBUG] With params: [{category}, {period}]")
    
    codes_df = pd.read_sql(query, conn, params=[category, period])
    print(f"\n[DEBUG] Query result: {codes_df}")
    
    conn.close()
    return codes_df.to_dict('records')


#. 현재 연월을 YYYYMM 형식으로 반환
def get_year_month():
    return pendulum.now().format('YYYYMM')


#. 기간 유형에 따른 시작/종료 날짜 반환
def get_date_range(period_type='M'):
    now = pendulum.now()
    
    if period_type == 'M':
        # 12개월 전부터 현재까지
        from_date = now.subtract(months=11).format('YYYYMM')  # 11개월을 빼면 총 12개월
        to_date = now.format('YYYYMM')
        
    elif period_type == 'Y':
        # 작년부터 올해까지
        from_date = str(now.year - 1)
        to_date = str(now.year)
        
    elif period_type == 'Q':
        # 현재 분기 계산
        current_quarter = (now.month - 1) // 3 + 1
        current_year = now.year
        
        # 4분기 전의 연도와 분기 계산
        if current_quarter <= 4:
            from_year = current_year - 1
            from_quarter = current_quarter
        else:
            from_year = current_year
            from_quarter = current_quarter - 4
            
        from_date = f"{from_year}Q{from_quarter}"
        to_date = f"{current_year}Q{current_quarter}"
    
    else:
        # 기본값은 월간 데이터와 동일하게 처리
        from_date = now.subtract(months=11).format('YYYYMM')
        to_date = now.format('YYYYMM')
    
    return from_date, to_date

#. ECOS API URL 생성
def create_ecos_url(table_code, item_code, period='M'):
    from_date, to_date = get_date_range(period)
    api_period = 'A' if period == 'Y' else period
    
    url = '/'.join([
        ECOS_CONFIG['API_URL'],
        ECOS_CONFIG['API_KEY'],
        ECOS_CONFIG['OUTPUT_FORMAT'],
        ECOS_CONFIG['LANG'],
        '1',
        ECOS_CONFIG['COLLECT_AT_ONCE_CNT'],
        table_code,
        api_period,
        from_date,
        to_date,
        item_code
    ])
    
    print(f"\n[DEBUG] Generated URL: {url}")
    return url

#. ECOS 데이터 수집 공통 함수
def collect_ecos_data(category, period):
    print('--' * 10, f'(start) ecos_{category.lower()}', '--' * 10)
    
    codes = get_ecos_codes(category, period)
    if not codes:
        print(f"[WARNING] No codes found for category {category} and period {period}")
        return None
        
    all_data = []
    for input_code in codes:
        print(f"\n[DEBUG] Processing code: {input_code}")
        
        # FULL_PATH에서 필요한 정보 추출 (파이프 구분자 사용)
        path_parts = input_code['FULL_PATH'].split('|')
        table_code = path_parts[1]
        
        # 나머지 모든 부분을 item_code로 처리
        item_codes = '/'.join(path_parts[2:])
        
        full_url = create_ecos_url(
            table_code,
            item_codes,
            period
        )
        
        print(f"\n[API Call URL] {full_url}\n")
        
        response = requests.get(full_url)
        print(f"[DEBUG] Response status: {response.status_code}")
        print(f"[DEBUG] Response content: {response.text[:500]}")
        
        if response.status_code == 200:
            response_json = response.json()
            if 'StatisticSearch' in response_json:
                data = response_json['StatisticSearch']['row']
                current_time = pendulum.now().format('YYYY-MM-DD HH:mm:ss')
                
                for item in data:
                    item = {key: value if value is not None else "" for key, value in item.items()}
                    item['CATEGORY'] = category
                    #item['CREATE_DTM'] = current_time
                    item['UPDATE_DTM'] = current_time
                    if 'DATA_VALUE' in item and item['DATA_VALUE'] == '':
                        item['DATA_VALUE'] = 0
                    all_data.append(item)
    
    if all_data:
        return pd.DataFrame(all_data)
    
    return None

#. DB에 저장
def insert_to_dataframe(df, period):
    if df is None or df.empty:
        print("No data to insert")
        return
        
    print("\n=== Inserting data to DB ===")
    print(f"Data shape: {df.shape}")
    
    # period에 따른 테이블 선택
    if period == 'Y':
        table_name = 'fct_ecos_statics_year'
    elif period == 'Q':
        table_name = 'fct_ecos_statics_quarter'
    elif period == 'M':
        table_name = 'fct_ecos_statics_month'
    else:
        raise ValueError(f"Invalid period: {period}")
    
    conn = maria_kmi_dw_db_connection()
    cursor = conn.cursor()
    
    try:
        # 컬럼명과 값을 동적으로 생성
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        update_stmt = ', '.join([f"{col} = VALUES({col})" for col in df.columns])
        
        # INSERT ON DUPLICATE KEY UPDATE 구문 생성
        query = f"""
            INSERT INTO {table_name} 
            ({columns}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_stmt}
        """
        
        # 데이터프레임을 리스트로 변환
        values = [tuple(x) for x in df.values]
        
        print(f"\n[DEBUG] Target table: {table_name}")
        print(f"[DEBUG] Executing query: {query}")
        print(f"[DEBUG] First row of values: {values[0] if values else 'No values'}")
        
        # 실행
        cursor.executemany(query, values)
        conn.commit()
        
        print(f"Successfully inserted/updated {len(df)} rows into {table_name}")
        
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
        conn.rollback()
        raise
        
    finally:
        cursor.close()
        conn.close()

def month_time_to_month(df):
    df['year'] = df['TIME'].str[:4]
    df['month'] = df['TIME'].str[-2:]
    
    return df

def quarter_time_to_quarter(df):
    df['YEAR'] = df['TIME'].str[:4]
    df['QUARTER'] = df['TIME'].str[-1:]
    
    return df


######################
## 수집 함수 정의
######################
#. ECOS 금리 데이터 수집
def collect_ecos_interest_rate_data():    
    df = collect_ecos_data('INTEREST_RATE', 'M')
    if df is not None:
        print("\n=== INTEREST_RATE 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_year = collect_ecos_data('INTEREST_RATE', 'Y')
    if df_year is not None:
        print("\n=== INTEREST_RATE 연간 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 주식 데이터 수집
def collect_ecos_stock_data():
    df = collect_ecos_data('STOCK', 'M')
    if df is not None:
        print("\n=== STOCK 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_year = collect_ecos_data('STOCK', 'Y')
    if df_year is not None:
        print("\n=== STOCK 연간 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 성장률 데이터 수집
def collect_ecos_growth_rate_data():
    df = collect_ecos_data('GROWTH_RATE', 'Y')
    if df is not None:
        print("\n=== GROWTH_RATE 데이터 ===")
        print(df)
        insert_to_dataframe(df, 'Y')

#. ECOS 소득 데이터 수집
def collect_ecos_income_data():
    df = collect_ecos_data('INCOME', 'Y')
    if df is not None:
        print("\n=== INCOME 데이터 ===")
        print(df)
        insert_to_dataframe(df, 'Y')

#. ECOS 생산 데이터 수집
def collect_ecos_production_data():
    df = collect_ecos_data('PRODUCTION', 'M')
    if df is not None:
        print("\n=== PRODUCTION 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_year = collect_ecos_data('PRODUCTION', 'Y')
    if df_year is not None:
        print("\n=== PRODUCTION 연간 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 경기 데이터 수집
def collect_ecos_economy_data():
    df = collect_ecos_data('ECONOMY', 'M')
    if df is not None:
        print("\n=== ECONOMY 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_year = collect_ecos_data('ECONOMY', 'Y')
    if df_year is not None:
        print("\n=== ECONOMY 연간 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 고용/인구 데이터 수집
def collect_ecos_employ_population_data():
    df = collect_ecos_data('EMPLOY_POPULATION', 'M')
    if df is not None:
        print("\n=== EMPLOY_POPULATION 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_year = collect_ecos_data('EMPLOY_POPULATION', 'Y')
    if df_year is not None:
        print("\n=== EMPLOY_POPULATION 연간 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 대외거래 데이터 수집
def collect_ecos_foreign_trade_data():
    df = collect_ecos_data('FOREIGN_TRADE', 'M')
    if df is not None:
        print("\n=== FOREIGN_TRADE 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_year = collect_ecos_data('FOREIGN_TRADE', 'Y')
    if df_year is not None:
        print("\n=== FOREIGN_TRADE 연간 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 물가 데이터 수집
def collect_ecos_price_data():
    df = collect_ecos_data('PRICE', 'M')
    if df is not None:
        print("\n=== PRICE 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_year = collect_ecos_data('PRICE', 'Y')
    if df_year is not None:
        print("\n=== PRICE 연간 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 건설 데이터 수집
def collect_ecos_cement_data():
    df_year = collect_ecos_data('CEMENT', 'Y')
    if df_year is not None:
        print("\n=== CEMENT 데이터 ===")
        print(df_year)
        insert_to_dataframe(df_year, 'Y')

#. ECOS 고철 데이터 수집
def collect_ecos_scrap_metal_data():
    df = collect_ecos_data('SCRAP_METAL', 'M')
    if df is not None:
        print("\n=== SCRAP_METAL 월간 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')
    
    df_quarter = collect_ecos_data('SCRAP_METAL', 'Q')
    if df_quarter is not None:
        print("\n=== SCRAP_METAL 분기 데이터 ===")
        print(df_quarter)
        df_quarter = quarter_time_to_quarter(df_quarter)
        insert_to_dataframe(df_quarter, 'Q')

#. ECOS 심리 데이터 수집
def collect_ecos_trial_data():
    df = collect_ecos_data('TRIAL', 'M')
    if df is not None:
        print("\n=== TRIAL 데이터 ===")
        print(df)
        df = month_time_to_month(df)
        insert_to_dataframe(df, 'M')

######################
## task 정의
######################

# 한국은행 기준금리, 시장금리(콜금리, 국고채) 등 금리 관련 데이터 수집
t_collect_ecos_interest_rate_data = PythonOperator(
    task_id='collect_ecos_interest_rate_data',
    python_callable=collect_ecos_interest_rate_data,
    dag=init_dag
)

# KOSPI, KOSDAQ 지수 및 거래량/거래대금 데이터 수집
t_collect_ecos_stock_data = PythonOperator(
    task_id='collect_ecos_stock_data',
    python_callable=collect_ecos_stock_data,
    dag=init_dag
)

# GDP 실질성장률, 수출입 증가율  성장 관련 지표 수집
t_collect_ecos_growth_rate_data = PythonOperator(
    task_id='collect_ecos_growth_rate_data',
    python_callable=collect_ecos_growth_rate_data,
    dag=init_dag
)

# GDP, GNI, 1인당 국민총소득 등 소득 관련 지표 수집
t_collect_ecos_income_data = PythonOperator(
    task_id='collect_ecos_income_data',
    python_callable=collect_ecos_income_data,
    dag=init_dag
)

# 산업별 생산/출하/재고 지수, 제조업 생산능력 및 가동률 지수 수집
t_collect_ecos_production_data = PythonOperator(
    task_id='collect_ecos_production_data',
    python_callable=collect_ecos_production_data,
    dag=init_dag
)

# 승용차 판매, 설비투자지수, 기계수주액, 건설수주액 등 경기 관련 지표 수집
t_collect_ecos_economy_data = PythonOperator(
    task_id='collect_ecos_economy_data',
    python_callable=collect_ecos_economy_data,
    dag=init_dag
)

# 실업률, 고용률, 추계인구, 고령인구비율 등 고용/인구 관련 지표 수집
t_collect_ecos_employ_population_data = PythonOperator(
    task_id='collect_ecos_employ_population_data',
    python_callable=collect_ecos_employ_population_data,
    dag=init_dag
)

# 경상수지, 직접투자, 증권투자, 외환보유액, 대외채권/채무 등 대외거래 지표 수집
t_collect_ecos_foreign_trade_data = PythonOperator(
    task_id='collect_ecos_foreign_trade_data',
    python_callable=collect_ecos_foreign_trade_data,
    dag=init_dag
)

# 소비자물가지수, 생산자물가지수, 수출입물가지수, 국제상품가격 등 물가 관련 지표 수집
t_collect_ecos_price_data = PythonOperator(
    task_id='collect_ecos_price_data',
    python_callable=collect_ecos_price_data,
    dag=init_dag
)

# 국내 건설수주액 관련 지표(공공/민간, 건축/토목) 수집
t_collect_ecos_cement_data = PythonOperator(
    task_id='collect_ecos_cement_data',
    python_callable=collect_ecos_cement_data,
    dag=init_dag
)

# 건설투자, 건설기성액 등 건설 관련 지표 수집
t_collect_ecos_scrap_metal_data = PythonOperator(
    task_id='collect_ecos_scrap_metal_data',
    python_callable=collect_ecos_scrap_metal_data,
    dag=init_dag
)

# 소비자심리지수, 기업경기실사지수(BSI) 등 경제 심리 관련 지표 수집
t_collect_ecos_trial_data = PythonOperator(
    task_id='collect_ecos_trial_data',
    python_callable=collect_ecos_trial_data,
    dag=init_dag
)


task_start >> \
t_collect_ecos_interest_rate_data >> \
t_collect_ecos_stock_data >> \
t_collect_ecos_growth_rate_data >> \
t_collect_ecos_income_data >> \
t_collect_ecos_production_data >> \
t_collect_ecos_economy_data >> \
t_collect_ecos_employ_population_data >> \
t_collect_ecos_foreign_trade_data >> \
t_collect_ecos_price_data >> \
t_collect_ecos_cement_data >> \
t_collect_ecos_scrap_metal_data >> \
t_collect_ecos_trial_data >> \
task_end

