import sys, os, warnings
## 서버용 경로
sys.path.append('/opt/airflow/dags/utils')
from config import *
from python_library import *
from api_account import *
from bs4 import BeautifulSoup
import numpy as np
import pymysql

######################
## 기록
######################
## 본 데이터는 ihs economy_new 연간,월간 데이터를 수집
## URL = https://connect.ihsmarkit.com/
## 해당 ihs 사이트에서 로그인 후 mysaved 탭에 들어가서
## 미리 설정된 economy_new 조건에 맞게 api 주소를 호출하여 사용
## * Tool Queries 명 : SLX_Global Economy_NEW_Annual, SLX_Global Economy_NEW_Monthly

## 1) 수집주기.
## -> api 갱신일에 맞게 분기 or 월단위로 설정

## 2) 증분조건.
## 수집기간에 따라 데이터 upsert 진행
## * 수집기간 : 현재년도 기준 +- 1년

######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11),
    'retries': 1
}
init_dag = DAG(
    dag_id = 'ihs_economy_new_data_collector',
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
def upsert_to_dataframe(result_dataframe, table_name):  
    conn = maria_kmi_dw_db_connection()    
    col_list = result_dataframe.columns.tolist()
    row_list = result_dataframe.values.tolist()  
    
    # INSERT SQL 쿼리 기본 부분
    columns = ', '.join([f"`{col}`" for col in col_list])
    values = ', '.join(['%s'] * len(col_list))
    
    # ON DUPLICATE KEY UPDATE 부분 생성
    update_columns = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in col_list if col != 'CREATE_DTM'])
    
    # 전체 SQL 쿼리 구성
    upsert_sql = f"""
    INSERT INTO {table_name} ({columns})
    VALUES ({values})
    ON DUPLICATE KEY UPDATE {update_columns}, `UPDATE_DTM`=NOW()
    """
    try:
        # 데이터베이스 연결을 사용하여 쿼리 실행
        with conn.cursor() as cur:
            cur.executemany(upsert_sql, row_list)            
            conn.commit()
            print(upsert_sql)
            print(f"insert data at {table_name} : success ★☆★☆")
            return True
        
    except pymysql.Error as e:        
        conn.rollback()
        return False

def try_login(browser, api_key):

    try:
        # Login
        print(f'[Login] Trying to Login ~~~')
        login_btn_element = browser.find_element(By.XPATH, IHS_LOGIN_BTN_ELEMENT)
        login_btn_element.click()
        browser.implicitly_wait(10)
        # Login - Input USER ID
        print(f'[Login] Input User ID ~~~')
        user_id_input_element = browser.find_element(By.XPATH, IHS_USER_ID_INPUT_ELEMENT)
        user_id_input_element.send_keys(IHS_USER_ID)
        login_continue_btn_element = browser.find_element(By.XPATH, IHS_LOGIN_CONTINUE_BTN_ELEMENT)
        login_continue_btn_element.click()
        browser.implicitly_wait(10)
        # Login - Input USER PW
        print(f'[Login] Input User PW ~~~')
        user_pw_input_element = browser.find_element(By.XPATH, IHS_USER_PW_INPUT_ELEMENT)
        user_pw_input_element.send_keys(IHS_USER_PW)
        login_finish_btn_element = browser.find_element(By.XPATH, IHS_LOGIN_FINISH_BTN_ELEMENT)
        login_finish_btn_element.click()
        browser.implicitly_wait(20)
        print(f'[Login] Success Login.')
        browser.get(api_key)
    except Exception as e:
        import traceback
        tb = traceback.extract_tb(e.__traceback__)
        line = tb[-1].lineno
        print(f'[error][line : {line}] : {e}')
        print(f'[error][Crawling Fail: Login Filed]')
        browser.quit()
        sys.exit()

def try_api(api_key):    
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, IHS_NEW_DOWNLOAD_PATH)
    browser.get(IHS_URL)
    browser.implicitly_wait(10)
    try_login(browser, api_key)
    
    browser.get(api_key)
    browser_result = browser.page_source        
    soup = BeautifulSoup(browser_result, 'html.parser')                
    body_content = soup.find('body')
            
    if body_content:
        raw_json_data = body_content.get_text(strip=True)        
        json_data = json.loads(raw_json_data)
        
        df = pd.DataFrame(json_data)
        df['Date'] = pd.to_datetime(df['Date'])
        df['YEAR'] = df['Date'].dt.year
        df['YEAR'] = df['YEAR'].astype(str).str.zfill(4)
        df.rename(columns={'SourceGeographicLocation': 'GEOGRAPHY'}, inplace=True)
        df.rename(columns={'EconomicConcept': 'CONCEPT'}, inplace=True)
        df.rename(columns={'Frequency': 'FREQUENCY'}, inplace=True)
        df.rename(columns={'Value': 'DATA_VALUE'}, inplace=True)
        df.rename(columns={'Unit': 'UNIT'}, inplace=True)
        df = df.replace({np.nan: None})
        return df
    
##. ihs_economy_new_year (테이블 명 : fct_ihs_economy_new)
def func1():
    print('--' * 10, '(start) ihs_economy_new_year', '--' * 10)
    ## tool quiries 에 따라 변경 가능성 있음
    ## saved tool name: SLX_Global Economy_NEW_Annual
    api_key_year = 'https://api.connect.ihsmarkit.com/shared/v1/databrowser/savedqueries/331416/Annual?pageSize=100&pageIndex=0'
    def_table_name = 'fct_ihs_economy_new'
    df = try_api(api_key_year)
    df.drop(columns=['Title', 'Date'], inplace=True)
    
    ## DB insert
    upsert_to_dataframe(df, def_table_name)
    print('--' * 10, '(end) ihs_economy_new_year', '--' * 10)
    

def func2():
    print('--' * 10, '(start) ihs_economy_new_month', '--' * 10)
    ## tool quiries 에 따라 변경 가능성 있음
    ## saved tool name: SLX_Global Economy_NEW_Monthly
    api_key_month = 'https://api.connect.ihsmarkit.com/shared/v1/databrowser/savedqueries/331421/Annual?pageSize=100&pageIndex=0'
    def_table_name = 'fct_ihs_economy_new_month'
    df = try_api(api_key_month)
    df['MONTH'] = df['Date'].dt.month
    df['MONTH'] = df['MONTH'].astype(str).str.zfill(2)
    df.drop(columns=['Title', 'Date'], inplace=True)  
    
    ## DB insert
    upsert_to_dataframe(df, def_table_name)
    print('--' * 10, '(end) ihs_economy_new_month', '--' * 10)

######################
## task 정의
######################
ihs_economy_year = PythonOperator(
    task_id = 'ihs_economy_year',
    python_callable = func1,
    dag = init_dag
)

ihs_economy_month = PythonOperator(
    task_id = 'ihs_economy_month',
    python_callable = func2,
    dag = init_dag
)

task_start >> ihs_economy_year >> ihs_economy_month >> task_end