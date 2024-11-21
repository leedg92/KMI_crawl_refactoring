import sys, os, warnings
sys.path.append('/home/diquest/kmi_airflow/utils')

from config import *
from python_library import *
from bs4 import BeautifulSoup
import numpy as np


######################
## 기록
######################
## 본 데이터는 ihs economy 연간,월간 데이터를 수집
## URL = https://connect.ihsmarkit.com/
## 해당 ihs 사이트에서 로그인 후 mysaved 탭에 들어가서 
## 미리 설정된 economy 조건에 맞게 api 주소를 호출하여 사용
## * Tool Queries 명 : SLX_Global Economy, SLX_Global Economy_month

## 1) 수집주기.
## -> api 갱신일에 맞게 분기 or 월단위로 설정
## 본 데이터는 6개월에 1번씩 append 형식으로 수집 함(1월 1일, 7월 1일, 새벽 1시)

## 2) 증분조건.
## 수집기간에 따라 데이터 upsert 진행 
## * 수집기간 : 현재년도 기준 과거 1년치 부터 25년12월 까지로 설정 (수정 가능)



######################
## DAG 정의
######################

init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}

init_dag = DAG(
    dag_id = 'ihs_economy_data_collector',
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
    browser = set_webdriver_browser(opts)
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

##. ihs_economy_year (테이블 명 : fct_ihs_economy)
def func1():
    print('--' * 10, '(start) ihs_economy_year', '--' * 10)
    
    ## tool quiries 에 따라 변경 가능성 있음
    api_key_year = 'https://api.connect.ihsmarkit.com/shared/v1/databrowser/savedqueries/331390/Annual?pageSize=100&pageIndex=0'
    def_table_name = 'fct_ihs_economy'
    
    df = try_api(api_key_year)
    df.drop(columns=['Title', 'Date'], inplace=True)
    
    ## DB insert
    insert_to_dataframe(df, def_table_name)
    
    print('--' * 10, '(end) ihs_economy_year', '--' * 10)

##. ihs_economy_month (테이블 명 : ihs_economy_month)
def func2():
    print('--' * 10, '(start) ihs_economy_month', '--' * 10)
    
    ## tool quiries 에 따라 변경 가능성 있음
    api_key_month = 'https://api.connect.ihsmarkit.com/shared/v1/databrowser/savedqueries/331391/Annual?pageSize=100&pageIndex=0'
    def_table_name = 'fct_ihs_economy_month'
    
    df = try_api(api_key_month)
    df['MONTH'] = df['Date'].dt.month
    df['MONTH'] = df['MONTH'].astype(str).str.zfill(2)
    df.drop(columns=['Title', 'Date'], inplace=True)  
    
    ## DB insert
    insert_to_dataframe(df, def_table_name)
    
    print('--' * 10, '(end) ihs_economy_month', '--' * 10)
    

    
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