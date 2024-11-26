import sys, os, warnings
## 서버용 경로
sys.path.append('/opt/airflow/dags/utils')
from config import *
from python_library import *
from bs4 import BeautifulSoup
import numpy as np
import pymysql

######################
## 기록
######################
## 본 데이터는 KSA 해상운임 지수 데이터를 수집 (일간)
## URL = https://oneksa.kr/shipping_index
## 해당 URL 사이트에 접속해서 
## 해상운임 지수 데이터를 crawring 한다


## 1) 수집주기.
## -> 일간


######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}
init_dag = DAG(
    dag_id = 'ksa_sea_freight_index_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 * * *'
)
task_start = DummyOperator(task_id='start', dag=init_dag)
task_end = DummyOperator(task_id='end', dag=init_dag)


######################
## Function 정의
######################
def upsert_to_dataframe(result_dataframe, table_name, val_list):  
    conn = maria_kmi_dw_db_connection()    
    col_list = result_dataframe.columns.tolist()
    row_list = val_list  
    
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
        print(e)       
        conn.rollback()
        return False
    
    finally:
        conn.close()

def ksa_file_download():
    # Set selenium
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, KSA_DOWNLOAD_PATH)

    # URL Access    
    try:
        browser.get(KSA_URL)
        browser.implicitly_wait(10)
        time.sleep(0.5)

        select_date_div_element = browser.find_element(By.XPATH, '/html/body/div[1]/div/section/form/div/div')
        download_excel_btn_element = select_date_div_element.find_element(By.XPATH, './a[2]')

        start_date_element = browser.find_element(By.XPATH, '/html/body/div[1]/div/section/form/div/div/div/div[1]/input')
        start_date_element.click()
        
        #2010 - 2019 date seetting
        start_year_element = browser.find_element(By.XPATH, '/html/body/div[2]/div[1]/nav/div[2]')
        browser.execute_script("arguments[0].innerText = '2010 - 2019';", start_year_element)
        
        start_date_title_element = browser.find_element(By.XPATH, '/html/body/div[2]/div[1]/nav/div[2]')
        start_date_title_element.click()
        start_date_title_element = browser.find_element(By.XPATH, '/html/body/div[2]/div[1]/nav/div[2]')
        start_date_title_element.click()

        start_date_year_elements = browser.find_elements(By.XPATH, '/html/body/div[2]/div[1]/div/div[3]/div/div')
        for start_date_year_element in start_date_year_elements:
            #if start_date_year_element.text == str(int(get_year()) - 1):
            if start_date_year_element.text == str(2011):
                start_date_year_element.click()
                break
        start_date_month_elements = browser.find_elements(By.XPATH, '/html/body/div[2]/div[1]/div/div[2]/div/div')
        for start_date_month_element in start_date_month_elements:
            if start_date_month_element.text == '1' + '월':
                start_date_month_element.click()
                break
        start_date_day_elements = browser.find_elements(By.XPATH, '/html/body/div[2]/div[1]/div/div[1]/div[2]/div')
        for start_date_day_element in start_date_day_elements:
            if start_date_day_element.text == '1':
                start_date_day_element.click()
                break

        today_month = str(int(get_month()))
        
        end_date_element = browser.find_element(By.XPATH, '/html/body/div[1]/div/section/form/div/div/div/div[2]/input')
        end_date_element.click()
        end_date_title_element = browser.find_element(By.XPATH, '/html/body/div[2]/div[2]/nav/div[2]')
        end_date_title_element.click()
        end_date_month_elements = browser.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div/div[2]/div/div')
        for end_date_month_element in end_date_month_elements:
            if end_date_month_element.text == today_month + '월':
                end_date_month_element.click()
                break
        end_date_day_elements = browser.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div/div/div[2]/div')
        for end_date_day_element in end_date_day_elements:
            if end_date_day_element.text == get_day():
                end_date_day_element.click()
                break
        
        download_excel_btn_element.click()
        
        new_files = wait_for_xls_and_read(KSA_DOWNLOAD_PATH)

        return new_files
    except Exception as e:
        browser.close()
        sys.exit()

def wait_for_xls_and_read(download_path, timeout=60):
    """
    지정된 다운로드 경로에서 XLS 파일을 기다린 후 읽어서 DataFrame으로 반환.
    :param download_path: 다운로드 폴더 경로
    :param timeout: 다운로드가 완료되기를 기다리는 최대 시간 (초 단위)
    :return: DataFrame 또는 None (타임아웃 시)
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        # 다운로드 폴더에서 XLS 파일 찾기
        for file_name in os.listdir(download_path):
            if file_name.endswith('.xls'):
                xls_file_path = os.path.join(download_path, file_name)
                print(f'다운로드 파일 발견: {xls_file_path}')
                try:
                    return file_name
                except Exception as e:
                    print(f'[error] XLS 파일 읽기 중 오류 발생: {e}')
                    return None
        time.sleep(1) 
    print(f'[warning] 타임아웃: {download_path}에서 XLS 파일을 찾을 수 없습니다.')
    return None

def preprocessing_data(origin_df):
    
    df = origin_df.copy()

    result_df = df.iloc[:, 1:]
    result_df['YEAR'] = pd.to_datetime(df.iloc[:, 0]).dt.year
    result_df['MONTH'] = pd.to_datetime(df.iloc[:, 0]).dt.month.astype(str).str.zfill(2)
    result_df['DAY'] = pd.to_datetime(df.iloc[:, 0]).dt.day.astype(str).str.zfill(2)
    result_df.rename(columns={
        'SCFI(USWC)': 'SCFI_USWC',
        'SCFI(Europe)':	'SCFI_EUROPE',
        'SCFI(USEC)': 'SCFI_USEC'
    }, inplace=True)
    
    result_df = result_df.replace({'-': None})    
    result_df['BDI'] = result_df['BDI'].replace(',', '')
    result_df['SCFI_EUROPE'] = result_df['SCFI_EUROPE'].astype(str).str.replace(',,', ',')
    result_df['SCFI_EUROPE'] = result_df['SCFI_EUROPE'].replace(',', '')
    result_df['SCFI_EUROPE'] = pd.to_numeric(result_df['SCFI_EUROPE'], errors='coerce')  # 숫자로 변환
    result_df = result_df.replace({np.nan: None})

    return result_df    
    
##. KSA 해상운임 지수 데이터 (테이블 명 : fct_ksa_sea_freight_index)
def func1():
    print('--' * 10, '(start) ksa_sea_freight_index_collector', '--' * 10)    
    
    
    file_names = ksa_file_download()
    print('--' * 10, ' download success ', '--' * 10)    
    
    full_file_name = KSA_DOWNLOAD_PATH + file_names
    done_file_name = KSA_DESTINATION_PATH + get_year_month_day() + '_' + file_names
        
    origin_df = pd.read_html(full_file_name, encoding='utf-8')[0]
    result_df = preprocessing_data(origin_df)

    print(result_df)
    val_list = []
    for index, result in result_df.iterrows():
        val_list.append(result.tolist())
    
    def_table_name = 'fct_ksa_sea_freight_index'
    upsert_to_dataframe(result_df, def_table_name, val_list)
    
    # 데이터 추출이 완료된 파일, done 으로 이동
    shutil.move(full_file_name, done_file_name)
    
    print('--' * 10, '(end) ksa_sea_freight_index_collector', '--' * 10)
    


######################
## task 정의
######################
ksa_sea_freight_index = PythonOperator(
    task_id = 'ksa_sea_freight_index',
    python_callable = func1,
    dag = init_dag
)


task_start >> ksa_sea_freight_index >> task_end