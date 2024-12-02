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
## 본 데이터는 Investing (BADI) 데이터를 수집한다
## URL = https://www.investing.com/indices/baltic-dry-historical-data
## 해당 URL 사이트에 접속해서 
## Baltic Dry Index Historical Data (BADI) 데이터를 크롤링 한다.


## 1) 수집주기.
## -> 일간, 주간, 월간


######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}
init_dag = DAG(
    dag_id = 'investing_bdi_index_collector',
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


def wait_for_csv_and_read(download_path, timeout=60):
    """
    지정된 다운로드 경로에서 CSV 파일을 기다린 후 읽어서 DataFrame으로 반환.
    :param download_path: 다운로드 폴더 경로
    :param timeout: 다운로드가 완료되기를 기다리는 최대 시간 (초 단위)
    :return: DataFrame 또는 None (타임아웃 시)
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        # 다운로드 폴더에서 CSV 파일 찾기
        for file_name in os.listdir(download_path):
            if file_name.endswith('.csv'):
                csv_file_path = os.path.join(download_path, file_name)
                print(f'다운로드 파일 발견: {csv_file_path}')
                try:
                    return file_name
                except Exception as e:
                    print(f'[error] CSV 파일 읽기 중 오류 발생: {e}')
                    return None
        time.sleep(1) 
    print(f'[warning] 타임아웃: {download_path}에서 CSV 파일을 찾을 수 없습니다.')
    return None
def random_sleep():
    time.sleep(random.uniform(1, 3))


def preprocessing_data(term, origin_df):
    df = origin_df.copy()

    columns_mapping = eval(conf['COLS_MAPPING'])
    df.rename(columns=columns_mapping, inplace=True)
    drop_col_name = ['VOL']
    df.drop(columns=drop_col_name, axis=1, inplace=True)

    df['PRICE'] = df['PRICE'].str.replace(',', '').astype(float)
    df['OPEN'] = df['OPEN'].str.replace(',', '').astype(float)
    df['HIGH'] = df['HIGH'].str.replace(',', '').astype(float)
    df['LOW'] = df['LOW'].str.replace(',', '').astype(float)
    df['CHANGE'] = df['CHANGE'].str.replace(',', '').str.rstrip('%').astype(float)

    datetime_col_name = 'DATE'
    df['YEAR'] = pd.to_datetime(df[datetime_col_name]).dt.year.astype(str).str.zfill(4)
    df['MONTH'] = pd.to_datetime(df[datetime_col_name]).dt.month.astype(str).str.zfill(2)
    if term == 'W':
        df['DAY'] = pd.to_datetime(df[datetime_col_name]).dt.day.astype(str).str.zfill(2)
        df['WEEK'] = pd.to_datetime(df[datetime_col_name]).apply(calculate_week_starting_sunday)
        
    elif term == 'D':
        df['DAY'] = pd.to_datetime(df[datetime_col_name]).dt.day.astype(str).str.zfill(2)
        df.drop(columns=datetime_col_name, axis=1, inplace=True)
        df = df.replace({np.nan: None})        
        
    
    df.drop(columns=datetime_col_name, axis=1, inplace=True)
    df = df.replace({np.nan: None})
        
    return df


    
def investing_file_download(time_frame):
    
    # Set selenium
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, INVESTING_DOWNLOAD_PATH)

    # URL Access    
    try:
        browser.get(INVESTING_URL)
        browser.implicitly_wait(10)
        time.sleep(0.5)
        
        # 다운로드 버튼 클릭
        download_btn_element = browser.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[2]/div[1]/div/span[2]')
        download_btn_element.click()
        
        # login 처리 
        sign_in_btn_element = browser.find_element(By.XPATH, '/html/body/div[4]/div/div/form/p[2]/button')
        sign_in_btn_element.click()
        
        print(f'[Login] Input User ID ~~~')
        user_id_input_element = WebDriverWait(browser, 20).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[4]/div/div/form/div[3]/input')))
        user_id_input_element.send_keys(INVESTING_USER_ID)
        random_sleep()
        # Login - Input USER PW
        print(f'[Login] Input User PW ~~~')
        user_pw_input_element = browser.find_element(By.XPATH, '/html/body/div[4]/div/div/form/div[5]/input')
        user_pw_input_element.send_keys(INVESTING_USER_PW)
        random_sleep()
        
        login_finish_signin_btn_element = browser.find_element(By.XPATH, '/html/body/div[4]/div/div/form/button')
        login_finish_signin_btn_element.click()
        
        
        # time frame 선택
        time_frame_element = browser.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/span') 
        time_frame_element.click()
        random_sleep()
        
        term_select_btn_element = browser.find_element(By.XPATH, time_frame) 
        term_select_btn_element.click()

        # 다운로드 버튼 클릭
        download_btn_element = browser.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[2]/div[1]/div/span[2]')
        download_btn_element.click()
        
        new_files = wait_for_csv_and_read(INVESTING_DOWNLOAD_PATH)
        
        return new_files       
        
        
    except Exception as e:
        print(e)
        browser.close()
        sys.exit()
    
    
##. [일간] Baltic Dry Index Historical Data (BADI) 데이터 (테이블 명 : fct_investing_bdi_index_daily)
def func1():
    print('--' * 10, '(start) investing_bdi_index_collector (daily)', '--' * 10)    
    
    time_frame_daliy = '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[1]/span'
    term = 'D'
    file_names = investing_file_download(time_frame_daliy)
    
    full_file_name = INVESTING_DOWNLOAD_PATH + file_names
    done_file_name = INVESTING_DESTINATION_PATH + term +'_' + get_year_month_day() + '_' + file_name
        
    origin_df = pd.read_csv(full_file_name, header=0, dtype=str)
    result_df = preprocessing_data(term , origin_df)

    val_list = []
    for index, result in result_df.iterrows():
        val_list.append(result.tolist())
    
    def_table_name = 'fct_investing_bdi_index_daily'
    upsert_to_dataframe(result_df, def_table_name, val_list)
    
    # 데이터 추출이 완료된 파일, done 으로 이동
    shutil.move(full_file_name, done_file_name)
    
    
    print('--' * 10, '(end) investing_bdi_index_collector (daily)', '--' * 10)
    
##. [주간] Baltic Dry Index Historical Data (BADI) 데이터 (테이블 명 : fct_investing_bdi_index_weekly)
def func2():
    print('--' * 10, '(start) investing_bdi_index_collector (weekly)', '--' * 10)    
    
    time_frame_weekly = '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[2]/span'    
    term = 'W'
    download_file = investing_file_download(time_frame_weekly)
    
    
    
    print('--' * 10, '(end) investing_bdi_index_collector (weekly)', '--' * 10)
    
##. [월간] Baltic Dry Index Historical Data (BADI) 데이터 (테이블 명 : fct_investing_bdi_index_monthly)
def func3():
    print('--' * 10, '(start) investing_bdi_index_collector (monthly)', '--' * 10)    
    
    time_frame_monthly = '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[3]/span'    
    term = 'M'
    download_file = investing_file_download(time_frame_monthly)
    
    print('--' * 10, '(end) investing_bdi_index_collector (monthly)', '--' * 10)
    


######################
## task 정의
######################
investing_bdi_index_daily = PythonOperator(
    task_id = 'investing_bdi_index_daily',
    python_callable = func1,
    dag = init_dag
)

# investing_bdi_index_weekly = PythonOperator(
#     task_id = 'investing_bdi_index_weekly',
#     python_callable = func2,
#     dag = init_dag
# )

# investing_bdi_index_monthly = PythonOperator(
#     task_id = 'investing_bdi_index_monthly',
#     python_callable = func3,
#     dag = init_dag
# )


task_start >> investing_bdi_index_daily >> task_end