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
## 본 데이터는 한국철강협회(KOSA) ERS_RMTR_STTS (철 스크랩 데이터) 
## URL = https://stat.kosa.or.kr/ers/rmtr/ErsRmtrStts
## 해당 URL 사이트에 접속해서 
## 철 스크랩 데이터를 수집한다.


## 1) 수집주기.
## -> 월간, 연간 (2010.01 ~ )



######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}
init_dag = DAG(
    dag_id = 'ers_rmtr_stts_collector',
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
        
def wait_for_xls_and_read(download_path, timeout=120):
    """
    지정된 다운로드 경로에서 XLSX 파일을 기다린 후 읽어서 DataFrame으로 반환.
    :param download_path: 다운로드 폴더 경로
    :param timeout: 다운로드가 완료되기를 기다리는 최대 시간 (초 단위)
    :return: DataFrame 또는 None (타임아웃 시)
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        # 다운로드 폴더에서 XLSX 파일 찾기
        for file_name in os.listdir(download_path):
            if file_name.endswith('.xlsx'):
                xlsx_file_path = os.path.join(download_path, file_name)
                print(f'다운로드 파일 발견: {xlsx_file_path}')
                try:
                    return file_name
                except Exception as e:
                    print(f'[error] XLSX 파일 읽기 중 오류 발생: {e}')
                    return None
        time.sleep(1) 
    print(f'[warning] 타임아웃: {download_path}에서 XLSX 파일을 찾을 수 없습니다.')
    return None

def try_login(browser):
    # Login
    print(f'[Login] Trying to Login ~~~')
    login_btn_element = browser.find_element(By.XPATH, ERS_LOGIN_BTN_ELEMENT)
    login_btn_element.click()

    # Login - Input USER ID
    print(f'[Login] Input User ID ~~~')
    user_id_input_element = browser.find_element(By.XPATH, ERS_USER_ID_INPUT_ELEMENT)
    user_id_input_element.send_keys(ERS_USER_ID)

    # Login - Input USER PW
    print(f'[Login] Input User PW ~~~')
    user_pw_input_element = browser.find_element(By.XPATH, ERS_USER_PW_INPUT_ELEMENT)
    user_pw_input_element.send_keys(ERS_USER_PW)
    
    # Login Finish BTN click
    print(f'[Login] Login Finish BTN click ~~~')
    login_finish_btn_element = browser.find_element(By.XPATH, ERS_LOGIN_FINISH_BTN_ELEMENT)
    login_finish_btn_element.click()

    print(f'[Login] Success Login.')

def correct_date_format(x):
    if isinstance(x, float):
        x = str(x)
    if isinstance(x, str) and x.count('.') == 1:
        year, month = x.split('.')
        # 소수점 뒤에 한 자릿수이면 '10'으로 맞춤
        if month == '1':  # 2010.1 -> 2010.10 수정
            return f'{year}.10'
        return f'{year}.{month.zfill(2)}'  # 나머지는 두 자리로 맞춤
    return x 
 
def ers_rmtr_stts_download(browser, term):
    
    try:
        
        files_before = os.listdir(ERS_DOWNLOAD_PATH)
        
        login_btn_element = browser.find_element(By.XPATH, ERS_LOGIN_BTN_ELEMENT)
        time.sleep(3)
        #로그인 버튼이 있는지 체크
        # 로그인 처리
        if login_btn_element:
            try_login(browser)
            
        # Move Page - Data Search Page
        time.sleep(3)
        print(f'[Move Page] Moving Search Page ~~~')

        menu_survey_statistics_category_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_MENU_SURVEY_STATISTICS_CATEGORY_ELEMENT)))
        actions = ActionChains(browser) # Mouse Over Event
        actions.move_to_element(menu_survey_statistics_category_element).perform()

        time.sleep(3)

        inner_menu_raw_material_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_INNER_MENU_RAW_MATERIAL_ELEMENT)))
        inner_menu_raw_material_element.click()
        
        time.sleep(3)
        
        # 철 스크랩 체크박스 선택
        steel_check_box_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_STEEL_CHECK_BOX_ELEMENT)))
        steel_check_box_element.click()        
        # 시점 버튼
        date_setting_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_DATE_SETTING_BTN_ELEMENT)))
        date_setting_btn_element.click()
        
        if term == 'Y':
            print(f'[Date Setting] Year Setting ~~~')
            year_setting_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_YEAR_SETTING_BTN_ELEMENT)))
            year_setting_btn_element.click()
            
            # Start Year set
            start_year_select_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_START_YEAR_SELECT_BTN_ELEMENT)))
            select = Select(start_year_select_btn_element)
            select.select_by_value(ERS_START_YEAR)  
            
            print(f'[Date Setting] DATE_ALL_SELECT_YEAR_BTN_ELEMENT ~~~')
            date_all_select_year_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_DATE_ALL_SELECT_YEAR_BTN_ELEMENT)))
            date_all_select_year_btn_element.click()           
            
            
        elif term == 'M':
            print(f'[Date Setting] Month Search ~~~')
            month_setting_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_MONTH_SETTING_BTN_ELEMENT)))
            month_setting_btn_element.click()
            
            # Start Year set
            start_year_select_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_START_YEAR_SELECT_BTN_ELEMENT)))
            select = Select(start_year_select_btn_element)
            select.select_by_value(ERS_START_YEAR)
            
            # Start Month set
            start_month_select_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_START_MONTH_SELECT_BTN_ELEMENT)))
            select = Select(start_month_select_btn_element)
            select.select_by_value(ERS_START_MONTH)
            
            print(f'[Date Setting] DATE_ALL_SELECT_MONTH_BTN_ELEMENT ~~~')
            date_all_select_month_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_DATE_ALL_SELECT_MONTH_BTN_ELEMENT)))
            date_all_select_month_btn_element.click()
            
            
        print(f'[Date Setting] DATE_APPLY_BTN_ELEMENT ~~~')
        date_apply_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_DATE_APPLY_BTN_ELEMENT)))
        date_apply_btn_element.click()
        
        print(f'[Unit Setting] UNIT_SELECT_BTN_ELEMENT ~~~')
        unit_select_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_UNIT_SELECT_BTN_ELEMENT)))
        unit_select_btn_element.click()
        
        print(f'[Unit Setting] UNIT_TON_SELECT_ELEMENT ~~~')
        unit_ton_select_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_UNIT_TON_SELECT_ELEMENT)))
        unit_ton_select_element.click()
        
        time.sleep(60)
        
        print(f'[Excel] EXCEL_SAVE_BTN_ELEMENT ~~~')
        excel_save_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, ERS_EXCEL_SAVE_BTN_ELEMENT)))
        print(excel_save_btn_element)
        excel_save_btn_element.click()
        
        wait_for_xls_and_read(ERS_DOWNLOAD_PATH)
        files_after = os.listdir(ERS_DOWNLOAD_PATH)

        # 다운 받은 파일 명
        new_files = [f for f in files_after if f not in files_before]

        return new_files

    except Exception as e:
        print(e)
        browser.close()
        sys.exit()

def preprocessing_data(origin_df, term):
    result_df = origin_df.copy()        
    columns_mapping = ERS_COLS_MAPPING
    result_df.rename(columns=columns_mapping, inplace=True)
    
    if term == 'M':
        if 'YEAR' in result_df.columns:
            result_df['MONTH'] = result_df['YEAR'].apply(lambda x: f"{str(x).split('.')[1].zfill(2)}" if '.' in str(x) else '00')
            result_df['YEAR'] = result_df['YEAR'].apply(lambda x: str(x).split('.')[0] if '.' in str(x) else str(x))
            
    result_df.fillna(0, inplace=True)    
    result_df.replace('', 0, inplace=True)
    result_df['UNIT'] = 'ton'
    return result_df
        
###철 스크랩 데이터 (연간)
def func1():
    print('--' * 10, '(start) ers_rmtr_stts_collector (year)', '--' * 10)    
    
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, ERS_DOWNLOAD_PATH)

    # URL Access    
    try:
        browser.get(ERS_URL)
        browser.implicitly_wait(10)
        time.sleep(0.5)
        
        term = 'Y'
        download_files = ers_rmtr_stts_download(browser, term)
        
        for file_name in download_files:
            full_file_name = ERS_DOWNLOAD_PATH + file_name
            done_file_name = ERS_DESTINATION_PATH + get_year_month_day() + '_' + term + '_' + file_name

            origin_df = pd.read_excel(full_file_name, skiprows=3, header=0, engine='openpyxl', dtype={'시점': str})
            result_df =  preprocessing_data(origin_df, term)

            val_list = []
            for index, result in result_df.iterrows():
                val_list.append(result.tolist())
            
            def_table_name = 'fct_ers_rmtr_stts_year'
            upsert_to_dataframe(result_df, def_table_name, val_list)
            
            # 데이터 추출이 완료된 파일, done 으로 이동
            shutil.move(full_file_name, done_file_name)
        
    except Exception as e:
        print(e)
        browser.close()
        sys.exit()
    
    print('--' * 10, '(end) ers_rmtr_stts_collector (year)', '--' * 10)    
    
###철 스크랩 데이터 (월간)
def func2():
    print('--' * 10, '(start) ers_rmtr_stts_collector (month)', '--' * 10)    
    
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, ERS_DOWNLOAD_PATH)

    # URL Access    
    try:
        browser.get(ERS_URL)
        browser.implicitly_wait(10)
        time.sleep(0.5)
        
        term = 'M'
        download_files = ers_rmtr_stts_download(browser, term)
        
        for file_name in download_files:
            full_file_name = ERS_DOWNLOAD_PATH + file_name
            done_file_name = ERS_DESTINATION_PATH + get_year_month_day() + '_' + term + '_' + file_name

            origin_df = pd.read_excel(full_file_name, skiprows=3, header=0, engine='openpyxl', dtype={'시점': str})
            result_df =  preprocessing_data(origin_df, term)

            val_list = []
            for index, result in result_df.iterrows():
                val_list.append(result.tolist())
            
            def_table_name = 'fct_ers_rmtr_stts_month'
            upsert_to_dataframe(result_df, def_table_name, val_list)
            
            # 데이터 추출이 완료된 파일, done 으로 이동
            shutil.move(full_file_name, done_file_name)
        
    except Exception as e:
        print(e)
        browser.close()
        sys.exit()
    
    print('--' * 10, '(end) ers_rmtr_stts_collector (month)', '--' * 10)



######################
## task 정의
######################

###철 스크랩 데이터 (연간)
ers_rmtr_stts_year = PythonOperator(
    task_id = 'ers_rmtr_stts_year',
    python_callable = func1,
    dag = init_dag
)
###철 스크랩 데이터 (월간)
ers_rmtr_stts_month = PythonOperator(
    task_id = 'ers_rmtr_stts_month',
    python_callable = func2,
    dag = init_dag
)

task_start >> ers_rmtr_stts_year >> ers_rmtr_stts_month >> task_end