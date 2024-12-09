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
## 본 데이터는 CLARKSONS Timeseries & Graphs 의 데이터를 수집 
## URL = https://oneksa.kr/shipping_index
## 해당 URL 사이트에 접속해서 
## Timeseries & Graphs 의 데이터를 crawring


## 1) 수집주기.
## -> 연간,분기,월간


######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 12, 6)
}
init_dag = DAG(
    dag_id = 'fct_clarksons_statics_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 * * *'
)
task_start = DummyOperator(task_id='start', dag=init_dag)
task_end = DummyOperator(task_id='end', dag=init_dag)



download_path = CLARKSONS_DOWNLOAD_PATH

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
def try_login(browser):
    try:
        # Login
        print(f'[Login] Trying to Login ~~~')
        login_btn_element = browser.find_element(By.XPATH, CLARKSONS_LOGIN_BTN)
        login_btn_element.click()

        # Login - Input USER ID
        print(f'[Login] Input User ID ~~~')
        user_id_input_element = browser.find_element(By.CSS_SELECTOR, '#usernameText')
        user_id_input_element.send_keys(CLARKSONS_USER_ID)
        continue_btn_element = browser.find_element(By.CSS_SELECTOR, 'button.btn:nth-child(3)')
        continue_btn_element.click()
        
        time.sleep(3)

        # Login - Input USER PW
        print(f'[Login] Input User PW ~~~')
        user_pw_input_element = WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#passwordText'))
        )
        user_pw_input_element.send_keys(CLARKSONS_USER_PW)
        login_finish_btn_element = browser.find_element(By.CSS_SELECTOR, 'button.btn')
        login_finish_btn_element.click()

        print(f'[Login] Success Login.')
    except Exception as e:
        print(e)          
        browser.quit()
        raise e
        
def try_move_page(browser):
    ## Move Page: TimeSeries & Graphs
    ## OUTPUT: X
    try:
        time.sleep(5)
        # TimeSeries & Graphs
        print(f'[Move Page] Moving \'TimeSeries & Graphs\' Page ~~~')
        tg_menu_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[1]/div[3]/div/div/ul/li[6]/a')))
        tg_menu_element.click()
        print(f'[Move Page] Success Move \'TimeSeries & Graphs\' Page')
    except Exception as e:
        print('[Crawling Fail: Move Page Failed]')
        browser.close()
        raise e
        
def try_close_popup(browser):
    try:
        print(f'[Popup Close] Closing Popup ~~~')
        popup_close_btn_element = WebDriverWait(browser, 30).until(EC.visibility_of_element_located((By.XPATH, '/html/body/app-root/div/app-cookie-banner/div/div/a')))
        popup_close_btn_element.click()
        print(f'[Popup Close] Popup Closed.')
    except Exception as e:
        print(f'[Popup Close] Can not find Popup.')
        browser.close()
        raise e

def try_download_excel(browser, frequncy):
    files_before = os.listdir(download_path)
    try:
        # Frequency Box
        is_exist_frequency_btn = False
        time.sleep(1)
        frequency_div_element = WebDriverWait(browser, 30).until(EC.element_to_be_clickable((By.XPATH, FREQUENCY_DIV_ELEMENT)))
        frequency_btn_elements = frequency_div_element.find_elements(By.XPATH, './button')
        for frequency_btn_element in frequency_btn_elements:
            print(frequency_btn_element.text.lower())
            if frequency_btn_element.text.lower() == frequncy.lower():
                # frequency_btn_element.click()
                clickable_frequeuncy_btn_element = WebDriverWait(browser, 30).until(EC.element_to_be_clickable(frequency_btn_element))
                clickable_frequeuncy_btn_element.click()
                is_exist_frequency_btn = True
                break
        if not is_exist_frequency_btn:
            return

        # File Download(Excel)
        download_panel_element = WebDriverWait(browser, 30).until(EC.visibility_of_element_located((By.XPATH, DOWNLOAD_PANEL_ELEMENT)))
        download_panel_elements = download_panel_element.find_elements(By.XPATH, './crsl-button')
        for download_panel_element in download_panel_elements:
            if download_panel_element.text.lower().replace(' ', '') == 'excel':
                clickable_excel_btn_element = WebDriverWait(browser, 30).until(EC.element_to_be_clickable(download_panel_element))
                clickable_excel_btn_element.click()
                break
        download_excel_accept_btn_element = WebDriverWait(browser, 30).until(EC.element_to_be_clickable((By.XPATH, DOWNLOAD_EXCEL_ACCEPT_BTN)))
        download_excel_accept_btn_element.click()

        print(f'[DOWNLOAD EXCEL] Download Path: {download_path}')
        print(f'[DOWNLOAD EXCEL] Before Download File List: {files_before}')
        print(f'[DOWNLOAD EXCEL] Waiting Download File ~~~')
        start_time = time.time()
        while True:
            time.sleep(1)  # 1초간 대기 후 다시 확인
            print(f'[DOWNLOAD EXCEL] While Download File List: {os.listdir(download_path)}')
            if all(not filename.endswith('.crdownload') for filename in os.listdir(download_path)) \
                    and bool(glob.glob(os.path.join(download_path, '*.xlsx'))):
                print(f'[DOWNLOAD EXCEL] Download Finished: {os.listdir(download_path)}')
                break
            elif time.time() - start_time > 120:
                print('[DOWNLOAD EXCEL] ({multiprocessing.current_process().pid})Time Over Downloading.')
                break
        files_after = os.listdir(download_path)
        print(f'[DOWNLOAD EXCEL] After Download File List: {files_after}')

        download_file_nm = [f for f in files_after if f not in files_before]
        return download_file_nm[0]
    except Exception as e:
        print(e)
        browser.close()

def preprocessing_annual_data(origin_df):
    df = origin_df.copy()

    # 컬럼명 지정
    new_cols = df.iloc[3:6, :].apply(lambda x: '!@#'.join(map(str, x)), axis=0)
    df.columns = new_cols.values

    # Date 열에서 날짜 타입만 추출
    df = df[pd.to_datetime(df.iloc[:, 0], errors='coerce').notnull()]
    df.rename(columns={df.columns[0]: 'DATE'}, inplace=True)

    result_df = df.melt(id_vars=['DATE'], var_name='TITLE', value_name='DATA_VALUE')
    result_df[['SERIAL_NUM', 'CONCEPT', 'UNIT']] = result_df['TITLE'].str.split('!@#', expand=True)
    result_df['YEAR'] = pd.to_datetime(result_df['DATE']).dt.year
    result_df.drop(columns=['TITLE', 'DATE'], inplace=True)
    result_df = result_df.replace({np.nan: None})

    return result_df
 
def preprocessing_quarterly_data(origin_df):
    df = origin_df.copy()

    # 컬럼명 지정
    new_cols = df.iloc[3:6, :].apply(lambda x: '!@#'.join(map(str, x)), axis=0)
    df.columns = new_cols.values
    # Date 열에서 날짜 타입만 추출
    df = df[pd.to_datetime(df.iloc[:, 0].apply(convert_quarter_str_to_date), errors='coerce').notnull()]
    df.rename(columns={df.columns[0]: 'DATE'}, inplace=True)

    result_df = df.melt(id_vars=['DATE'], var_name='TITLE', value_name='DATA_VALUE')

    result_df[['SERIAL_NUM', 'CONCEPT', 'UNIT']] = result_df['TITLE'].str.split('!@#', expand=True)
    result_df['YEAR'] = result_df['DATE'].apply(convert_quarter_str_to_date).dt.year
    result_df['QUARTER'] = result_df['DATE'].apply(convert_quarter_str_to_date).dt.quarter
    result_df.drop(columns=['TITLE', 'DATE'], inplace=True)
    result_df = result_df.replace({np.nan: None})

    return result_df

def preprocessing_monthly_data(origin_df):
    df = origin_df.copy()

    # 컬럼명 지정
    new_cols = df.iloc[3:6, :].apply(lambda x: '!@#'.join(map(str, x)), axis=0)
    df.columns = new_cols.values

    # Date 열에서 날짜 타입만 추출
    df = df[pd.to_datetime(df.iloc[:, 0], errors='coerce').notnull()]
    df.rename(columns={df.columns[0]: 'DATE'}, inplace=True)

    result_df = df.melt(id_vars=['DATE'], var_name='TITLE', value_name='DATA_VALUE')
    result_df[['SERIAL_NUM', 'CONCEPT', 'UNIT']] = result_df['TITLE'].str.split('!@#', expand=True)
    result_df['YEAR'] = pd.to_datetime(result_df['DATE']).dt.year
    result_df['MONTH'] = pd.to_datetime(result_df['DATE']).dt.month
    result_df.drop(columns=['TITLE', 'DATE'], inplace=True)
    result_df = result_df.replace({np.nan: None})

    return result_df

#. 수집할 통계 코드 조회
def get_clarksons_codes():
    
    conn = maria_kmi_dw_db_connection()
    query = """
        SELECT * FROM view_clarksons_path_view
    """
    print(f"\n[DEBUG] Executing query: {query}")
    
    codes_df = pd.read_sql(query, conn)
    print(f"\n[DEBUG] Query result: {codes_df}")
    
    conn.close()
    
    return codes_df.to_dict('records') 
           
def clarksons_file_download(frequency):
    
    browser = set_firefox_browser(CLARKSONS_DOWNLOAD_PATH)

    # URL Access
    browser.get(CLARKSONS_URL)
    browser.implicitly_wait(20)
        
    try_login(browser)
    time.sleep(3)
    try_move_page(browser)
    try_close_popup(browser)
    
    
    # Element Stack Define
    DATA_ELEMENT_STACK = []
    MAX_CHECK_BOX_CNT = 15
    
    
    processed_records = []

    df = get_clarksons_codes()
    for record in df:
        full_path_list = record['FULL_PATH'].split('|')
        processed_records.append(full_path_list)

    # 최종 다운받은 파일 명 리스트
    downloaded_files = []
    for each_data in processed_records:
        print(f'category_depth_list = {each_data}')
        category_top_element = WebDriverWait(browser, 30).until(EC.visibility_of_element_located((By.XPATH, CATEGORY_TOP_ELEMENT)))
        for category_value in each_data:
            category_ul_element = category_top_element.find_element(By.XPATH, './ul')
            category_li_elements = category_ul_element.find_elements(By.XPATH, './li')
            for category_li_element in category_li_elements:
                # 선택한 li의 라벨 텍스트
                category_li_text_element = category_li_element.find_element(By.XPATH, './div')
                if category_li_text_element.text.replace(' ', '') == category_value.replace(' ', ''):
                    if category_li_text_element.text.replace(' ', '') == each_data[-1].replace(' ', ''):
                        if len(DATA_ELEMENT_STACK) == MAX_CHECK_BOX_CNT:
                            for pre_clicked_element in DATA_ELEMENT_STACK:
                                browser.execute_script('arguments[0].scrollIntoView();', pre_clicked_element)
                                pre_clicked_element.click()
                            DATA_ELEMENT_STACK.clear()

                        category_check_box_element = category_li_element.find_element(By.XPATH, CATEGORY_CHECK_BOX_ELEMENT)
                        browser.execute_script('arguments[0].scrollIntoView();', category_check_box_element)
                        category_check_box_element.click()
                        DATA_ELEMENT_STACK.append(category_check_box_element)
                        print(f'The Check Box({category_li_text_element.text}) is Checked: {category_check_box_element.is_selected()}')
                        if not category_check_box_element.is_selected():
                            print(f'[warning] The Check Box is not Checked: {category_li_text_element.text}')

                        if len(DATA_ELEMENT_STACK) == MAX_CHECK_BOX_CNT \
                                or category_value.replace(' ', '') == processed_records[-1][-1].replace(' ', ''):
                            download_file_name = try_download_excel(browser, frequency)
                            downloaded_files.append(download_file_name)

                    if category_li_element.get_attribute('aria-expanded') == 'false':
                        category_li_toggle_element = WebDriverWait(category_li_element, 10).until(EC.element_to_be_clickable((By.XPATH, CATEGORY_TOGGLE_ELEMENT)))
                        browser.execute_script('arguments[0].scrollIntoView();', category_li_toggle_element)
                        category_li_toggle_element.click()
                    category_top_element = category_li_element

    return downloaded_files




##. CLARKSONS Timeseries & Graphs 의 데이터 (연간) (테이블 명 : fct_clarksons_statics_year)
def func1():
    print('--' * 10, '(start) clarksons_statics_year (year)', '--' * 10)    
    
    frequency = 'annual'
    
    file_names = clarksons_file_download(frequency)
    print(file_names)
    
    
    print('--' * 10, ' download success ', '--' * 10)    
    for file_name in file_names:
        full_file_name = CLARKSONS_DOWNLOAD_PATH + file_name
        done_file_name = CLARKSONS_DESTINATION_PATH + frequency +'_' + get_year_month_day() + '_' + file_name
        
        origin_df = pd.read_excel(full_file_name, header=None)
        result_df = preprocessing_annual_data(origin_df)    

        print(result_df)
        val_list = []
        for index, result in result_df.iterrows():
            val_list.append(result.tolist())
        
        def_table_name = 'fct_clarksons_statics_year'
        upsert_to_dataframe(result_df, def_table_name, val_list)
        
        # 데이터 추출이 완료된 파일, done 으로 이동
        shutil.move(full_file_name, done_file_name)
    
    print('--' * 10, '(end) clarksons_statics_year (year)', '--' * 10)
    

##. CLARKSONS Timeseries & Graphs 의 데이터 (분기) (테이블 명 : fct_clarksons_statics_quarter)
def func2():
    print('--' * 10, '(start) clarksons_statics_quarter (quarter)', '--' * 10) 
    
    frequency = 'quarterly'
    
    file_names = clarksons_file_download(frequency)
    print(file_names)
    
    
    print('--' * 10, ' download success ', '--' * 10)    
    for file_name in file_names:
        full_file_name = CLARKSONS_DOWNLOAD_PATH + file_name
        done_file_name = CLARKSONS_DESTINATION_PATH + frequency +'_' + get_year_month_day() + '_' + file_name
        
        origin_df = pd.read_excel(full_file_name, header=None)
        result_df = preprocessing_quarterly_data(origin_df)    
    
        val_list = []
        for index, result in result_df.iterrows():
            val_list.append(result.tolist())
        
        def_table_name = 'fct_clarksons_statics_quarter'
        upsert_to_dataframe(result_df, def_table_name, val_list)
        
        # 데이터 추출이 완료된 파일, done 으로 이동
        shutil.move(full_file_name, done_file_name)   
    
    print('--' * 10, '(end) clarksons_statics_quarter (quarter)', '--' * 10)
    
##. CLARKSONS Timeseries & Graphs 의 데이터 (월간) (테이블 명 : fct_clarksons_statics_month)
def func3():
    print('--' * 10, '(start) clarksons_statics_month (month)', '--' * 10)    
    
    frequency = 'monthly'
    
    file_names = clarksons_file_download(frequency)
    print(file_names)
    
    
    print('--' * 10, ' download success ', '--' * 10)    
    for file_name in file_names:
        full_file_name = CLARKSONS_DOWNLOAD_PATH + file_name
        done_file_name = CLARKSONS_DESTINATION_PATH + frequency +'_' + get_year_month_day() + '_' + file_name
        
        origin_df = pd.read_excel(full_file_name, header=None)
        result_df = preprocessing_monthly_data(origin_df)    
    
        val_list = []
        for index, result in result_df.iterrows():
            val_list.append(result.tolist())
        
        def_table_name = 'fct_clarksons_statics_month'
        upsert_to_dataframe(result_df, def_table_name, val_list)
        
        # 데이터 추출이 완료된 파일, done 으로 이동
        shutil.move(full_file_name, done_file_name)
    
    print('--' * 10, '(end) clarksons_statics_month (month)', '--' * 10)
    


######################
## task 정의
######################
fct_clarksons_statics_year = PythonOperator(
    task_id = 'fct_clarksons_statics_year',
    python_callable = func1,
    dag = init_dag
)

fct_clarksons_statics_quarter = PythonOperator(
    task_id = 'fct_clarksons_statics_quarter',
    python_callable = func2,
    dag = init_dag
)

fct_clarksons_statics_month = PythonOperator(
    task_id = 'fct_clarksons_statics_month',
    python_callable = func3,
    dag = init_dag
)


task_start >> \
fct_clarksons_statics_year >> \
fct_clarksons_statics_quarter >> \
fct_clarksons_statics_month >> \
task_end
