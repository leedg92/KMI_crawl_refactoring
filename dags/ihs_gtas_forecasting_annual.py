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
## 본 데이터는 ihs_gtas_forecasting_data_collector 연간 데이터를 수집
## URL = https://connect.ihsmarkit.com/
## 해당 ihs 사이트에서 로그인 후 mysaved 탭에 들어가서
## 미리 설정된 economy 조건에 맞게 api 주소를 호출하여 사용
## * Tool Queries 명 : SLX_GTAS_FORECASTING_Annual

## 1) 수집주기.
## -> api 갱신일에 맞게 분기 or 월단위로 설정

## 2) 증분조건.
## 수집기간에 따라 데이터 upsert 진행
## * 수집기간 : 현재년도 기준 +- 1년치

######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}
init_dag = DAG(
    dag_id = 'ihs_gtas_forecasting_annual_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 * * *'
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

def try_login(browser):

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
        
    except Exception as e:
        import traceback
        tb = traceback.extract_tb(e.__traceback__)
        line = tb[-1].lineno
        print(f'[error][line : {line}] : {e}')
        print(f'[error][Crawling Fail: Login Filed]')
        browser.quit()
        sys.exit()

def try_move_page(browser, tool_query_nm):
    ## Move Page: Data Search Page -> Saved Query Page
    ## OUTPUT: Tool Query Table Rows
    try:
        # Move Page - Data Search Page
        print('[Move Page] Moving Search Page ~~~')

        menu_maritime_trade_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_MENU_MARITIME_TRADE_ELEMENT)))
        actions = ActionChains(browser) # Mouse Over Event
        actions.move_to_element(menu_maritime_trade_element).perform()
        
        time.sleep(5)

        inner_menu_gtas_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_INNER_MENU_GTAS_ELEMENT)))
        inner_menu_gtas_element.click()

        time.sleep(5)

        # Move Page - Saved Query Page
        print('[Move Page] Moving MySaved Page ~~~')
        side_menu_my_saved_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_SIDE_MENU_MY_SAVED_ELEMENT)))
        side_menu_my_saved_element.click()

        # Show More Table Rows
        print('[Move Page] Clicking Show More Btn ~~~')
        show_more_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_SHOW_MORE_BTN_ELEMENT)))
        show_more_btn_element.click()

        print('[Move Page] Success Move Page.')

        # Find Rows Containing Specific Word (ex. SLX_GTAS_FORECASTING_Annual)
        print('[Move Page] Getting Query Table Rows ~~~')
        tool_queries_table_element = browser.find_element(By.XPATH, TOOL_QUERIES_TABLE_ELEMENT)
        tool_queries_table_rows = tool_queries_table_element.find_elements(By.XPATH, './tbody/tr/td/a')

        for tool_queries_table_row in tool_queries_table_rows:
            if tool_queries_table_row.text == tool_query_nm:
                tool_queries_table_row.click()
                break

        print('[Move Page] Success Move Page.')
        
    except Exception as e:
        import traceback
        tb = traceback.extract_tb(e.__traceback__)
        line = tb[-1].lineno
        print(f'[error][line : {line}] : {e}')
        print(f'[error][Crawling Fail: Move Page Filed]')
        browser.quit()
        sys.exit()

def try_close_notice_popup(browser):
    """
    [Notice Popup]: Notice Popup Close
    :param browser: (WebDriver) Web Driver
    :return: (bool) Notice Popup is Closed
    """

    # Notice Popup - Notice popup close
    try:
        print('[Notice Popup] Closing Notice Popup ~~~')
        notice_popup_zone_element = browser.find_element(By.XPATH, NOTICE_POPUP_ZONE_ELEMENT)
        notice_popup_elements = notice_popup_zone_element.find_elements(By.XPATH, './cui-growl/div')
        for notice_popup_element in notice_popup_elements:
            notice_popup_close_btn_element = notice_popup_element.find_element(By.XPATH, './section/div/cui-icon')
            notice_popup_close_btn_element.click()
        print('[Notice Popup] All Notice Popup Closed.')
        return True
    except Exception as e:
        print('[Notice Popup] Can not find Notice popup.')
        return False    
    
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
                print(f'CSV 파일 발견: {csv_file_path}')
                try:
                    return csv_file_path
                except Exception as e:
                    print(f'[error] CSV 파일 읽기 중 오류 발생: {e}')
                    return None
        time.sleep(1) 
    print(f'[warning] 타임아웃: {download_path}에서 CSV 파일을 찾을 수 없습니다.')
    return None

def func1():
    print('--' * 10, '(start) ihs_gtas_forecasting_annual', '--' * 10)
    
    tool_query_nm = 'SLX_GTAS_FORECASTING_Annual'
    
    
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, IHS_DOWNLOAD_PATH)
    browser.get(IHS_URL)
    browser.implicitly_wait(10)
    
    # 로그인
    try_login(browser)
    
    #페이지 이동
    try_move_page(browser, tool_query_nm)
    
    while True:
            time.sleep(5)
            # 컨셉 모두 선택
            concept_all_select_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_CONCEPT_ALL_SELECT_ELEMENT)))
            concept_all_select_element.click()
            
            # 스크롤을 맨 위로 올리기
            browser.execute_script("window.scrollTo(0, 0);")
            
            # export > selected series > csv static 클릭
            export_btn_element = browser.find_element(By.XPATH, IHS_EXPORT_BTN_ELEMENT)
            export_btn_element.click()
            
            selected_series_btn_element = browser.find_element(By.XPATH, IHS_SELECTED_SERIES_BTN_ELEMENT)
            actions = ActionChains(browser) # Mouse Over Event
            actions.move_to_element(selected_series_btn_element).perform()
            selected_series_btn_element.click()
            
            selected_export_csv_btn_element = browser.find_element(By.XPATH, IHS_SELECTED_EXPORT_CSV_BTN_ELEMENT)
            selected_export_csv_btn_element.click()
            
        
            download_popup_close_btn_element = WebDriverWait(browser, 400).until(EC.visibility_of_element_located((By.XPATH, IHS_DOWNLOAD_POPUP_CLOSE_BTN_ELEMENT)))
            download_popup_close_btn_element.click()
            
            time.sleep(random.uniform(5, 10))
            
            # Export CSV - Download csv Link Click (csv)
            print(f'[EXPORT CSV] Clicking Download Link ~~~')
            download_link_element = WebDriverWait(browser, 360).until(EC.visibility_of_element_located((By.XPATH, IHS_DOWNLOAD_LINK_ELEMENT)))
            download_link_element.click()

            # Export Csv 완료 될때 까지 기다리기
            time.sleep(random.uniform(5, 10))

            # csv 파일 읽기
            csv_file_path = wait_for_csv_and_read(IHS_DOWNLOAD_PATH)
            
            if csv_file_path is not None:
                print('CSV 파일을 성공적으로 읽었습니다.')
            else:
                print('CSV 파일을 읽지 못했습니다.')
            
            #####CSV READ######
            df = pd.read_csv(csv_file_path)
            
            
            ####### 전처리 하기#############
            
            ### Remove Cols(Not Use)
            drop_col_name = ['Start Date', 'End Date', 'Period']
            df['YEAR'] = pd.to_datetime(df['Period']).dt.year
            df.drop(columns=drop_col_name, axis=1, inplace=True)
            df.rename(columns={'Import Country/Territory': 'IMPORT', 'Export Country/Territory': 'EXPORT', 'Value': 'DATA_VALUE'}, inplace=True)
            df.columns = df.columns.str.upper()
            df = df.replace({np.nan: None})
            
            print(df.head)
            
            ####### 전처리 하기#############
            
            ### DB insert 하기            
            
            
            # total page count 랑 현재 페이지가 같으면 break
            current_page_count_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_CURRENT_PAGE_COUNT_ELEMENT)))
            current_page_count = current_page_count_element.text
            
            total_page_count_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_TOTAL_PAGE_COUNT_ELEMENT)))
            total_page_count = total_page_count_element.text
            
            # 파일 done 경로로 이동
            ## shutil.move(csv_file_path, f"{conf['DESTINATION_PATH']}_{os.path.basename(csv_file_path).split('.')[0]}_{current_page_count}.csv")
            
            # 파일 삭제
            os.remove(csv_file_path)
            
            if (current_page_count == total_page_count):
                print('마지막 페이지 입니다.')                
                break
            else:
                # 스크롤 제일 밑으로
                browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                concept_all_select_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, CONCEPT_ALL_SELECT_ELEMENT)))
                concept_all_select_element.click()
                
                time.sleep(5)
                
                # 다음페이지 누르기
                next_page_btn_element = WebDriverWait(browser, 360).until(EC.visibility_of_element_located((By.XPATH, NEXT_PAGE_BTN_ELEMENT)))
                next_page_btn_element.click()
                print(f'현재 페이지: {current_page_count}, 총 페이지: {total_page_count}')
                
                time.sleep(5)
                continue
    
    
    
    
    
    print('--' * 10, '(end) ihs_gtas_forecasting_annual', '--' * 10)

######################
## task 정의
######################
ihs_gtas_forecasting_annual = PythonOperator(
    task_id = 'ihs_gtas_forecasting_annual',
    python_callable = func1,
    dag = init_dag
)

task_start >> ihs_gtas_forecasting_annual >> task_end