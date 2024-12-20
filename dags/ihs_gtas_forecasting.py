import sys, os, warnings
## 서버용 경로
sys.path.append('/opt/airflow/dags/utils')
from config import *
from python_library import *
from bs4 import BeautifulSoup
import numpy as np
import pymysql
import glob

######################
## 기록
######################
## 본 데이터는 ihs_gtas_forecasting_data_collector 연간,분기 데이터를 수집
## URL = https://connect.ihsmarkit.com/
## 해당 ihs 사이트에서 로그인 후 mysaved 탭에 들어가서
## 미리 설정된 economy 조건에 맞게 api 주소를 호출하여 사용
## * Tool Queries 명 : SLX_GTAS_FORECASTING_Annual (연간)

## 수집 흐름
# concept, commodity 2개씩
# all csv 파일 읽어서 적재
# 중간에 에러나면 DB파일이던 txt파일이던 떨궈서 체킹
# DB 에 체킹하는 테이블 하나 만들어서 하나씩 체킹하면서 적재할 예정

## 1) 수집주기.
## -> api 갱신일에 맞게 연 or 분기 단위로 설정

## 2) 증분조건.
## 수집기간에 따라 데이터 upsert 진행
## * 수집기간 : 현재년도 기준 +- 1년치

######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 12, 9)
}
init_dag = DAG(
    dag_id = 'ihs_gtas_forecasting_collector',
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
        print(f'[error][Crawling Fail: Login Filed]')
        browser.quit()
        sys.exit()
        
def try_move_page(browser, tool_query_nm):
    
    try:
        print('[Move Page] Moving Search Page ~~~')

        menu_maritime_trade_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_MENU_MARITIME_TRADE_ELEMENT)))
        actions = ActionChains(browser)
        actions.move_to_element(menu_maritime_trade_element).perform()
        
        time.sleep(5)

        inner_menu_gtas_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_INNER_MENU_GTAS_ELEMENT)))
        inner_menu_gtas_element.click()

        time.sleep(5)

        print('[Move Page] Moving MySaved Page ~~~')
        side_menu_my_saved_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_SIDE_MENU_MY_SAVED_ELEMENT)))
        side_menu_my_saved_element.click()

        print('[Move Page] Clicking Show More Btn ~~~')
        show_more_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_SHOW_MORE_BTN_ELEMENT)))
        show_more_btn_element.click()

        print('[Move Page] Success Move Page.')

        print('[Move Page] Getting Query Table Rows ~~~')
        tool_queries_table_element = browser.find_element(By.XPATH, TOOL_QUERIES_TABLE_ELEMENT)
        tool_queries_table_rows = tool_queries_table_element.find_elements(By.XPATH, './tbody/tr/td/a')

        for tool_queries_table_row in tool_queries_table_rows:
            if tool_queries_table_row.text == tool_query_nm:
                tool_queries_table_row.click()
                break

        print('[Move Page] Success Move Page.')
        
    except Exception as e:
        print(e)
        print(f'[error][Crawling Fail: Move Page Filed]')
        browser.quit()
        
    
def wait_for_csv_and_read(download_path, timeout=120):
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

#. 수집할 ihs commodity 조회
def get_ihs_commodity(concept_nm, frequency):
    conn = maria_kmi_dw_db_connection()
    
    try:
        # MAX(UPDATE_DTM) 값을 조회
        query = f"""
            SELECT MAX(UPDATE_DTM) AS MAX_UPDATE_DTM
            FROM fct_ihs_gtas_monitoring
            WHERE CONCEPT = '{concept_nm}'
            AND FREQUENCY = '{frequency}'            
        """
        max_update_dtm_result = pd.read_sql(query, conn)
        max_update_dtm = max_update_dtm_result['MAX_UPDATE_DTM'].iloc[0]
        
        if max_update_dtm:
            max_update_dtm = pd.to_datetime(max_update_dtm)
            months_diff = (datetime.datetime.now() - max_update_dtm).days // 30

            # 3개월 이상 차이 나면 CHECKED 컬럼을 'N'로 업데이트
            if months_diff >= 3:
                update_query = f"""
                    UPDATE fct_ihs_gtas_monitoring
                    SET CHECKED = 'N'
                    WHERE FREQUENCY = '{frequency}'
                """
                with conn.cursor() as cur:
                    cur.execute(update_query)
                    conn.commit()
                print(f"CHECKED column updated to 'N' for {concept_nm}.")

        # 'CHECKED'가 'N'인 데이터 조회
        query = f"""
            SELECT * FROM fct_ihs_gtas_monitoring
            WHERE CHECKED = 'N'
            AND CONCEPT = '{concept_nm}'
            AND FREQUENCY = '{frequency}'
            ORDER BY CONCEPT, COMMODITY, FREQUENCY
        """
        codes_df = pd.read_sql(query, conn)
        
        return codes_df.to_dict('records')
    
    except Exception as e:
        print(f"Database query error: {e}")
        return []
    
    finally:
        conn.close()
    


#. 수집할 ihs concept 조회
def get_ihs_concept():
    conn = maria_kmi_dw_db_connection()
    
    try:
        query = """
            SELECT * FROM dim_category_code 
            WHERE CATEGORY_CD LIKE '0201%' AND DEPTH_LEVEL = 3
        """
        print(f"Executing query: {query}")
        
        codes_df = pd.read_sql(query, conn)
        
        print(f"Query result: {codes_df}")
        
        return codes_df.to_dict('records')
    
    except Exception as e:
        print(f"Database query error: {e}")
        return []
    
    finally:
        conn.close()

def try_concept_check_box_click(browser, concept_nm):

    try:
        
        # 스크롤을 맨 위로 올리기
        browser.execute_script("window.scrollTo(0, 0);")
        
        time.sleep(2)   
        
        print(f'[Concept Check Box] Input Keyword (Concept) ~~~')
        search_concept_input_element = WebDriverWait(browser, 20).until(EC.presence_of_element_located((By.XPATH, IHS_GTAS_SEARCH_CONCEPT_INPUT_ELEMENT)))
        search_concept_input_element.clear()
        search_concept_input_element.send_keys(concept_nm)
        
        time.sleep(2)
        
        # Concept Check Box - Click Concept Check Box
        print(f'[Concept Check Box] Clicking Concept Check Box ~~~')
        concept_label_element = browser.find_element(By.XPATH, IHS_GTAS_CONCEPT_LABEL_ELEMENT)
        if concept_label_element.text.replace(' ', '') != concept_nm.replace(' ', ''):
            print(f'[Concept Check Box] Can not find Concept Name - {concept_nm}')
            return False
        concept_check_box_element = (WebDriverWait(browser, 30).until(EC.element_to_be_clickable((By.XPATH, IHS_GTAS_CONCEPT_CHECK_BOX_ELEMENT))))
        concept_check_box_element.click()

        print(f'[Concept Check Box] Success Concept Check Box Click.')
        return True
    
    except Exception as e:
        print(e)
        print(f'[Concept Check Box] Can not find Concept Check Box. - {concept_nm}')
        return False

def try_commodity_check_box_click(browser, commodity_nm):
    
    try: 
        print(f'[Commodity Check Box] Input Keyword (Commodity) ~~~')
        search_commodity_input_element = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, IHS_GTAS_SEARCH_COMMODITY_INPUT_ELEMENT)))
        search_commodity_input_element.clear()

        time.sleep(5)
        search_commodity_input_element.send_keys(commodity_nm)

        print(f'[Commodity Check Box] Clicking Commodity Check Box ~~~')
        commodity_zone_elements = browser.find_elements(By.XPATH, IHS_GTAS_COMMODITY_ZONE_ELEMENTS)

        for commodity_zone_element in commodity_zone_elements:
            if commodity_zone_element.text.replace(' ', '') == commodity_nm.replace(' ', ''):
                commodity_check_box_state_parents_element = commodity_zone_element.find_element(By.XPATH, '../../..')
                commodity_check_box_state_elements = commodity_check_box_state_parents_element.find_elements(By.XPATH, './span')

                if len(commodity_check_box_state_elements) > 0:
                    check_box_is_checked = 'checked' in commodity_check_box_state_elements[0].get_attribute('class').split()
                    if check_box_is_checked:
                        print(f'[Commodity Check Box] Success Already Clicked Commodity Check Box.')
                        return True
                commodity_zone_element.click()

                try:
                    print(f'[Commodity Check Box] Waiting Concept Loading Bar ~~~')
                    concept_loading_bar_element = browser.find_element(By.XPATH, IHS_GTAS_CONCEPT_LOADING_BAR_ELEMENT)
                    WebDriverWait(browser, 10).until(EC.invisibility_of_element(concept_loading_bar_element))
                except Exception as e:
                    print(f'[Commodity Check Box] Can not find Concept loading bar.')
                    pass

                print(f'[Commodity Check Box] Success Commodity Check Box Click.')
                return True        
        return False
    
    except Exception as e:
        print(e)
        print(f'[Commodity Check Box] Can not find Commodity Name - {commodity_nm}')
        return False


def try_view_result_click(browser):
    print(f'[Search Data] Clicking View Results Btn ~~~')
    view_results_btn_element = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, IHS_GTAS_VIEW_RESULTS_BTN_ELEMENT)))
    browser.execute_script("arguments[0].click();", view_results_btn_element)  
    
    try:
        print(f'[Search Data] Clicking Confirm Btn ~~~')
        confirm_btn_element = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, IHS_GTAS_CONFIRM_BTN_ELEMENT)))
        browser.execute_script("arguments[0].click();", confirm_btn_element)
    except Exception as e:
        print(f'[Search Data] Can not find Confirm button.')
        pass

    try:
        print(f'[Search Data] Waiting View Results Loading Bar ~~~')
        view_results_loading_bar_element = browser.find_element(By.XPATH, IHS_GTAS_VIEW_RESULTS_LOADING_BAR_ELEMENT)
        WebDriverWait(browser, 5).until(EC.invisibility_of_element(view_results_loading_bar_element))
    except Exception as e:
        print(f'[Search Data] Can not find View Results loading bar.')
        pass

    print(f'[Search Data] Success Search Data.')

def try_export_csv_file(browser):
    try:
        retry_count = 3  # 재시도 횟수        
        time.sleep(2)
        
        # 스크롤을 맨 위로 올리기
        browser.execute_script("window.scrollTo(0, 0);")
        
        time.sleep(2)
        
        # export > All series > CSV static 클릭
        export_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_EXPORT_BTN_ELEMENT)))
        browser.execute_script("arguments[0].click();", export_btn_element)
        
        time.sleep(2)
        
        all_series_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_GTAS_ALL_SERIES_BTN_ELEMENT)))            
        actions = ActionChains(browser)  # Mouse Over Event
        actions.move_to_element(all_series_btn_element).perform()
        all_series_btn_element.click()
        
        time.sleep(2)
        
        selected_export_csv_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_GATS_SELECTED_EXPORT_CSV_BTN_ELEMENT)))
        selected_export_csv_btn_element.click()
        
        time.sleep(2)
        
        download_popup_close_btn_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, IHS_DOWNLOAD_POPUP_CLOSE_BTN_ELEMENT)))
        download_popup_close_btn_element.click()
        
        
        time.sleep(1)
        
        files_before = os.listdir(IHS_GTAS_DOWNLOAD_PATH)
        
        # 두 개의 XPath 중 하나를 기다림
        xpaths = [IHS_GTAS_DOWNLOAD_LINK_ELEMENT, IHS_GTAS_DOWNLOAD_LINK_ELEMENT_RE]
        download_link_element = None

        for attempt in range(retry_count):
            for xpath in xpaths:
                try:
                    download_link_element = WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, xpath)))
                    if download_link_element:
                        browser.execute_script("arguments[0].click();", download_link_element)
                        break
                except Exception as e:
                    print(f'[EXPORT CSV] XPath not found')
            if download_link_element:
                break  # 성공 시 루프 종료
        else:
            print("Download link not found after retries.")
            return []  # 실패 시 빈 리스트 반환

        # 다운로드된 파일 확인
        wait_for_csv_and_read(IHS_GTAS_DOWNLOAD_PATH)
        time.sleep(5)
        files_after = os.listdir(IHS_GTAS_DOWNLOAD_PATH)
        new_files = [f for f in files_after if f not in files_before]

        return new_files

    except Exception as e:
        print(f"Error occurred: {e}")
        if browser.service.is_connectable() and browser.session_id:
            print("현재 browser 상태: 브라우저가 켜져 있습니다.")
        else:
            print("현재 browser 상태: 브라우저가 꺼져 있습니다.")
        return []

def try_monitoring_check(concept_nm, commodities, frequency):
    conn = maria_kmi_dw_db_connection()
    
    try:
        # commodities_str = ", ".join([f"'{commodity.replace('\'', '\'\'')}'" for commodity in commodities])
        commodities_str = ", ".join(["'{}'".format(commodity.replace("'", "''")) for commodity in commodities])

        # CHECKED 컬럼 값을 'Y'로 업데이트
        update_query = f"""
            UPDATE fct_ihs_gtas_monitoring
            SET CHECKED = 'Y'
            WHERE CONCEPT = '{concept_nm}'
            AND COMMODITY IN ({commodities_str})
            AND FREQUENCY = '{frequency}'
        """
        print(f"Executing UPDATE query: {update_query}")
        with conn.cursor() as cursor:
            cursor.execute(update_query)
            conn.commit()
    
    except Exception as e:
        print(f"Database query error: {e}")
        return []
    
    finally:
        conn.close()

####### 전처리 하기#############
def preprocessing_csv_data_annual(df):     
    ### Remove Cols(Not Use)
    drop_col_name = ['Start Date', 'End Date', 'Period']
    df['YEAR'] = pd.to_datetime(df['Period']).dt.year
    df.drop(columns=drop_col_name, axis=1, inplace=True)
    df.rename(columns={'Import Country/Territory': 'IMPORT', 'Export Country/Territory': 'EXPORT', 'Value': 'DATA_VALUE'}, inplace=True)
    df.columns = df.columns.str.upper()
    df = df.replace({np.nan: None})
    df = df.dropna(subset=['IMPORT', 'EXPORT'])
    
    return df

def try_insert_to_DB(result_df, def_table_name):
    val_list = []
    for index, result in result_df.iterrows():
        val_list.append(result.tolist())
    
    ## DB insert
    print(result_df)    
    upsert_to_dataframe(result_df, def_table_name, val_list)    

def clear_check(browser):
    search_commodity_input_element = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, IHS_GTAS_SEARCH_COMMODITY_INPUT_ELEMENT)))
    search_commodity_input_element.clear()
    time.sleep(1)
    search_commodity_input_element.send_keys(' ')
    
    time.sleep(1)

    search_commodity_clear_all_element = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, IHS_GTAS_SEARCH_COMMODITY_CLEAR_ALL_ELEMENT)))
    search_commodity_clear_all_element.click()
    
    time.sleep(2)

def clear_check_concept(browser):
    
    # 스크롤을 맨 위로 올리기
    browser.execute_script("window.scrollTo(0, 0);")    
    time.sleep(2)   
    
    search_concept_input_element = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, IHS_GTAS_SEARCH_CONCEPT_INPUT_ELEMENT)))
    search_concept_input_element.clear()
    time.sleep(1)

    search_concept_clear_all_element = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, IHS_GTAS_SEARCH_CONCEPT_CLEAR_ALL_ELEMENT)))
    search_concept_clear_all_element.click()
    
    time.sleep(2)

def execute_crawling(browser, tool_query_nm, frequency, def_table_name):
    
    # URL Access
    browser.get(IHS_URL)
    browser.implicitly_wait(20)
    browser.maximize_window()
    
    # 로그인
    try_login(browser)
    
    #페이지 이동
    try_move_page(browser, tool_query_nm)
    time.sleep(5)
    
    try:
        df = get_ihs_concept()
        
        df = pd.DataFrame(df)
        ihs_concept_list = df['CATEGORY_NM'].tolist()
        
        for idx, concept_nm in enumerate(ihs_concept_list):            
            
            if idx != 0:
                clear_check_concept(browser)
                time.sleep(5)                
                
            if not try_concept_check_box_click(browser, concept_nm):
                print(f'Search Data Warning: Can not find Concept ({concept_nm}) !!!')
                continue  

            df_commodity = get_ihs_commodity(concept_nm, frequency)
            
            if not df_commodity:
                # 마지막 concept_nm인 경우
                if idx == len(ihs_concept_list) - 1:
                    print('--' * 10, '수집이 완료되었습니다.', '--' * 10)
                    browser.quit()
                    sys.exit()
                    break                
            else:
                df_commodity = pd.DataFrame(df_commodity)
                ihs_commodity_list = df_commodity['COMMODITY'].tolist()
                
                for i in range(0, len(ihs_commodity_list), 2):
                    # commodity 2개씩 가져오기
                    chunk = ihs_commodity_list[i:i+2]
                    for commodity_nm in chunk:
                        print('-' * 30)
                        print(f'Concetp Name :: {concept_nm}')
                        print(f'Commodity Name :: {commodity_nm}')
                        print('-' * 30)
                        
                        if not try_commodity_check_box_click(browser, commodity_nm):
                            print(f'Search Data Warning: Can not find Concept/Commodity ({concept_nm}/{commodity_nm}) !!!')
                            
                    time.sleep(2)
                        
                    # view Results 버튼 클릭
                    try_view_result_click(browser)
                    
                    time.sleep(2)
                    
                    # Export > all > csv 다운로드
                    download_file_name = try_export_csv_file(browser)
                    if not download_file_name:
                        print(f'해당 commodity는 다운로드를 할 수 없습니다. {chunk}')
                        screenshot_file = f"screenshot_{chunk}.png"
                        browser.save_screenshot(screenshot_file)
                        print(f"화면 캡처 저장됨: {screenshot_file}")
                        for file in glob.glob(os.path.join(IHS_GTAS_DOWNLOAD_PATH, "*.csv")):
                            os.remove(file)
                        continue
                    full_file_name = IHS_GTAS_DOWNLOAD_PATH + download_file_name[0]
                    
                    # csv 파일 읽기
                    if full_file_name is not None:
                        print('CSV 파일을 찾았습니다.')
                    else:
                        print('CSV 파일을 찾지 못했습니다.')
                    
                    time.sleep(2)
                    
                    #####CSV READ######
                    df = pd.read_csv(full_file_name, encoding='utf-8')                    
                    
                    # 데이터 전처리
                    result_df = preprocessing_csv_data_annual(df)
                    
                    # 다운받은 파일 DB insert
                    try_insert_to_DB(result_df, def_table_name)
                    
                    # monitoring 에 Y 표시
                    commodity_lists = chunk
                    try_monitoring_check(concept_nm, commodity_lists, frequency)
                    
                    # 다운받은 csv 파일 삭제
                    os.remove(full_file_name)

                    # commodity 체크 풀기
                    clear_check(browser)                
        
        # 루프가 정상적으로 완료되었을 때 실행
        print('--' * 10, '모든 데이터 수집이 완료되었습니다.', '--' * 10)
            
    
    except Exception as e:
        print(f'[error] crawring 오류 발생: {e}')
        print('--' * 10, 'crawring restarting...', '--' * 10)
        browser.quit()
        
        for file in glob.glob(os.path.join(IHS_GTAS_DOWNLOAD_PATH, "*.csv")):
            os.remove(file)
            
        browser = set_firefox_browser(IHS_GTAS_DOWNLOAD_PATH)        
        execute_crawling(browser, tool_query_nm, frequency, def_table_name)


def func1():
    print('--' * 10, '(start) ihs_gtas_forecasting_annual', '--' * 10)
    
    tool_query_nm = 'SLX_GTAS_FORECASTING_Annual'
    frequency = 'Annual'
    browser = set_firefox_browser(IHS_GTAS_DOWNLOAD_PATH)
    def_table_name = 'fct_ihs_gtas_forecasting_annual'
    
    execute_crawling(browser, tool_query_nm, frequency, def_table_name)
    
    print('--' * 10, '(end) ihs_gtas_forecasting_annual', '--' * 10)
    
def func2():
    print('--' * 10, '(start) ihs_gtas_forecasting_quarter', '--' * 10)
        
    print('--' * 10, '(end) ihs_gtas_forecasting_quarter', '--' * 10)

######################
## task 정의
######################
ihs_gtas_forecasting_annual = PythonOperator(
    task_id = 'ihs_gtas_forecasting_annual',
    python_callable = func1,
    dag = init_dag
)

ihs_gtas_forecasting_quarter = PythonOperator(
    task_id = 'ihs_gtas_forecasting_quarter',
    python_callable = func2,
    dag = init_dag
)

task_start >> ihs_gtas_forecasting_annual >> ihs_gtas_forecasting_quarter >> task_end