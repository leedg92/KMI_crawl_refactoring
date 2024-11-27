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
## 본 데이터는 KNOC 유류 데이터를 수집 (월간)
## URL = https://www.petronet.co.kr/
## 해당 URL 사이트에 접속해서 
## KNOC 석유수급총괄, 석유제품생산, 유류 제품수입, 유류 제품수출, 제품별 소비 데이터를 수집한다


## 1) 수집주기.
## -> 월간
## -> start_year : (현재년도-1)
## -> start_month : 01
## -> end_year : (현재년도)
## -> end_month : (현재월)-2



######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}
init_dag = DAG(
    dag_id = 'knoc_oil_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 1 * *'
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
        
def wait_for_xls_and_read(download_path, timeout=60):
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
        
def knoc_file_download(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn):
    # Set selenium
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, KNOC_DOWNLOAD_PATH)

    # URL Access    
    try:
        browser.get(KNOC_URL)
        browser.implicitly_wait(10)
        
        # Search
        oil_supply_demand_element = browser.find_element(By.XPATH, '/html/body/div[3]/div/ul/li[5]/a/img')
        oil_supply_demand_element.click()
        browser.implicitly_wait(10)

        
        browser.switch_to.frame('left')
        
        # depth1
        if (category != 1 and category !=2):
            product_manufacture_element = browser.find_element(By.XPATH, depth1)
            product_manufacture_element.click()

        # depth2
        product_manufacture_element = browser.find_element(By.XPATH, depth2)
        product_manufacture_element.click()
        browser.implicitly_wait(10)

        # 날짜 설정 DOM 접근
        browser.switch_to.parent_frame()
        browser.switch_to.frame('body')

        select_from_year_element = browser.find_element(By.XPATH, start_year_element)
        select_from_month_element = browser.find_element(By.XPATH, start_month_element)
        select_to_year_element = browser.find_element(By.XPATH, end_year_element)
        select_to_month_element = browser.find_element(By.XPATH, end_month_element)
        select_from_year_object = Select(select_from_year_element)
        select_from_month_object = Select(select_from_month_element)
        select_to_year_object = Select(select_to_year_element)
        select_to_month_object = Select(select_to_month_element)

        ## !데이터 범위 3년씩 조회 가능
        select_from_year = int(get_year()) - 1
        select_from_month = 1
        select_to_year = int(get_year())
        select_to_month = int(get_month()) - 2
        
        # 날짜 지정(From, To)
        select_from_year_object.select_by_value(str(select_from_year))
        select_from_month_object.select_by_value(str(select_from_month).zfill(2))
        select_to_year_object.select_by_value(str(select_to_year))
        select_to_month_object.select_by_value(str(select_to_month).zfill(2))        

        # 지정한 날짜 범위로 데이터 조회
        search_btn_element = browser.find_element(By.XPATH, search_btn)
        search_btn_element.click()
        browser.implicitly_wait(10)
        time.sleep(3)
        
        # 다운로드 하기전 다운로드 경로 확인
        files_before = os.listdir(KNOC_DOWNLOAD_PATH)
        print(f'Before Download [File List]: {files_before}')
        
        if(category == 4 or category == 5):
            is_checked = True
            if(category == 4):
                list_items = WebDriverWait(browser, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[4]/td/div/ul/li'))
                )
            else:
                list_items = WebDriverWait(browser, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '/html/body/div[1]/div[2]/fieldset/form/table/tbody/tr[4]/td/div/ul/li'))
                )
            for i in range(0, int(len(list_items)/5)+1):
                # 선택해제
                if(category == 4):
                    selected_cancel_element = browser.find_element(By.XPATH, '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[4]/td/ul/li[2]/a')
                else:
                    selected_cancel_element = browser.find_element(By.XPATH, '/html/body/div[1]/div[2]/fieldset/form/table/tbody/tr[4]/td/ul/li[2]/a')                
                selected_cancel_element.click()
                
                time.sleep(2)
                for j in range(5):
                    checkbox_id = 'check_'+str(j+(i*5)+1).zfill(2)
                    if ((j+(i*5)+1) > len(list_items)):
                        if(category ==5):
                            checkbox_id = 'check_15'
                            script = f"document.getElementById('{checkbox_id}').checked = false;"
                            browser.execute_script(script)
                            is_checked = False
                        break
                    script = f"document.getElementById('{checkbox_id}').checked = true;"
                    browser.execute_script(script)
                    
                # 대기
                time.sleep(2)
                
                
                # 지정한 날짜 범위로 데이터 조회
                if is_checked:
                    search_btn_element = browser.find_element(By.XPATH, search_btn)
                    search_btn_element.click()
                    browser.implicitly_wait(10)
                    time.sleep(3)
                
                # CSV 다운로드 버튼 클릭
                csv_download_btn_element = browser.find_element(By.XPATH, csv_btn)
                csv_download_btn_element.click()

                # 파일이 정상적으로 모두 다운로드 될 때까지 대기                
                wait_for_xls_and_read(KNOC_DOWNLOAD_PATH)
                
                # 방금 다운로드된 첫 번째 CSV 파일을 찾고 파일명 변경
                try:
                    csv_file_path = glob.glob(os.path.join(KNOC_DOWNLOAD_PATH, '*.csv'))[0]                    
                    timestamp = time.strftime("%Y%m%d_%H%M%S")                      
                    base_name, ext = os.path.splitext(os.path.basename(csv_file_path)) 
                    
                    new_file_path = os.path.join(KNOC_DOWNLOAD_PATH, f"{base_name}_{timestamp}{ext}")
                    
                    os.rename(csv_file_path, new_file_path)
                    print(f"File renamed from {csv_file_path} to {new_file_path}")
                except Exception as e:
                    print(f"[error] Failed to rename file: {e}")                

        else:
            # CSV 다운 버튼 DOM 접근하여 파일 다운로드
            files_before = os.listdir(KNOC_DOWNLOAD_PATH)
            csv_download_btn_element = browser.find_element(By.XPATH, csv_btn)
            csv_download_btn_element.click()

            # 파일이 정상적으로 모두 다운로드 될 때까지 대기
            wait_for_xls_and_read(KNOC_DOWNLOAD_PATH)
        
            
        files_after = os.listdir(KNOC_DOWNLOAD_PATH)
        

        # 다운 받은 파일 명
        new_files = []
        for f in files_after:
            if f not in files_before and f.endswith('.csv'):
                new_files.append(f)
        print(f'Download Files Name : {new_files}')

        return new_files
        
        

    except Exception as e:
        print(e)
        browser.close()
        sys.exit()


def preprocessing_data(category, df):
    
    category_cd_mapping = {
        1: '07010000000000', # 석유수급총괄
        2: '07020000000000', # 석유제품생산
        3: '07050000000000', # 제품별 소비 데이터
        4: '07030000000000', # 제품수입(제품별)
        5: '07040000000000' # 제품수출(제품별)
    }
    category_cd = category_cd_mapping.get(category, '07000000000000')

    # 다중 헤더를 DataFrame으로 변환 후 결측값 채우기
    header_df = df.columns.to_frame(index=False)

    for col in header_df.columns:
        header_df[col] = header_df[col].apply(lambda x: None if 'Unnamed' in str(x) else x)
    header_df.fillna(method='ffill', axis=0, inplace=True)

    print(header_df)
    # 다중 헤더를 단일 레벨로 결합 ('Unnamed' 및 빈 값을 제거하고 결합)
    df.columns = ['_'.join([x for x in col if pd.notna(x) and 'Unnamed' not in x]).strip() for col in header_df.values]

    df.reset_index(inplace=True)
    df = df[~df.iloc[:, 0].str.contains('합계', na=False)]    
    df['year'] = df.iloc[:, 0].str.extract(r'(\d{2})년')[0]
    df['month'] = df.iloc[:, 0].str.extract(r'(\d{2})월')[0]
    df['year'].fillna(method='ffill', inplace=True)    
    df.dropna(subset=['year', 'month'], inplace=True)
    
    df['year'] = df['year'].str.zfill(2).apply(lambda x: '19' + x if int(x) > 80 else '20' + x)
    df['month'] = df['month'].astype(int).astype(str).str.zfill(2)  

    df.drop(df.columns[0], axis=1, inplace=True)
    df_melted = df.melt(id_vars=['year', 'month'], var_name='category_nm', value_name='data_value')
    df_melted.dropna(subset=['data_value'], inplace=True)
    df_melted['data_value'] = pd.to_numeric(df_melted['data_value'], errors='coerce')
    df_melted.dropna(subset=['data_value'], inplace=True)    
    df_melted['category_nm'] = df_melted['category_nm'].str.replace(r'\s+', '', regex=True)
        
    df_melted['category_cd'] = category_cd
    
    print(df_melted)
    return df_melted


def extract_process(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn):
    try:
        download_files = knoc_file_download(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn)
        
        for file_name in download_files:
            full_file_name = KNOC_DOWNLOAD_PATH + file_name
            done_file_name = KNOC_DESTINATION_PATH + get_year_month_day() + '_' + file_name
        
            if category in [1, 4, 5]:
                df = pd.read_csv(full_file_name, header=[0, 1], index_col=0)
                result_df = preprocessing_data(category, df)
            elif category in [2, 3]:
                df = pd.read_csv(full_file_name, header=[0], index_col=0)
                result_df = preprocessing_data(category, df)
            
            val_list = []
            for index, result in result_df.iterrows():
                val_list.append(result.tolist())
            
            def_table_name = 'fct_knoc_oil'
            upsert_to_dataframe(result_df, def_table_name, val_list)
            
            # 데이터 추출이 완료된 파일, done 으로 이동
            shutil.move(full_file_name, done_file_name)
                                
        
        
        print('[End to Crawling KNOC]')
        
    except Exception as e:
        print(e)
        print('[Crawling Finish]')
        sys.exit()


###석유수급총괄
def func1():
    print('--' * 10, '(start) knoc_oil_collector (oil_supply_summary)', '--' * 10)    
    
    category = 1
    depth1 = '/html/body/div/ul/li[3]/a'
    depth2 = '/html/body/div/ul/li[3]/ul/li[1]/a'
    start_year_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[1]/select'
    start_month_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[3]/select'
    end_year_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[4]/select'
    end_month_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[6]/select'
    search_btn = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/p/a'
    csv_btn = '/html/body/form[1]/div/div[2]/fieldset/ul/li[1]/a'
    
    extract_process(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn)
    
    print('--' * 10, '(end) knoc_oil_collector (oil_supply_summary)', '--' * 10)    
    
###석유제품생산
def func2():
    print('--' * 10, '(start) knoc_oil_collector (petroleum_product_production)', '--' * 10)    
    
    category = 2
    depth1 = '/html/body/div/ul/li[3]/a'
    depth2 = '/html/body/div/ul/li[3]/ul/li[2]/a'
    start_year_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[1]/select'
    start_month_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[3]/select'
    end_year_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[4]/select'
    end_month_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[6]/select'
    search_btn = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[3]/td/p/a'
    csv_btn = '/html/body/form[1]/div/div[2]/fieldset/ul/li[1]/a'
    
    extract_process(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn)
    
    print('--' * 10, '(end) knoc_oil_collector (petroleum_product_production)', '--' * 10)   
    
###제품별 소비 데이터
def func3():
    print('--' * 10, '(start) knoc_oil_collector (product_consumption_data)', '--' * 10)    
    
    category = 3
    depth1 = '/html/body/div/ul/li[4]/a'
    depth2 = '/html/body/div/ul/li[4]/ul/li[1]/a'
    start_year_element = '/html/body/div[1]/div[2]/form/fieldset/table/tbody/tr[2]/td/div/div[1]/select'
    start_month_element = '/html/body/div[1]/div[2]/form/fieldset/table/tbody/tr[2]/td/div/div[3]/select'
    end_year_element = '/html/body/div[1]/div[2]/form/fieldset/table/tbody/tr[2]/td/div/div[4]/select'
    end_month_element = '/html/body/div[1]/div[2]/form/fieldset/table/tbody/tr[2]/td/div/div[6]/select'
    search_btn = '/html/body/div[1]/div[2]/form/fieldset/table/tbody/tr[3]/td/p/a'
    csv_btn = '/html/body/div[1]/div[2]/form/fieldset/ul/li[1]/a'
        
    extract_process(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn)
    
    print('--' * 10, '(end) knoc_oil_collector (product_consumption_data)', '--' * 10) 
    
###제품수입(제품별)
def func4():
    print('--' * 10, '(start) knoc_oil_collector (oil_product_import)', '--' * 10)    
    
    category = 4
    depth1 = '/html/body/div/ul/li[5]/a'
    depth2 = '/html/body/div/ul/li[5]/ul/li[3]/a'
    start_year_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[1]/select'
    start_month_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[3]/select'
    end_year_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[4]/select'
    end_month_element = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[2]/td/div/div[6]/select'
    search_btn = '/html/body/form[1]/div/div[2]/fieldset/table/tbody/tr[4]/td/p/a'
    csv_btn = '/html/body/form[1]/div/div[2]/fieldset/ul/li[1]/a'
    
    extract_process(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn)
    
    print('--' * 10, '(end) knoc_oil_collector (oil_product_import)', '--' * 10) 
    
###제품수출(제품별)
def func5():
    print('--' * 10, '(start) knoc_oil_collector (oil_product_export)', '--' * 10)    
    
    category = 5
    depth1 = '/html/body/div/ul/li[5]/a'
    depth2 = '/html/body/div/ul/li[5]/ul/li[6]/a'
    start_year_element = '/html/body/div[1]/div[2]/fieldset/form/table/tbody/tr[2]/td/div/div[1]/select'
    start_month_element = '/html/body/div[1]/div[2]/fieldset/form/table/tbody/tr[2]/td/div/div[3]/select'
    end_year_element = '/html/body/div[1]/div[2]/fieldset/form/table/tbody/tr[2]/td/div/div[4]/select'
    end_month_element = '/html/body/div[1]/div[2]/fieldset/form/table/tbody/tr[2]/td/div/div[6]/select'
    search_btn = '/html/body/div[1]/div[2]/fieldset/form/table/tbody/tr[4]/td/p/a'
    csv_btn = '/html/body/div[1]/div[2]/fieldset/ul/li[1]/a'    
    
    extract_process(category,depth1,depth2,start_year_element,start_month_element,end_year_element,end_month_element,search_btn,csv_btn)
    
    print('--' * 10, '(end) knoc_oil_collector (oil_product_export)', '--' * 10) 
    


######################
## task 정의
######################

###석유수급총괄
oil_supply_summary = PythonOperator(
    task_id = 'oil_supply_summary',
    python_callable = func1,
    dag = init_dag
)
###석유제품생산
petroleum_product_production = PythonOperator(
    task_id = 'petroleum_product_production',
    python_callable = func2,
    dag = init_dag
)

###제품별 소비 데이터
product_consumption_data = PythonOperator(
    task_id = 'product_consumption_data',
    python_callable = func3,
    dag = init_dag
)

###제품수입(제품별)
oil_product_import = PythonOperator(
    task_id = 'oil_product_import',
    python_callable = func4,
    dag = init_dag
)

###제품수출(제품별)
oil_product_export = PythonOperator(
    task_id = 'oil_product_export',
    python_callable = func5,
    dag = init_dag
)

task_start >> oil_supply_summary >> petroleum_product_production >> product_consumption_data >> oil_product_import >> oil_product_export >> task_end