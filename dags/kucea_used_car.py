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
## 본 데이터는 KUCEA 중고차 수출 데이터를 수집 (월간)
## URL = http://www.kucea.or.kr/source_kor
## 1. 해당 URL 사이트에 접속
## 2. "통관기준" 이 들어간 첫번째 제목을 클릭
## 3. 첨부파일(.xlsx) 다운로드
## 4. 해당년도 시트에 노란색으로 칠해진 월별 "소계" 부분의 데이터를 수집한다


## 1) 수집주기.
## -> 월간


######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 12, 4)
}
init_dag = DAG(
    dag_id = 'kucea_used_car_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 1 * * *',
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
        conn.rollback()
        return False
    
    finally:
        conn.close()

def wait_for_xlsx_and_read(download_path, timeout=60):
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
                xls_file_path = os.path.join(download_path, file_name)
                print(f'다운로드 파일 발견: {xls_file_path}')
                try:
                    return file_name
                except Exception as e:
                    print(f'[error] XLSX 파일 읽기 중 오류 발생: {e}')
                    return None
        time.sleep(1) 
    print(f'[warning] 타임아웃: {download_path}에서 XLSX 파일을 찾을 수 없습니다.')
    return None

def kucea_file_download():
    
    # Set firefox
    browser = set_firefox_browser(KUCEA_DOWNLOAD_PATH)
    
    # URL Access
    try:
        browser.get(KUCEA_URL)
        browser.implicitly_wait(20)
        
        specific_word = "통관기준"
        
       # 행의 수를 기반으로 반복문 실행
        rows_count = len(browser.find_elements(By.XPATH, "//table/tbody/tr"))
        time.sleep(3)
        
        print(rows_count)

        new_files = []

        for i in range(rows_count):
            # 페이지 변경 시 마다 행 재검색
            rows = browser.find_elements(By.XPATH, "/html/body/div[1]/div[2]/div/div/div/form/fieldset/table/tbody/tr")
            try:
                row = rows[i]
                if specific_word in row.text:
                    # 게시글 중 "통관기준" 단어가 포함된 셀 선택
                    cell = row.find_element(By.XPATH, "./td[2]/a")
                    cell.click()
                    browser.implicitly_wait(10)
                    
                    #sleep 안주면 에러발생
                    time.sleep(1)

                    # 첨부 파일 선택
                    attached_btn_element = browser.find_elements(By.XPATH, '/html/body/div[1]/div[2]/div/div/div/div[1]/div/div[3]/dl/dt/button')
                    attached_btn_element[0].click()
                    browser.implicitly_wait(10)
                    # time.sleep(3)
                    
                    # 첨부 파일 다운로드                    
                    files_before = os.listdir(KUCEA_DOWNLOAD_PATH)
                    print('files_before::')
                    print(files_before)
                    download_file_element = browser.find_elements(By.XPATH, '/html/body/div[1]/div[2]/div/div/div/div[1]/div/div[3]/dl/dd/ul/li/a')
                    download_file_element[0].click()
                    
                    wait_for_xlsx_and_read(KUCEA_DOWNLOAD_PATH)

                    files_after = os.listdir(KUCEA_DOWNLOAD_PATH)
                    print('files_after::')
                    print(files_after)
                    new_files = [f for f in files_after if f not in files_before]
                    
                    return new_files
                
            except Exception as e:
                print(e)
                browser.close()
                sys.exit()   
                
    except Exception as e:
        print(e)
        browser.close()
        sys.exit()   
                
def preprocessing_data(df):
    
    car_type = {
        '승용': 'CAR',
        '승합': 'VANS',
        '트렉터': 'TRACTORS',
        '화물': 'TRUCKS'
    }

    # 헤더라인 제거
    temp_df = df.copy().drop([0, 1])

    # Null 데이터 유효한 데이터로 채우기 & "소계", "합계", "차군" 컬럼만 사용
    if not temp_df.iloc[0].str.contains('월', na=False).any():
        return pd.DataFrame()
    temp_df.iloc[0] = temp_df.iloc[0].fillna(method='ffill').str.replace('월', '').apply(lambda x: str(x).zfill(2))
    temp_df.iloc[:, 0] = temp_df.iloc[:, 0].fillna(method='ffill')
    temp_df = temp_df[temp_df.iloc[:, 0]
        .replace('\s+', '', regex=True)
        .str.contains('소계|합계|차군', regex=True, case=False, na=False)]

    # 1~12월 소계 데이터 제거 및 전치
    data_df = temp_df[0:8].T.iloc[:-2]

    ###################################
    # 차군 순서 리스트
    car_list = df.copy()[0].dropna()
    car_list = car_list[car_list.isin(car_type.keys()).values]
    ###################################

    # 중고차수출 금액 -> 데이터프레임 생성
    price_df = data_df[data_df.iloc[:, 1].replace('\s+', '', regex=True) == '금액']
    price_df = price_df.drop(price_df.columns[1], axis=1)
    price_df.columns = ['MONTH', f'{car_type[car_list.iloc[0]]}_EXPORT_PRICE'
        , f'{car_type[car_list.iloc[1]]}_EXPORT_PRICE', f'{car_type[car_list.iloc[2]]}_EXPORT_PRICE'
        , f'{car_type[car_list.iloc[3]]}_EXPORT_PRICE', 'TOTAL_EXPORT_PRICE']
    print(f'price_df: \n{price_df}')
    # 중고차수출 수량 -> 데이터프레임 생성
    quantity_df = data_df[data_df.iloc[:, 1].replace('\s+', '', regex=True) == '수량']
    quantity_df = quantity_df.drop(quantity_df.columns[1], axis=1)
    quantity_df.columns = ['MONTH', f'{car_type[car_list.iloc[0]]}_EXPORT_QUANTITY'
        , f'{car_type[car_list.iloc[1]]}_EXPORT_QUANTITY', f'{car_type[car_list.iloc[2]]}_EXPORT_QUANTITY'
        , f'{car_type[car_list.iloc[3]]}_EXPORT_QUANTITY', 'TOTAL_EXPORT_QUANTITY']
    print(f'quantity_df: \n{quantity_df}')

    # 중고차수출 금액 & 중고차수출 수량 병합
    result_df = pd.merge(left=price_df, right=quantity_df, how='inner', on='MONTH')

    return result_df               


##. KUCEA 중고차 수출 데이터 수집 (테이블 명 : fct_kucea_used_car)
def func1():
    print('--' * 10, '(start) kucea_used_car_collector', '--' * 10)    
    
    download_file_name = kucea_file_download()[0]
    
    full_file_name = KUCEA_DOWNLOAD_PATH + download_file_name
    done_file_name = KUCEA_DESTINATION_PATH + get_year_month_day() + '_' + download_file_name
    
    origin_df = pd.read_excel(full_file_name, header=None, sheet_name=None)
    for sheet_name, sheet_df in origin_df.items():
        print('sheet_name')
        print(sheet_name)
        number_data = re.search(r'\d+', sheet_name)
        if number_data:
            year = number_data.group()
        else:  # 시트에 숫자(년도) 정보가 없는 경우 패스 (ex. HS CODE)
            continue

        result_df = preprocessing_data(sheet_df)
        if len(result_df) == 0:
            continue
        # 시트이름의 연도 값 사용
        result_df['YEAR'] = year
        
        val_list = []
        for index, result in result_df.iterrows():
            val_list.append(result.tolist())
        
        ## DB insert
        def_table_name = 'fct_kcuea_used_car'
        upsert_to_dataframe(result_df, def_table_name, val_list)    

        time.sleep(1)
        
    # 데이터 추출이 완료된 파일, done 으로 이동
    shutil.move(full_file_name, done_file_name)
        
        
    
    print('--' * 10, '(end) kucea_used_car_collector', '--' * 10)
    


######################
## task 정의
######################
kucea_used_car = PythonOperator(
    task_id = 'kucea_used_car',
    python_callable = func1,
    dag = init_dag
)


task_start >> kucea_used_car >> task_end