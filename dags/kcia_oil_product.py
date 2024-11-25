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
## 본 데이터는 KCIA 석유화학 제품 통계 데이터를 수집 (연간)
## URL = https://www.kpia.or.kr/petrochemical-industry/statistics
## 해당 URL 사이트에 접속해서 
## 기초유분, 중간원료, 합성수지, 합섬원료, 합성고무, 기타제품 데이터를 수집한다

## 1) 수집주기.
## -> 연간


######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11)
}
init_dag = DAG(
    dag_id = 'kcia_oil_product_collector',
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
        conn.rollback()
        return False
    
    finally:
        conn.close()


def kcia_file_download(index, category_path):
    # Set selenium
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, KCIA_DOWNLOAD_PATH)

    try:
        browser.get(KCIA_URL)
        browser.implicitly_wait(10)
        time.sleep(0.5)
        
        # 날짜 선택
        date_btn_element = browser.find_element(By.XPATH, KCIA_DATE_BTN_ELEMENT)
        date_btn_element.click()
        
        time.sleep(2)       
        
        etc_product_btn_element = browser.find_element(By.XPATH, category_path)
        etc_product_btn_element.click()
        category_col = etc_product_btn_element.text
        
        indexCnt = index + 1
        table_path = '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/div[' + str(indexCnt) +']/div[2]/table'
        data_table_element = browser.find_element(By.XPATH, table_path)

        time.sleep(2)
        
        # 테이블의 헤더 추출
        header = data_table_element.find_element(By.TAG_NAME, 'thead')
        header_cols = header.find_elements(By.TAG_NAME, 'th')
        # years = [col.text for col in header_cols if col.text][1:]  # 첫 번째 빈 칸 무시
        years = [col.text for col in header_cols if col.text]

        # 테이블의 바디 추출
        body = data_table_element.find_element(By.TAG_NAME, 'tbody')
        rows = body.find_elements(By.TAG_NAME, 'tr')

        # 데이터 저장을 위한 리스트 초기화
        data = []

        # 제품명 추적을 위한 변수
        current_product = ""

        # 각 행의 데이터를 추출
        for row in rows:
            ths = row.find_elements(By.TAG_NAME, 'th')
            tds = row.find_elements(By.TAG_NAME, 'td')

            # th 데이터와 td 데이터를 합침
            if len(ths) == 2:  # 제품명이 있는 행
                current_product = ths[0].text
                category = ths[1].text
                values = [td.text for td in tds]
            elif len(ths) == 1:  # 제품명은 없지만 카테고리와 값이 있는 행
                category = ths[0].text
                values = [td.text for td in tds]
            else:  # 데이터가 부족한 행은 무시
                continue

            for year, value in zip(years, values):
                data.append([current_product, category, value, year])

        # 데이터프레임으로 변환
        df = pd.DataFrame(data, columns=['PRODUCT', 'CATEGORY', 'VALUE', 'YEAR'])

        # VALUE 컬럼의 ',' 제거
        df['VALUE'] = df['VALUE'].str.replace(',', '')

        # 피벗 테이블 형식으로 변환
        df_pivot = df.pivot_table(index=['PRODUCT', 'YEAR'], columns='CATEGORY', values='VALUE', aggfunc='first').reset_index()

        # 피벗된 테이블 컬럼 이름 변경
        df_pivot.columns.name = None
        df_pivot.columns = ['PRODUCT', 'YEAR', 'DEMAND', 'PRODUCTION', 'IMPORT', 'EXPORT']
        df_pivot['CATEGORY'] = category_col
        browser.close()
        return df_pivot
    
    except Exception as e:
        print(f"Error occurred: {e}")
        browser.close()
        return None
    
    
##. KCIA 석유화학 제품 통계 데이터 (테이블 명 : fct_kcia_oil_product)
def func1():
    print('--' * 10, '(start) kcia_oil_product_collector', '--' * 10)    
    
    # 데이터 다운로드
    for index, category_path in enumerate(KCIA_CATEGORY_LIST):
        origin_df = kcia_file_download(index, category_path)
        
        val_list = []
        for index, result in origin_df.iterrows():
            val_list.append(result.tolist())
        
        
        ## DB insert
        print(origin_df)
        def_table_name = 'fct_kcia_oil_product'
        upsert_to_dataframe(origin_df, def_table_name, val_list)
        
    
    print('--' * 10, '(end) kcia_oil_product_collector', '--' * 10)
    


######################
## task 정의
######################
kcia_oil_product = PythonOperator(
    task_id = 'kcia_oil_product',
    python_callable = func1,
    dag = init_dag
)


task_start >> kcia_oil_product >> task_end