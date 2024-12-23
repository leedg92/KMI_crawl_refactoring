import sys, os, warnings
## 서버용 경로
sys.path.append('/opt/airflow/dags/utils')
from config import *
from python_library import *
from bs4 import BeautifulSoup
import numpy as np
import pymysql
import py7zr

######################
## 기록
######################
## 본 데이터는 unctad_shipping 데이터를 수집
## UNCTAD 항만 정기선 해운 연결성 지수 데이터 수집
## URL = https://unctadstat-api.unctad.org/bulkdownload/US.PLSCI/US_PLSCI
## 해당 URL 사이트에 접속해서 다운로드 받아지는 압축파일을 읽어서 DB에 적재

## 1) 수집주기.
## -> 각 분기마다


######################
## DAG 정의
######################
init_args = {
    'owner' : OWNER_NAME,
    'start_date' : datetime.datetime(2024, 11, 11),
    'retries': 1
}
init_dag = DAG(
    dag_id = 'unctad_shipping_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 2 1 1,4,7,10 *',
    catchup=False
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
    
    finally:
        conn.close()

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

def unzip_7z_file(archive_path, extract_to_dir):
    with py7zr.SevenZipFile(archive_path, mode='r') as z:
        z.extractall(path=extract_to_dir)
        
def preprocessing_data(origin_df):
    COLS_NAME_LIST = ['DATE', 'DATE_LABEL', 'PORT_CD', 'PORT_NM', 'DATA_VALUE', 'FOOT_NOTE', 'MISSING_VALUE']
    cols_nm_list = COLS_NAME_LIST
    df = origin_df.copy()
    df.columns = cols_nm_list
    
    # df[['QUARTER', 'YEAR']] = df['DATE_LABEL'].str.split(' ', 1, expand=True)
    df[['QUARTER', 'YEAR']] = df['DATE_LABEL'].str.split(' ', n=1, expand=True)
    df.drop(columns={'DATE', 'DATE_LABEL', 'FOOT_NOTE', 'MISSING_VALUE'}, axis=1, inplace=True)
    df = df.replace({np.nan: None})

    return df
    
##. 항만 정기선 연결성 지수 데이터 (테이블 명 : fct_unctad_shipping_connectivity)
def func1():
    print('--' * 10, '(start) unctad_shipping_collector', '--' * 10)
    
    zip_7z_nm = 'download.7z'
    zip_7z_path = UNCTAD_DOWNLOAD_PATH + zip_7z_nm
    done_zip_7z_path = UNCTAD_DESTINATION_PATH + get_year_month_day() + '_' + zip_7z_nm
    
    
    # 7z 파일 다운 및 압축 해제
    print(f'Try to Download Zip File')
    download_file(UNCTAD_URL, zip_7z_path)
    
    unzip_7z_file(zip_7z_path, UNCTAD_DOWNLOAD_PATH)
    print(f'Success to Download Zip File & Unzip')
    
    # 압축 해제한 CSV 파일 탐색
    csv_file_nm = [file_nm for file_nm in os.listdir(UNCTAD_DOWNLOAD_PATH) if file_nm.endswith('.csv')][0]
    csv_file_path = UNCTAD_DOWNLOAD_PATH + csv_file_nm
    done_csv_file_path = UNCTAD_DESTINATION_PATH + get_year_month_day() + '_' + csv_file_nm
    
    # CSV 데이터 전처리
    origin_df = pd.read_csv(csv_file_path)
    result_df = preprocessing_data(origin_df)
    
    ## DB insert
    def_table_name = 'fct_unctad_shipping_connectivity'
    upsert_to_dataframe(result_df, def_table_name)
    
    # 데이터 추출이 완료된 파일, done 으로 이동
    shutil.move(zip_7z_path, done_zip_7z_path)
    shutil.move(csv_file_path, done_csv_file_path)
    
    print('--' * 10, '(end) unctad_shipping_collector', '--' * 10)
    


######################
## task 정의
######################
unctad_shipping = PythonOperator(
    task_id = 'unctad_shipping',
    python_callable = func1,
    dag = init_dag
)


task_start >> unctad_shipping >> task_end