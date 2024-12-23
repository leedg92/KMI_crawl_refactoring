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
    'start_date' : datetime.datetime(2024, 12, 3),
    'retries': 1
}
init_dag = DAG(
    dag_id = 'investing_bdi_index_collector',
    default_args = init_args,
    # schedule_interval = '@once'
    schedule_interval = '0 3 * * *',
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

def calculate_week_starting_sunday(date):
    # 일요일을 시작으로 주차 계산
    sunday = date - pd.Timedelta(days=date.weekday() + 1) if date.weekday() != 6 else date
    return sunday.isocalendar()[1]
    
def investing_file_download(time_frame, term):
    
    # Set selenium
    opts = set_selenium_options()
    browser = set_webdriver_browser(opts, INVESTING_DOWNLOAD_PATH)

    # URL Access
    try:
        browser.get(INVESTING_URL)
        browser.implicitly_wait(60)
        print("OPEN URL")
        time.sleep(0.5)

        # time frame 선택
        time_frame_element = browser.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/span')         
        browser.execute_script("arguments[0].scrollIntoView(true);", time_frame_element)
        browser.execute_script("arguments[0].click();", time_frame_element)
        
        time.sleep(1)
        print('time frame 선택')
        term_select_btn_element = browser.find_element(By.XPATH, time_frame) 
        browser.execute_script("arguments[0].click();", term_select_btn_element)
        print('time frame 주기 선택')
        time.sleep(1)


        try:
            text = browser.find_element(By.XPATH, '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[3]/table')
        except Exception as e:
            print("Element not found XPath (table) . Error:", e)
            browser.quit()
            raise e
            
        raw_data = text.text

        lines = raw_data.split("\n")    
        header = ["Date", "Price", "Open", "High", "Low", "Change"]

        rows = []
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= len(header):  
                date = " ".join(parts[:3])  
                row = [date] + parts[3:len(header)+3]  
                rows.append(row)

        rows = [
            row if len(row) == len(header) else row + ["-"] * (len(header) - len(row))
            for row in rows
        ]

        df = pd.DataFrame(rows, columns=header)

        # Map month names to numbers
        month_map = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
            "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }

        # Extract Year, Month, Day from Date
        df['YEAR'] = df['Date'].apply(lambda x: x.split()[-1])  
        df['MONTH'] = df['Date'].apply(lambda x: month_map[x.split()[0]])  
        if(term != 'M'):
            df['DAY'] = df['Date'].apply(lambda x: x.split()[1].replace(",", ""))
        if(term == 'W'):
            df['WEEK'] = pd.to_datetime(df['Date']).apply(calculate_week_starting_sunday)
        
        df.columns = [col.upper() for col in df.columns]
        df['CHANGE'] = df['CHANGE'].replace('%', '', regex=True)
        df.drop(columns=['DATE'], inplace=True)


        print(df)

        browser.quit()
        
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        browser.quit()
    
    
##. [일간] Baltic Dry Index Historical Data (BADI) 데이터 (테이블 명 : fct_investing_bdi_index_daily)
def func1():
    print('--' * 10, '(start) investing_bdi_index_collector (daily)', '--' * 10)    
    
    time_frame_daliy = '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[1]/span'
    term = 'D'
    result_df = investing_file_download(time_frame_daliy, term)
    
    val_list = []
    for index, result in result_df.iterrows():
        val_list.append(result.tolist())
    
    def_table_name = 'fct_investing_bdi_index_daily'
    upsert_to_dataframe(result_df, def_table_name, val_list)
    
    print('--' * 10, '(end) investing_bdi_index_collector (daily)', '--' * 10)
    
##. [주간] Baltic Dry Index Historical Data (BADI) 데이터 (테이블 명 : fct_investing_bdi_index_weekly)
def func2():
    print('--' * 10, '(start) investing_bdi_index_collector (weekly)', '--' * 10)    
    
    time_frame_weekly = '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[2]/span'    
    term = 'W'
    result_df = investing_file_download(time_frame_weekly, term)
    
    val_list = []
    for index, result in result_df.iterrows():
        val_list.append(result.tolist())
    
    def_table_name = 'fct_investing_bdi_index_weekly'
    upsert_to_dataframe(result_df, def_table_name, val_list)
    
    print('--' * 10, '(end) investing_bdi_index_collector (weekly)', '--' * 10)
    
##. [월간] Baltic Dry Index Historical Data (BADI) 데이터 (테이블 명 : fct_investing_bdi_index_monthly)
def func3():
    print('--' * 10, '(start) investing_bdi_index_collector (monthly)', '--' * 10)    
    
    time_frame_monthly = '/html/body/div[1]/div[2]/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[3]/span'    
    term = 'M'
    result_df = investing_file_download(time_frame_monthly, term)
    
    val_list = []
    for index, result in result_df.iterrows():
        val_list.append(result.tolist())
    
    def_table_name = 'fct_investing_bdi_index_monthly'
    upsert_to_dataframe(result_df, def_table_name, val_list)
    
    print('--' * 10, '(end) investing_bdi_index_collector (monthly)', '--' * 10)
    


######################
## task 정의
######################
investing_bdi_index_daily = PythonOperator(
    task_id = 'investing_bdi_index_daily',
    python_callable = func1,
    dag = init_dag
)

investing_bdi_index_weekly = PythonOperator(
    task_id = 'investing_bdi_index_weekly',
    python_callable = func2,
    dag = init_dag
)

investing_bdi_index_monthly = PythonOperator(
    task_id = 'investing_bdi_index_monthly',
    python_callable = func3,
    dag = init_dag
)


task_start >> investing_bdi_index_daily >> investing_bdi_index_weekly >> investing_bdi_index_monthly >> task_end
