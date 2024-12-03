##. Library
import sys, os, warnings
warnings.filterwarnings(action='ignore')

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
import time, datetime
import random
import glob
import shutil
from selenium import webdriver



import pandas as pd
import numpy as np
import tqdm
import requests
import json
import datetime
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pendulum

sys.path.append('/opt/airflow/dags/utils')
from config import *

pymysql.install_as_MySQLdb()

def maria_kmi_dw_db_connection():
    def_conn = pymysql.connect(host = MDB_HOST,
                               port = MDB_PORT,
                               user = MDB_USERNAME,
                               password = MDB_PASSWORD,
                               db = MDB_DATABASE,
                               charset = 'utf8')
    

    return def_conn

def get_year_month_day():
    return datetime.datetime.now().strftime('%Y%m%d')

def get_year():
    return datetime.datetime.now().strftime('%Y')

def get_month():
    return datetime.datetime.now().strftime('%m')

def get_day():
    return datetime.datetime.now().strftime('%d')

def insert_to_dwdb(result_dataframe, table_name):
    url = sqlalchemy.engine.URL.create(
        drivername = 'mysql',
        username = MDB_USERNAME,
        password = MDB_PASSWORD,
        host = MDB_HOST,
        port = MDB_PORT,
        database = MDB_DATABASE
    )

    engine = create_engine(url)
    conn = engine.connect()
    result_dataframe.to_sql(name = table_name, con=engine, if_exists='append', index=False)
    conn.close()

def create_table(create_commend):
    try:        
        def_conn = maria_kmi_dw_db_connection()
        def_cursor = def_conn.cursor()
        
        def_cursor.execute(create_commend)
        def_cursor.close()
        def_conn.close()

    except Exception as e:
        print(e)

def drop_table(table_name):
    try:
        def_conn = maria_kmi_dw_db_connection()
        def_cursor = def_conn.cursor()

        def_drop_commend = f"""
            DROP TABLE {table_name}
        """

        def_cursor.execute(def_drop_commend)
        def_cursor.close()
        def_conn.close()

    except Exception as e:
        print(e)
        
        
def set_selenium_options():
    opts = Options()
    
    # 헤드리스 모드 설정
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("window-size=1920,1080")
    
    # 안정성 및 성능 관련 설정
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-browser-side-navigation")
    
    # 언어 및 사용자 에이전트 설정
    opts.add_argument("lang=ko_KR")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")   
    
    # 자동화 감지 방지
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    # 인증서 오류 무시
    opts.add_argument("--ignore-certificate-errors")
    
    return opts


def set_webdriver_browser(options, downloadPath):

    service = Service(executable_path='/usr/local/bin/chromedriver')    
    browser = webdriver.Chrome(service=service, options=options)
    print(f"[INFO] ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) => Initializing WebDriver...")
    params = {'behavior': 'allow', 'downloadPath': downloadPath}
    browser.execute_cdp_cmd('Page.setDownloadBehavior', params)
    browser.set_page_load_timeout(600)

    return browser

def set_webdriver_browser_cookies(options, downloadPath, cookies, target_url):
    service = Service(executable_path='/usr/local/bin/chromedriver')    
    browser = webdriver.Chrome(service=service, options=options)
    print(f"[INFO] ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) => Initializing WebDriver...")
    
    # 페이지 이동 (쿠키를 추가하려는 도메인)
    browser.get(target_url)
    browser.implicitly_wait(10)  # 페이지 로드 대기
    
    # 쿠키 추가
    for cookie in cookies:
        browser.add_cookie(cookie)
        print(f"[INFO] Added cookie: {cookie}")

    # 쿠키 적용을 위해 다시 로드
    browser.get(target_url)
    
    params = {'behavior': 'allow', 'downloadPath': downloadPath}
    browser.execute_cdp_cmd('Page.setDownloadBehavior', params)
    browser.set_page_load_timeout(600)
    return browser