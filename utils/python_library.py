##. Library
import sys, os, warnings
warnings.filterwarnings(action='ignore')

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

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

sys.path.append('/home/diquest/kmi_airflow/utils')
from config import *

pymysql.install_as_MySQLdb()

def maria_kmi_dw_db_connection():
    def_conn = pymysql.connect(host = MDB_HOST,
                               port = MDB_PORT,
                               user = MDB_USERNAME,
                               password = MDB_PASSWORD,
                               db = 'kmi_dw_db',
                               charset = 'utf8')
    

    return def_conn

def insert_to_dwdb(result_dataframe, table_name):
    url = sqlalchemy.engine.URL.create(
        drivername = 'mysql',
        username = MDB_USERNAME,
        password = MDB_PASSWORD,
        host = MDB_HOST,
        port = MDB_PORT,
        database = 'kmi_dw_db'
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

            