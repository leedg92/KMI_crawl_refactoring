import requests
import pandas as pd
import datetime
from utils.config import *
from utils.python_library import *

######################
## 공통 함수
######################
def get_ecos_config():
    """ECOS 공통 설정 조회"""
    conn = maria_kmi_dw_db_connection()
    config_df = pd.read_sql("""
        SELECT config_id, config_value 
        FROM config_ecos_common
    """, conn)
    conn.close()
    
    return dict(zip(config_df['config_id'], config_df['config_value']))

def get_ecos_codes(category, period):
    """ECOS 수집 코드 조회"""
    conn = maria_kmi_dw_db_connection()
    codes_df = pd.read_sql("""
        SELECT table_code, item_code_list
        FROM config_ecos_codes
        WHERE category = %s 
        AND period = %s
        AND use_yn = 'Y'
    """, conn, params=[category, period])
    conn.close()
    
    return [
        {
            'TABLE_CODE': row['table_code'],
            'ITEM_CODE_LIST': row['item_code_list'].split(',')
        }
        for _, row in codes_df.iterrows()
    ]

def get_date_range(period_type='M'):
    """기간 유형에 따른 시작/종료 날짜 반환"""
    from_date = str(int(get_year_month()) - 100)
    to_date = get_year_month()
    
    if period_type == 'A':
        return from_date[:-2], to_date[:-2]
    elif period_type == 'Q':
        quarter_from = from_date[:-2] + 'Q' + str(int(from_date[-2:])//3 + 1)
        quarter_to = to_date[:-2] + 'Q' + str(int(to_date[-2:])//3 + 1)
        return quarter_from, quarter_to
    else:
        return from_date, to_date

def create_ecos_url(table_code, item_code_list, period='M'):
    """ECOS API URL 생성"""
    config = get_ecos_config()
    from_date, to_date = get_date_range(period)
    
    return '/'.join([
        config['API_URL'],
        config['API_KEY'],
        config['OUTPUT_FORMAT'],
        config['LANG'],
        '1',
        config['COLLECT_AT_ONCE_CNT'],
        table_code,
        period,
        from_date,
        to_date,
        '/'.join(item_code_list)
    ])

def check_latest_data(new_df, table_name):
    """최신 데이터 비교"""
    check_columns = ['CATEGORY', 'STAT_CODE', 'ITEM_CODE1', 'ITEM_CODE2', 
                    'ITEM_CODE3', 'ITEM_CODE4', 'TIME']
    
    def_conn = maria_kmi_dw_db_connection()
    def_origin_df = pd.read_sql(f"""
        SELECT DISTINCT {', '.join(check_columns)} 
        FROM {table_name}
        ORDER BY TIME DESC
        LIMIT 1
    """, con=def_conn, dtype='object')
    def_conn.close()

    if len(def_origin_df) > 0:
        existing_data = def_origin_df.iloc[0].to_dict()
        new_data = new_df[new_df['TIME'] == new_df['TIME'].max()].iloc[0][check_columns].to_dict()
        return all(existing_data[col] == new_data[col] for col in check_columns)
    
    return False

def collect_ecos_data(category, period='M'):
    """ECOS 데이터 수집 공통 함수"""
    print('--' * 10, f'(start) ecos_{category.lower()}', '--' * 10)
    
    all_data = []
    for input_code in get_ecos_codes(category, period):
        full_url = create_ecos_url(
            input_code['TABLE_CODE'], 
            input_code['ITEM_CODE_LIST'],
            period
        )
        
        response = requests.get(full_url)
        response_json = response.json()
        
        if 'StatisticSearch' in response_json:
            data = response_json['StatisticSearch']['row']
            for item in data:
                item = {key: value if value is not None else "" for key, value in item.items()}
                item['CATEGORY'] = category
                item['YEAR'] = item['TIME'][0:4]
                item['MONTH'] = item['TIME'][-2:] if period == 'M' else ''
                item['CREATED_DTM'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                item['UPDATE_DTM'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                all_data.append(item)
    
    if all_data:
        df = pd.DataFrame(all_data)
        table_name = f'fct_ecos_statics_{period.lower()}'
        
        if check_latest_data(df, table_name):
            print("No Update Data")
            return None
        
        return df
    
    return None

######################
## 수집 함수
######################
def func1():
    """ECOS 월간 금리 데이터 수집"""
    df = collect_ecos_data('INTEREST_RATE', 'M')
    if df is not None:
        insert_to_dataframe(df, 'fct_ecos_statics_month')


if __name__ == "__main__":
    func1()