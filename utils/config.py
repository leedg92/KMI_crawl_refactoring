##. Airflow
OWNER_NAME = 'ISCHOI'

##. DB
##. dev DB
#MDB_USERNAME = 'ischoi'
#MDB_PASSWORD = 'dlrtn2@'
#MDB_HOST = '172.30.1.71'
#MDB_PORT = 3306
#MDB_DATABASE = 'kmi_dw_db'

##. kmi DB
#MDB_USERNAME = 'saltlux'
#MDB_PASSWORD = 'saltlux1qw2#ER$'
#MDB_HOST = '192.168.2.125'
#MDB_PORT = 33306
#MDB_DATABASE = 'ihs'

##, ulsan DB
MDB_HOST = '211.193.141.217'
MDB_PORT = 33671
MDB_USERNAME = 'dgyoo'
MDB_PASSWORD = 'ehdrms2@'
MDB_DATABASE = 'kmi_dw_db'


##. KOSIS
# KOSIS_API_KEY = "ZDUyOTMwN2MxMjkxMmM4ZTVlNGZmZjE1Yzc5ZjUyMmY="

##. WORLDBANK


##. ECOS
# ECOS API KEY 유효기간 2023.01.11 ~ 2025.01.11
ECOS_API_KEY = "8KD0RJO6MEUW4E540QG8"

##. unctad_shipping_connectivity
UNCTAD_URL = 'https://unctadstat-api.unctad.org/bulkdownload/US.PLSCI/US_PLSCI'
UNCTAD_DOWNLOAD_PATH = '/opt/airflow/downloads/unctad/'
UNCTAD_DESTINATION_PATH = '/opt/airflow/downloads/unctad/done/'

##. kcia_oil_product
KCIA_URL = 'https://kcia.kr/petrochemical-industry/statistics'
KCIA_DOWNLOAD_PATH = '/opt/airflow/downloads/kcia/'
KCIA_DESTINATION_PATH = '/opt/airflow/downloads/kcia/done/'
KCIA_DATE_BTN_ELEMENT = '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/p/select/option[1]'

KCIA_CATEGORY_LIST = [
    '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/ul/li[1]/p',
    '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/ul/li[2]/p',
    '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/ul/li[3]/p',
    '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/ul/li[4]/p',
    '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/ul/li[5]/p',
    '/html/body/section/main/section[2]/section/div/div/div/div/div[1]/ul/li[6]/p',
    ]

##. ksa_sea_freight_index
KSA_URL = 'https://oneksa.kr/shipping_index'
KSA_DOWNLOAD_PATH = '/opt/airflow/downloads/ksa/'
KSA_DESTINATION_PATH = '/opt/airflow/downloads/ksa/done/'

##. kucea_used_car
KUCEA_URL = 'http://www.kucea.or.kr/source_kor'
KUCEA_DOWNLOAD_PATH = '/opt/airflow/downloads/kucea/'
KUCEA_DESTINATION_PATH = '/opt/airflow/downloads/kucea/done/'

##. knoc_oil
KNOC_URL = 'https://www.petronet.co.kr/'
KNOC_DOWNLOAD_PATH = '/opt/airflow/downloads/knoc/'
KNOC_DESTINATION_PATH = '/opt/airflow/downloads/knoc/done/'

##. tms_cds_stts
TMS_USER_ID = 'kmilibrary'
TMS_USER_PW = '!kmilib2024'
TMS_URL = 'https://stat.kosa.or.kr/tms/cds/TmsCdsStts'
TMS_DOWNLOAD_PATH = '/opt/airflow/downloads/tms_cds_stts/'
TMS_DESTINATION_PATH = '/opt/airflow/downloads/tms_cds_stts/done/'

TMS_LOGIN_BTN_ELEMENT = '/html/body/div/div/div/div[2]/div/div[2]/button'
TMS_USER_ID_INPUT_ELEMENT = '/html/body/div/div/div/div[2]/div/div[3]/div/form/div/input[1]'
TMS_USER_PW_INPUT_ELEMENT = '/html/body/div/div/div/div[2]/div/div[3]/div/form/div/input[2]'
TMS_LOGIN_FINISH_BTN_ELEMENT = '/html/body/div/div/div/div[2]/div/div[3]/div/form/button'

TMS_START_YEAR = '2010'
TMS_START_MONTH = '01'

# 테마별 통계 마우스 올리기
TMS_MENU_THEME_BTN_ELEMENT = '/html/body/div/header/div/nav/ul/li[3]/a/span'
# 조강생산 버튼
TMS_INNER_MENU_TMS_CDS_STTS_BTN_ELEMENT = '/html/body/div/header/div/nav/ul/li[3]/div/ul/li/a/div'
# [조강] 체크박스
TMS_CDS_STTS_CHECK_ELEMENT = '/html/body/div/section/div[1]/div[1]/ul/li/label'
# 시점 버튼
TMS_DATE_SETTING_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/button[1]'
# 시점(월단위)
TMS_MONTH_SETTING_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[1]/li[1]/button'
# 시점(연단위)
TMS_YEAR_SETTING_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[1]/li[2]/button'
# 시작_연선택
TMS_START_YEAR_SELECT_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[1]/select'
# 시작_월선택
TMS_START_MONTH_SELECT_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[2]/select'
# 날짜 전체 선택 (연단위)
TMS_DATE_ALL_SELECT_YEAR_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[4]/label/span'
# 날짜 전체 선택 (월단위)
TMS_DATE_ALL_SELECT_MONTH_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[6]/label/span'
# 적용하기 버튼
TMS_DATE_APPLY_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/button'
# 단위 선택 버튼
TMS_UNIT_SELECT_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/button[2]'
# 단위 : 톤/$ 선택
TMS_UNIT_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[1]/label/span'
# 단위 : 천톤/천$
TMS_UNIT_T_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[2]/label/span'
# 단위 : 백만톤/백만$
TMS_UNIT_H_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[3]/label/span'
# 엑셀 저장 버튼
TMS_EXCEL_SAVE_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[4]/div[2]/div[1]/button[1]'
TMS_COLS_MAPPING = {
        '품목명': 'ITEM_NAME',
        '시점': 'YEAR',
        '당년실적_계': 'CURRENT_RESULT_TOTAL',
        '당년실적_전로강_집계': 'CURRENT_RESULT_BOF_TOTAL',
        '당년실적_전로강_보통강': 'CURRENT_RESULT_BOF_ORDINARY_STEEL',
        '당년실적_전로강_특수강': 'CURRENT_RESULT_BOF_SPECIAL_STEEL',
        '당년실적_전기로강_집계': 'CURRENT_RESULT_EAF_TOTAL',
        '당년실적_전기로강_보통강': 'CURRENT_RESULT_EAF_ORDINARY_STEEL',
        '당년실적_전기로강_특수강': 'CURRENT_RESULT_EAF_SPECIAL_STEEL'
    }



##. IHS_ECONOMY
IHS_URL = "https://connect.ihsmarkit.com/"

##. IHS_ECONOMY_login 관련
IHS_USER_ID = "pdac@kmi.re.kr"
IHS_USER_PW = "!Kmi7974659"
IHS_LOGIN_BTN_ELEMENT = '/html/body/main/section[2]/div[2]/a/button'
IHS_USER_ID_INPUT_ELEMENT = '/html/body/div[2]/div/div[2]/div[2]/div[1]/div/data/div[1]/div[3]/div/div[2]/div[1]/input'
IHS_LOGIN_CONTINUE_BTN_ELEMENT = '/html/body/div[2]/div/div[2]/div[2]/div[1]/div/data/div[2]/div[3]/input'
IHS_USER_PW_INPUT_ELEMENT = '/html/body/div[2]/div/div[2]/div[2]/div[1]/div/div/div/div/div[1]/div[2]/div/div[3]/div/div/input'
IHS_LOGIN_FINISH_BTN_ELEMENT = '/html/body/div[2]/div/div[2]/div[2]/div[1]/div/div/div/div/div[2]/div[2]/button[2]'
IHS_DOWNLOAD_PATH = '/opt/airflow/downloads/ihs_economy/'
IHS_DESTINATION_PATH = '/opt/airflow/downloads/ihs_economy/done/'
TOOL_QUERIES_TABLE_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-content/cms-app/cms-page/cms-saved-content/section/cms-table-container[1]/section/cui-card/div/cms-default-table/div/table'

IHS_TOTAL_PAGE_COUNT_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-content/db-build-query/cui-content/div/div/div/db-search-results-container/db-series-results/cui-card/div/section/div/grid-proxy/div/div/dg-grid/ag-grid/span/ag-grid-angular/div/div[4]/span[2]/span/span[4]'
IHS_CONCEPT_ALL_SELECT_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-content/db-build-query/cui-content/div/div/div/db-search-results-container/db-series-results/cui-card/div/section/div/grid-proxy/div/div/dg-grid/ag-grid/span/ag-grid-angular/div/div[2]/div[2]/div[1]/div[2]/div/div[2]/div[1]/custom-header/span/cui-group/span[1]'
IHS_EXPORT_BTN_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-content/db-build-query/cui-content/div/div/div/db-search-results-container/db-series-results/cui-card/cui-card-header/section/cui-group/button-drop-down-menu[2]/cui-button/button'
IHS_SELECTED_SERIES_BTN_ELEMENT = '/html/body/cui-popover-container-window/div/cui-dropdown-menu/div/ul/span[2]/cui-dropdown-menu-parent/div/li/cui-dropdown-menu-item/div/span'
IHS_SELECTED_EXPORT_CSV_BTN_ELEMENT = '/html/body/cui-popover-container-window[2]/div/cui-dropdown-menu/div/ul/span[2]/cui-dropdown-menu-item/li/div/span/cui-group/span'
IHS_NEXT_PAGE_BTN_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-content/db-build-query/cui-content/div/div/div/db-search-results-container/db-series-results/cui-card/div/section/div/grid-proxy/div/div/dg-grid/ag-grid/span/ag-grid-angular/div/div[4]/span[2]/div[3]/span'
IHS_CURRENT_PAGE_COUNT_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-content/db-build-query/cui-content/div/div/div/db-search-results-container/db-series-results/cui-card/div/section/div/grid-proxy/div/div/dg-grid/ag-grid/span/ag-grid-angular/div/div[4]/span[2]/span/span[2]'
IHS_MENU_MARITIME_TRADE_ELEMENT = '/html/body/my-app/div[1]/cpe-menu/nav/cm-black-bar/section[1]/cm-black-bar-element[2]'
IHS_INNER_MENU_GTAS_ELEMENT = '/html/body/my-app/div[1]/cpe-menu/nav/cm-menu-panel/cui-card/cm-menu-tab-content/section/section[2]/section[1]/section/cm-link-group/section[5]/section/span/a'
IHS_SIDE_MENU_MY_SAVED_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-left-nav/cui-left-nav-main/div/cui-left-nav-main-element[3]/section'
IHS_SHOW_MORE_BTN_ELEMENT = '/html/body/my-app/div[2]/db-app/cui-page-template/cui-content/cms-app/cms-page/cms-saved-content/section/cms-table-container[1]/section/cui-card/cui-card-footer/cui-group/span'
IHS_DOWNLOAD_POPUP_CLOSE_BTN_ELEMENT = '/html/body/my-app/app-notifications-component/cui-growler/div/cui-growl/div/section/div/cui-icon'
IHS_DOWNLOAD_LINK_ELEMENT = '/html/body/my-app/app-notifications-component/cui-growler/div/cui-growl/div/section/section[2]/div/a'

