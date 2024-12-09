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
MDB_USERNAME = 'saltlux'
MDB_PASSWORD = 'saltlux1qw2#ER$'
MDB_HOST = '192.168.2.125'
MDB_PORT = 33306
MDB_DATABASE = 'ihs'

##, ulsan DB
# MDB_HOST = '211.193.141.217'
# MDB_PORT = 33671
# MDB_USERNAME = 'dgyoo'
# MDB_PASSWORD = 'ehdrms2@'
# MDB_DATABASE = 'kmi_dw_db'


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

##. investing_bdi_index
INVESTING_URL = 'https://www.investing.com/indices/baltic-dry-historical-data'
INVESTING_DOWNLOAD_PATH = '/opt/airflow/downloads/investing/'
INVESTING_DESTINATION_PATH = '/opt/airflow/downloads/investing/done/'

INVESTING_USER_ID = 'honggeun.kwak@diquest.com'
INVESTING_USER_PW = '1q2w3e4r!@'

INVESTING_COLS_MAPPING = {
               'Date': 'DATE',
               'Price': 'PRICE',
               'Open': 'OPEN',
               'High': 'HIGH',
               'Low': 'LOW',
               'Vol.': 'VOL',
               'Change %%': 'CHANGE'
            }

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
KUCEA_URL = 'http://kucea.or.kr/source_kor'
KUCEA_DOWNLOAD_PATH = '/opt/airflow/downloads/kucea/'
KUCEA_DESTINATION_PATH = '/opt/airflow/downloads/kucea/done/'

##. knoc_oil
KNOC_URL = 'https://www.petronet.co.kr/'
KNOC_DOWNLOAD_PATH = '/opt/airflow/downloads/knoc/'
KNOC_DESTINATION_PATH = '/opt/airflow/downloads/knoc/done/'

##. ers_rmtr_stts
ERS_USER_ID = 'kmilibrary'
ERS_USER_PW = '!kmilib2024'
ERS_URL = 'https://stat.kosa.or.kr/ers/rmtr/ErsRmtrStts'
ERS_DOWNLOAD_PATH = '/opt/airflow/downloads/ers_rmtr_stts/'
ERS_DESTINATION_PATH = '/opt/airflow/downloads/ers_rmtr_stts/done/'

ERS_LOGIN_BTN_ELEMENT = '/html/body/div/div/div/div[2]/div/div[2]/button'
ERS_USER_ID_INPUT_ELEMENT = '/html/body/div/div/div/div[2]/div/div[3]/div/form/div/input[1]'
ERS_USER_PW_INPUT_ELEMENT = '/html/body/div/div/div/div[2]/div/div[3]/div/form/div/input[2]'
ERS_LOGIN_FINISH_BTN_ELEMENT = '/html/body/div/div/div/div[2]/div/div[3]/div/form/button'

ERS_START_YEAR = '2010'
ERS_START_MONTH = '01'

ERS_MENU_SURVEY_STATISTICS_CATEGORY_ELEMENT = '/html/body/div/header/div/nav/ul/li[1]/a/span'
ERS_INNER_MENU_RAW_MATERIAL_ELEMENT = '/html/body/div/header/div/nav/ul/li[1]/div/ul/li[2]/a/div'
ERS_STEEL_CHECK_BOX_ELEMENT = '/html/body/div/section/div[1]/div[1]/ul/li[1]/label'
ERS_DATE_SETTING_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/button[1]'
ERS_MONTH_SETTING_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[1]/li[1]/button'
ERS_YEAR_SETTING_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[1]/li[2]/button'
ERS_START_YEAR_SELECT_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[1]/select'
ERS_START_MONTH_SELECT_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[2]/select'
ERS_DATE_ALL_SELECT_MONTH_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[6]/label/span'
ERS_DATE_ALL_SELECT_YEAR_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[4]/label/span'
ERS_DATE_APPLY_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/button'
ERS_UNIT_SELECT_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/button[2]'
ERS_UNIT_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[1]/label/span'
ERS_UNIT_T_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[2]/label/span'
ERS_UNIT_H_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[3]/label/span'
ERS_EXCEL_SAVE_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[4]/div[2]/div[1]/button[1]'

ERS_COLS_MAPPING = {
        '시점': 'YEAR',
        '업체명': 'COMPANY_NAME',
        '품목명': 'ITEM_NAME',    
        '국내구매': 'DOMESTIC_PURCHASE',
        '수입구매': 'IMPORT_PURCHASE',
        '자가발생구매': 'SELF_GENERATED_PURCHASE',
        '자가소비': 'SELF_CONSUMPTION',
        '구매집계': 'PURCHASE_SUMMARY',
        '국내판매': 'DOMESTIC_SALES',
        '기타판매': 'OTHER_SALES',
        '판매집계': 'SALES_SUMMARY',
        '당월재고': 'CURRENT_MONTH_INVENTORY',
        '전월재고': 'PREVIOUS_MONTH_INVENTORY'
    }


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

TMS_MENU_THEME_BTN_ELEMENT = '/html/body/div/header/div/nav/ul/li[3]/a/span'
TMS_INNER_MENU_TMS_CDS_STTS_BTN_ELEMENT = '/html/body/div/header/div/nav/ul/li[3]/div/ul/li/a/div'
TMS_CDS_STTS_CHECK_ELEMENT = '/html/body/div/section/div[1]/div[1]/ul/li/label'
TMS_DATE_SETTING_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/button[1]'
TMS_MONTH_SETTING_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[1]/li[1]/button'
TMS_YEAR_SETTING_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[1]/li[2]/button'
TMS_START_YEAR_SELECT_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[1]/select'
TMS_START_MONTH_SELECT_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[2]/select'
TMS_DATE_ALL_SELECT_YEAR_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[4]/label/span'
TMS_DATE_ALL_SELECT_MONTH_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/ul[2]/div/div/div[1]/div[6]/label/span'
TMS_DATE_APPLY_BTN_ELEMENT = '/html/body/div/div[3]/section/div/div/div[2]/div/button'
TMS_UNIT_SELECT_BTN_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/button[2]'
TMS_UNIT_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[1]/label/span'
TMS_UNIT_T_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[2]/label/span'
TMS_UNIT_H_TON_SELECT_ELEMENT = '/html/body/div/section/div[2]/div/div[3]/div[1]/div/div/div[3]/label/span'
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
IHS_NEW_DOWNLOAD_PATH = '/opt/airflow/downloads/ihs_economy_new/'
IHS_NEW_DESTINATION_PATH = '/opt/airflow/downloads/ihs_economy_new/done/'
IHS_GTAS_DOWNLOAD_PATH = '/opt/airflow/downloads/ihs/'
IHS_GTAS_DESTINATION_PATH = '/opt/airflow/downloads/ihs/done/'
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


##. clarksons
CLARKSONS_URL = 'https://sin.clarksons.net/'
CLARKSONS_DOWNLOAD_PATH = '/opt/airflow/downloads/clarksons/'
CLARKSONS_DESTINATION_PATH = '/opt/airflow/downloads/clarksons/done/'

CLARKSONS_USER_ID = 'kmimaritime'
CLARKSONS_USER_PW = '!Kmilib4388'

CLARKSONS_LOGIN_BTN = '/html/body/div[1]/div[2]/div[4]/div/div/div/div[3]/a[1]'
CLARKSONS_LOGIN_ID = '/html/body/div[15]/div/div/div[2]/form/div[1]/div/input'
CLARKSONS_LOGIN_ID_CONTINUE = '/html/body/div[14]/div/div/div[2]/form/div[2]/button'
CLARKSONS_LOGIN_PW = '/html/body/div[15]/div/div/div[2]/form/ng-template/div[1]/div/input[1]'
CLARKSONS_LOGIN_FINISH_BTN = '/html/body/div[14]/div/div/div[2]/form/div[2]/button'

CATEGORY_TOP_ELEMENT = '/html/body/app-root/div/main/div[2]/app-timeseries-base/section/div/div/app-browse/kendo-splitter/kendo-splitter-pane[1]/timeseries-browse-hierarchy/kendo-treeview'
CATEGORY_CHECK_BOX_ELEMENT = './div/span/span/span/div/input'
FREQUENCY_DIV_ELEMENT = '/html/body/app-root/div/main/div[2]/app-timeseries-base/section/div/div/app-browse/kendo-splitter/kendo-splitter-pane[2]/app-grid-chart-data/div/article/timeseries-frequency-panel/div'
DOWNLOAD_PANEL_ELEMENT = '/html/body/app-root/div/main/div[2]/app-timeseries-base/section/div/div/app-browse/kendo-splitter/kendo-splitter-pane[2]/app-grid-chart-data/div/article/timeseries-viewmode-panel/div/div'
DOWNLOAD_EXCEL_ACCEPT_BTN = '/html/body/ngb-modal-window/div/div/modal/div[2]/app-download-modal/section/div[4]/a'
CATEGORY_TOGGLE_ELEMENT = './div/span/kendo-icon-wrapper/kendo-svgicon'