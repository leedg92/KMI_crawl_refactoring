##. KOSIS
KOSIS_API_KEY = "ZDUyOTMwN2MxMjkxMmM4ZTVlNGZmZjE1Yzc5ZjUyMmY="
KOSIS_API_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"

##. Query
# fct_kosis_population_metrics
CREATE_FCT_KOSIS_POPULATION_METRICS = """
    CREATE TABLE `fct_kosis_population_metrics` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C2` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID2' COLLATE 'utf8_general_ci',
        `C2_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명2' COLLATE 'utf8_general_ci',
        `C2_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명2' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 연간 인구지표 \n* Author. ischoi \n* Created. 2024.11.11'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;
"""
# fct_kosis_population_secenario
CREATE_FCT_KOSIS_POPULATION_SECENARIO = """
    CREATE TABLE `fct_kosis_population_secenario` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C2` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID2' COLLATE 'utf8_general_ci',
        `C2_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명2' COLLATE 'utf8_general_ci',
        `C2_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명2' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 연간 시나리오별 추계인구 \n* Author. ischoi \n* Created. 2024.11.11'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;
"""
# fct_kosis_grain_consumption
CREATE_FCT_KOSIS_GRAIN_CONSUMPTION = """
   CREATE TABLE `fct_kosis_grain_consumption` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C2` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID2' COLLATE 'utf8_general_ci',
        `C2_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명2' COLLATE 'utf8_general_ci',
        `C2_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명2' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `UNIT_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 국문명' COLLATE 'utf8_general_ci',
        `UNIT_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 영문명' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 연간 1인단 양곡 소비량 \n* Author. ischoi \n* Created. 2024.11.11'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;
"""
# fct_kosis_region_income
CREATE_FCT_KOSIS_REGION_INCOME = """
    CREATE TABLE `fct_kosis_region_income` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C2` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID2' COLLATE 'utf8_general_ci',
        `C2_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명2' COLLATE 'utf8_general_ci',
        `C2_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명2' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `UNIT_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 국문명' COLLATE 'utf8_general_ci',
        `UNIT_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 영문명' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 시도별 경제활동별 지역내총생산 \n* Author. ischoi \n* Created. 2024.11.18'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;
"""
# fct_kosis_product_index
CREATE_FCT_KOSIS_PRODUCT_INDEX = """
    CREATE TABLE `fct_kosis_product_index` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C2` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID2' COLLATE 'utf8_general_ci',
        `C2_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명2' COLLATE 'utf8_general_ci',
        `C2_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명2' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `UNIT_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 국문명' COLLATE 'utf8_general_ci',
        `UNIT_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 영문명' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 시도/산업별 경제활동별 지역내총생산  \n* Author. ischoi \n* Created. 2024.11.18'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;        
"""
# fct_kosis_cattle_statistics
CREATE_FCT_KOSIS_CATTLE_STATISTICS = """
    CREATE TABLE `fct_kosis_cattle_statistics` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `UNIT_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 국문명' COLLATE 'utf8_general_ci',
        `UNIT_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 영문명' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 축종별 시도별 가구수 및 마리수 \n* Author. ischoi \n* Created. 2024.11.18'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;           
"""

# fct_kosis_service_industry
CREATE_FCT_KOSIS_SERVICE_INDUSTRY = """
    CREATE TABLE `fct_kosis_service_industry` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C2` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID2' COLLATE 'utf8_general_ci',
        `C2_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명2' COLLATE 'utf8_general_ci',
        `C2_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명2' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `UNIT_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 국문명' COLLATE 'utf8_general_ci',
        `UNIT_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '수 단위 영문명' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 시도/산업별 총괄(외식업/식품유통업 산업현황)  \n* Author. ischoi \n* Created. 2024.11.21'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;        
"""

#. fct_kosis_construction_bsi
CREATE_FCT_KOSIS_CONSTRUCTION_BSI = """
    CREATE TABLE `fct_kosis_construction_bsi` (
        `PRD_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록시점' COLLATE 'utf8_general_ci',
        `PRD_SE` VARCHAR(50) NULL DEFAULT NULL COMMENT '수록주기' COLLATE 'utf8_general_ci',
        `TBL_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 ID' COLLATE 'utf8_general_ci',
        `TBL_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '통계표 국문명' COLLATE 'utf8_general_ci',
        `C1` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID1' COLLATE 'utf8_general_ci',
        `C1_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명1' COLLATE 'utf8_general_ci',
        `C1_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명1' COLLATE 'utf8_general_ci',
        `C2` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID2' COLLATE 'utf8_general_ci',
        `C2_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 국문명2' COLLATE 'utf8_general_ci',
        `C2_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류값 영문명2' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C1_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 국문명' COLLATE 'utf8_general_ci',
        `C2_OBJ_NM_ENG` VARCHAR(200) NULL DEFAULT NULL COMMENT '분류 영문명' COLLATE 'utf8_general_ci',
        `ITM_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 ID' COLLATE 'utf8_general_ci',
        `ITM_NM` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 국문명' COLLATE 'utf8_general_ci',
        `ITM_NM_ENG` VARCHAR(50) NULL DEFAULT NULL COMMENT '분류값 영문명' COLLATE 'utf8_general_ci',
        `DT` VARCHAR(50) NULL DEFAULT NULL COMMENT '값' COLLATE 'utf8_general_ci',
        `ORG_ID` VARCHAR(50) NULL DEFAULT NULL COMMENT '기관코드' COLLATE 'utf8_general_ci',
        `LST_CHN_DE` VARCHAR(50) NULL DEFAULT NULL COMMENT '자료갱신일' COLLATE 'utf8_general_ci',
        `CREATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '생성일자' COLLATE 'utf8_general_ci',
        `UPDATE_DTM` VARCHAR(50) NULL DEFAULT NULL COMMENT '수정일자' COLLATE 'utf8_general_ci'
    )
    COMMENT='* KOSIS 업종별 기업경기실사지수 (건설업종, 실적 및 전망) \n* Author. ischoi \n* Created. 2024.11.21'
    COLLATE='utf8_general_ci'
    ENGINE=InnoDB
    ;
"""