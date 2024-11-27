import pandas as pd
import numpy as np

def get_sequence_code(num: int) -> str:
    """
    순차적인 코드 생성
    1 -> "01"
    99 -> "99"
    100 -> "A0"
    101 -> "A1"
    110 -> "B0"
    120 -> "C0"
    ...
    """
    if num <= 99:
        return f"{num:02d}"
    else:
        alphabet = chr(ord('A') + ((num - 100) // 10))  # 10개씩 알파벳 증가
        digit = (num - 100) % 10  # 0-9 숫자
        return f"{alphabet}{digit}"

def convert_to_category_structure(
   excel_path: str,
   collection_type: str = 'A',
   category_prefix: str = '11'
):
   # 엑셀 파일 읽기
   df = pd.read_excel(excel_path)
   print(f"총 {len(df)}개의 행을 읽었습니다.")
   
   # 결과를 저장할 딕셔너리
   result_dict = {}
   
   # depth별 카운터 초기화
   depth_counters = {i: 0 for i in range(1, 8)}
   current_depth_values = {i: None for i in range(1, 8)}
   current_depth_codes = {i: None for i in range(1, 8)}
   
   # 헤더 준비
   header = "CATEGORY_CD|CATEGORY_NM|API_PATH|PARENT_CD|DEPTH_LEVEL|LAST_DEPTH_YN|COLLECTION_CYCLE|COLLECTION_TYPE"
   
   for idx, row in df.iterrows():
       try:
           # 각 depth 값 확인
           last_valid_depth = 0
           for depth in range(1, 8):
               depth_value = row[f'DEPTH_{depth:02d}'].strip() if not pd.isna(row[f'DEPTH_{depth:02d}']) else None
               
               if depth_value:
                   last_valid_depth = depth
                   
                   # 값이 변경되었거나 처음인 경우
                   if depth_value != current_depth_values[depth]:
                       current_depth_values[depth] = depth_value
                       depth_counters[depth] += 1
                       
                       # 하위 depth 초기화
                       for d in range(depth + 1, 8):
                           current_depth_values[d] = None
                           depth_counters[d] = 0
                       
                       # 코드 생성
                       code_parts = [category_prefix] if depth == 1 else [current_depth_codes[1]]
                       for d in range(2, depth + 1):
                           code_parts.append(get_sequence_code(depth_counters[d]))
                       current_depth_codes[depth] = ''.join(code_parts)
                       
                       # 결과 저장
                       is_last = (depth == last_valid_depth)
                       parent_code = '00000000000000' if depth == 1 else current_depth_codes[depth-1]
                       
                       if is_last:
                           cycles = []
                           if row['Y'] == 'V': cycles.append('Y')
                           if row['Q'] == 'V': cycles.append('Q')
                           if row['M'] == 'V': cycles.append('M')
                           if row['W'] == 'V': cycles.append('W')
                           if row['D'] == 'V': cycles.append('D')
                           collection_cycle = ','.join(cycles)
                       else:
                           collection_cycle = ''
                       
                       result_dict[current_depth_codes[depth]] = (
                           f"{current_depth_codes[depth]}|{depth_value}||"
                           f"{parent_code}|{depth:02d}|"
                           f"{'Y' if is_last else 'N'}|"
                           f"{collection_cycle}|{collection_type if is_last else ''}"
                       )
           
           if (idx + 1) % 100 == 0 or (idx + 1) == len(df):
               print(f"처리 중... {idx + 1}/{len(df)} 행 완료")
                
       except Exception as e:
           print(f"행 {idx + 1} 처리 중 오류 발생: {str(e)}")
           continue
   
   # 결과를 코드 순서대로 정렬하여 파일로 저장하기 전에 코드 길이 보정
   formatted_dict = {}
   for code, value in result_dict.items():
       if len(code) != 14:
           new_code = code.ljust(14, '0')
           value_parts = value.split('|')
           value_parts[0] = new_code
           if value_parts[3] != '00000000000000':
               value_parts[3] = value_parts[3].ljust(14, '0')
           formatted_dict[new_code] = '|'.join(value_parts)
       else:
           formatted_dict[code] = value
   
   # 정렬 및 저장
   result_rows = [header] + [formatted_dict[k] for k in sorted(formatted_dict.keys())]
   
   output_path = excel_path.rsplit('.', 1)[0] + '_converted.csv'
   with open(output_path, 'w', encoding='utf-8-sig') as f:
       f.write('\n'.join(result_rows))
   
   print(f"변환 완료. 결과가 {output_path}에 저장되었습니다.")


# if __name__ == "__main__":
#     # 테스트용 Morocco 데이터
#     test_data = {
#         'DEPTH_01': ['유엔 무역 개발 기구 unctadstat(UNCTAD)'] * 10,
#         'DEPTH_02': ['항만 정기선 해운 연결성 지수'] * 10,
#         'DEPTH_03': ['Morocco'] * 10,
#         'DEPTH_04': [
#             'Agadir', 'Casablanca', 'Jorf Lasfar', 
#             'Mohammedia', 'Nador', 'Safi', 
#             'Tangier', 'Tangier Med', 'Tarfaya', 'Kenitra'
#         ],
#         'DEPTH_05': [''] * 10,  # 빈 컬럼 추가
#         'DEPTH_06': [''] * 10,  # 빈 컬럼 추가
#         'DEPTH_07': [''] * 10,  # 빈 컬럼 추가
#         'Y': ['V'] * 10,
#         'Q': ['V'] * 10,
#         'M': [''] * 10,
#         'W': [''] * 10,
#         'D': [''] * 10
#     }
    
#     # 테스트 데이터를 DataFrame으로 변환
#     import pandas as pd
#     test_df = pd.DataFrame(test_data)
    
#     # 테스트 파일 저장
#     test_file = "morocco_test.xlsx"
#     test_df.to_excel(test_file, index=False)
    
#     # 테스트 실행
#     convert_to_category_structure(
#         excel_path=test_file,
#         collection_type="A",
#         category_prefix="11"
#     )



if __name__ == "__main__":
   convert_to_category_structure(
       excel_path="D:\\Dev\\KMI_WORKSPACE\\WIP\\category_csv\\유엔 무역 개발 기구 unctadstat(UNCTAD)_dataset.xlsx",
       collection_type="A",
       category_prefix="11"
   )

def validate_results(result_dict: dict, df: pd.DataFrame) -> bool:
    """결과 검증"""
    is_valid = True
    
    try:
        # 1. 코드 길이 검증
        invalid_codes = [code for code in result_dict.keys() if len(code) != 14]
        if invalid_codes:
            print(f"14자리가 아닌 코드: {invalid_codes}")
            is_valid = False
        
        # 2. 부모-자식 관계 검증
        for code, value in result_dict.items():
            parent = value.split('|')[3]
            if parent != '00000000000000' and parent not in result_dict:
                print(f"존재하지 않는 부모: {parent}")
                is_valid = False
        
        # 3. depth 레벨 검증
        for code, value in result_dict.items():
            depth = int(value.split('|')[4])
            if depth < 1 or depth > 7:
                print(f"잘못된 depth: {depth}")
                is_valid = False
        
        # 4. 수집주기 검증
        valid_cycles = {'Y', 'Q', 'M', 'W', 'D'}
        for code, value in result_dict.items():
            cycles = value.split('|')[6].split(',') if value.split('|')[6] else []
            if cycles and not all(cycle in valid_cycles for cycle in cycles):
                print(f"잘못된 수집주기: {cycles}")
                is_valid = False
        
        # 5. 카테고리명 검증
        empty_categories = [code for code, value in result_dict.items() if not value.split('|')[1]]
        if empty_categories:
            print(f"빈 카테고리명: {empty_categories}")
            is_valid = False
        
        return is_valid
        
    except Exception as e:
        print(f"검증 실패: {str(e)}")
        return False

