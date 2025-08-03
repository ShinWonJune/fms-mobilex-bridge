import pandas as pd

# CSV 파일 경로
csv_file = "all_pdu_readings.csv"

# CSV 읽기 (헤더 없음)
df = pd.read_csv(csv_file, header=None)

# 세 번째 열(index=2)이 ObjID라고 가정
obj_ids = df[2]

# 'objId' 문자열 제거, 문자열로 변환 후 숫자 변환
obj_ids_clean = pd.to_numeric(obj_ids[obj_ids != 'objId'], errors='coerce')

# NaN 제거 후 고유값 추출, 정렬
unique_sorted_objids = sorted(obj_ids_clean.dropna().unique())

# 출력
print("고유 ObjID 목록 (정렬됨):", unique_sorted_objids)
print("고유 ObjID 개수:", len(unique_sorted_objids))
