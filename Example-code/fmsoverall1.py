import re
from datetime import datetime
from elasticsearch import Elasticsearch

def find_wide_fms_indices(
    host='10.20.2.21',
    port=59200,
    scheme='http',
    substring='fms'
):
    """
    1) 클러스터 내 모든 인덱스를 조회(cat.indices).
    2) 인덱스명에 특정 substring(기본 'fms')이 들어가는 인덱스만 필터링.
    3) 'YYYY.MM.DD' 형식의 날짜를 정규식으로 추출 시도(광범위 탐색).
    4) 날짜가 파싱된 인덱스는 (인덱스명, datetime) 형태로 모아 날짜 오름차순 정렬.
    5) 최종 목록, 가장 오래된(가장 과거 날짜) 인덱스, 날짜 파싱 실패 인덱스 목록을 반환.
    """
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': scheme}])

    # 1) 모든 인덱스 이름 가져오기
    response = es.cat.indices(h='index', s='index').strip()
    if not response:
        return [], None, []

    all_indices = response.split('\n')
    
    # 2) substring('fms')이 들어가는 인덱스 필터링
    fms_indices = [idx for idx in all_indices if substring in idx]

    # 3) 날짜 추출(YYYY.MM.DD) 정규식 준비
    #    예: "2025.03.31", "2021.12.01" 등
    date_pattern = re.compile(r"(\d{4}\.\d{2}\.\d{2})")

    parsed_list = []
    not_parsed_list = []
    
    for idx_name in fms_indices:
        match = date_pattern.search(idx_name)  # 인덱스명 어딘가에 날짜가 있으면 매칭
        if match:
            date_str = match.group(1)  # 예: "2025.03.31"
            try:
                dt_obj = datetime.strptime(date_str, "%Y.%m.%d")
                parsed_list.append((idx_name, dt_obj))
            except ValueError:
                # 날짜 파싱 실패(패턴은 맞았는데 실제 변환 실패)
                not_parsed_list.append(idx_name)
        else:
            # 날짜 정규식이 전혀 안 잡히는 인덱스
            not_parsed_list.append(idx_name)

    # 4) 날짜 파싱에 성공한 인덱스들 정렬(오래된 순)
    parsed_list.sort(key=lambda x: x[1])
    
    if parsed_list:
        oldest_index = parsed_list[0][0]
    else:
        oldest_index = None

    # (인덱스명, "YYYY-MM-DD")로 최종 가공
    sorted_result = [(name, dt.strftime("%Y-%m-%d")) for name, dt in parsed_list]

    return sorted_result, oldest_index, not_parsed_list


if __name__ == "__main__":
    # 원하는 호스트, 포트, substring(fms) 등을 변경
    sorted_indices, oldest_idx, not_parsed = find_wide_fms_indices()

    if not sorted_indices and not not_parsed:
        print("클러스터 내에 'fms' 문자열이 포함된 인덱스를 찾지 못했습니다.")
    else:
        print("[클러스터 내 'fms' 문자열이 포함된 인덱스 - 날짜 파싱 성공 목록(오래된 순)]")
        if sorted_indices:
            for idx_name, date_str in sorted_indices:
                print(f" - {idx_name} (날짜: {date_str})")
            print("\n가장 오래된(가장 과거) 인덱스:")
            print(f" => {oldest_idx}")
        else:
            print(" 날짜 파싱에 성공한 인덱스가 없습니다.")

        if not_parsed:
            print("\n[날짜 파싱 실패 / 날짜 패턴이 없는 인덱스 목록]")
            for idx_name in not_parsed:
                print(f" - {idx_name}")
