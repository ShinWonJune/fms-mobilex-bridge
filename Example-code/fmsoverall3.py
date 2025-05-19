"""

해당 코드를 실행하면 각 rscstatrawweek-fms- 파일의 크기 파악이 가능함함

실제 데이터가 얼마나 누적으로 저장되는지 파악악

실행 결과 예시시

 - rscstatrawweek-fms-2023.45 | size=28.8mb | docs=117714
 - rscstatrawweek-fms-2023.46 | size=32.3mb | docs=134064
 - rscstatrawweek-fms-2023.47 | size=32.4mb | docs=134064
 - rscstatrawweek-fms-2023.48 | size=32.3mb | docs=134064
 - rscstatrawweek-fms-2023.49 | size=32.4mb | docs=134064

"""

import re
from elasticsearch import Elasticsearch

def get_indices_size_and_docs(index_pattern="*", host="10.20.2.21", port=59200, scheme="http"):
    """
    index_pattern에 해당하는 인덱스들의
     - 인덱스명
     - 스토리지 사이즈 (store.size)
     - 문서 수 (docs.count)
    를 반환합니다.
    
    Returns:
        List[dict] 형태: [
          {
            "index": "...",
            "store_size": "...",
            "docs_count": <int>
          },
          ...
        ]
    """
    es = Elasticsearch([{"host": host, "port": port, "scheme": scheme}])

    # h 파라미터로 인덱스, store.size, docs.count를 가져옴
    # s='index' -> 인덱스명 기준으로 정렬
    cat_response = es.cat.indices(index=index_pattern, h="index,store.size,docs.count", s="index")
    if not cat_response:
        return []

    lines = cat_response.strip().split("\n")
    results = []
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        idx_name, store_size, docs_count_str = parts[0], parts[1], parts[2]
        try:
            docs_count = int(docs_count_str)
        except ValueError:
            docs_count = 0
        results.append({
            "index": idx_name,
            "store_size": store_size,
            "docs_count": docs_count
        })
    return results

if __name__ == "__main__":
    # 예: rscstatrawmonth-fms-*, rscstatrawweek-fms-*, rscstatrawyear-fms-*
    # 혹은 그냥 "*-fms-*" 식으로 해도 됨
    index_patterns = [
        "rscstatrawmonth-fms-*",
        "rscstatrawweek-fms-*",
        "rscstatrawyear-fms-*"
    ]
    
    for pat in index_patterns:
        print(f"\n=== 인덱스 패턴: {pat} ===")
        info_list = get_indices_size_and_docs(index_pattern=pat)
        if not info_list:
            print("  해당 패턴으로 매칭되는 인덱스가 없습니다.")
            continue
        
        for info in info_list:
            print(f" - {info['index']} | size={info['store_size']} | docs={info['docs_count']}")
