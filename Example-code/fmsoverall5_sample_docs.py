import json
from elasticsearch import Elasticsearch

def get_index_mapping(index_name, host='10.20.2.21', port=59200, scheme='http'):
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': scheme}])
    # 매핑 정보 확인
    mapping = es.indices.get_mapping(index=index_name)
    return mapping

def get_sample_docs(index_name, size=5, host='10.20.2.21', port=59200, scheme='http'):
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': scheme}])
    # 샘플 문서 size건 match_all 쿼리
    response = es.search(
        index=index_name,
        size=size,
        query={
            "match_all": {}
        }
    )
    return response["hits"]["hits"]

def get_fth_docs(index_name, size=24, host='10.20.2.21', port=59200, scheme='http'):
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': scheme}])
    # rsctypeId가 "FTH"이고 objId가 197인 문서들을 조회
    response = es.search(
        index=index_name,
        size=size,
        query={
            "bool": {
                "must": [
                    {"term": {"rsctypeId": "FTH"}},
                    {"term": {"objId": 197}}
                ]
            }
        }
    )
    return response["hits"]["hits"]


if __name__ == "__main__":
    # index_name = "rscstatrawmonth-fms-2024.05"
    index_name = "perfhist-fms-2025.07.31-0"
    
    # 1) 매핑 확인
    mapping_info = get_index_mapping(index_name=index_name)
    print(f"[매핑 정보: {index_name}]")
    print(json.dumps(mapping_info, indent=2, ensure_ascii=False))

    # 2) rsctypeId가 "FTH"인 문서 24건 가져오기
    fth_docs = get_fth_docs(index_name=index_name, size=24)
    if not fth_docs:
        print(f"{index_name} 인덱스에 rsctypeId가 'FTH'인 문서가 없습니다 (0 hits).")
    else:
        print(f"[{index_name}] 인덱스에서 rsctypeId='FTH'인 문서들 (최대 24건):")
        print(f"총 {len(fth_docs)}건의 문서를 찾았습니다.")
        print()
        
        for i, doc in enumerate(fth_docs, start=1):
            print(f"--- FTH Doc {i} ---")
            print(json.dumps(doc["_source"], indent=2, ensure_ascii=False))
            print()