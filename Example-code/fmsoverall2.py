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

if __name__ == "__main__":
    index_name = "rscstatrawmonth-fms-2024.06"
    
    # 1) 매핑 확인
    mapping_info = get_index_mapping(index_name=index_name)
    print(f"[매핑 정보: {index_name}]")
    print(json.dumps(mapping_info, indent=2, ensure_ascii=False))

    # 2) 샘플 문서 5건 가져오기
    docs = get_sample_docs(index_name=index_name, size=5)
    if not docs:
        print(f"{index_name} 인덱스에 문서가 없습니다 (0 hits).")
    else:
        print(f"[{index_name}] 인덱스 샘플 문서 (최대 5건):")
        for i, doc in enumerate(docs, start=1):
            print(f"--- Doc {i} ---")
            print(json.dumps(doc["_source"], indent=2, ensure_ascii=False))
            print()
