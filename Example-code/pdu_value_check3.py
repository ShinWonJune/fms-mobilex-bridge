from elasticsearch import Elasticsearch
import json
from datetime import datetime

def get_latest_documents(
    index_pattern,
    rsc_type="FPDUS",
    size=10,
    time_field="@timestamp",
    host="10.20.2.21",
    port=59200
):
    """
    특정 rsctypeId에 대한 가장 최근 문서 10개 조회
    """
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])

    query_body = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {"term": {"rsctypeId": rsc_type}}
                ]
            }
        },
        "sort": [
            {time_field: {"order": "desc"}}  # 최신 데이터부터 정렬
        ]
    }

    # 경고 해결: body 파라미터 대신 직접 파라미터 사용
    response = es.search(index=index_pattern, **query_body)
    
    # 총 개수 확인 수정
    total_docs = response['hits']['total']
    if isinstance(total_docs, dict) and 'value' in total_docs:
        total_count = total_docs['value']
    else:
        total_count = total_docs  # 이전 버전 호환
    
    print(f"총 {total_count}개 문서 중 최근 {size}개 조회:")
    
    for i, doc in enumerate(response['hits']['hits']):
        source = doc['_source']
        timestamp = source.get(time_field, "No timestamp")
        if isinstance(timestamp, str) and timestamp.endswith('Z'):
            # ISO 형식 날짜 변환
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
                
        print(f"\n=== 문서 {i+1} ===")
        print(f"Timestamp: {timestamp}")
        print(f"Index: {doc['_index']}")
        
        # 주요 필드 출력
        for field in ["objId","OUTPUT_CURRENT", "OUTPUT_POWER", "OUTPUT_VOLTAGE", "OUTPUT_FACTOR"]:
            if field in source:
                print(f"{field}: {source[field]}")
        
        # 추가 정보 출력
        for key in ["rsctypeId", "rscId", "metric_type"]:
            if key in source:
                print(f"{key}: {source[key]}")

if __name__ == "__main__":
    index_pattern = "perfhist-fms*"
    get_latest_documents(
        index_pattern=index_pattern,
        rsc_type="FPDUS",
        size=10,
        time_field="@timestamp"
    )