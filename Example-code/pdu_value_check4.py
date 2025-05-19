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
    특정 rsctypeId에 대한 가장 최근 문서 10개를 표 형식으로 조회
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
    
    # 헤더 및 구분선 정의
    headers = ["#", "Timestamp", "objId", "OUTPUT_CURRENT", "OUTPUT_POWER", "OUTPUT_VOLTAGE", "OUTPUT_FACTOR", "rscId"]
    col_widths = [3, 22, 20, 15, 15, 15, 15, 20]
    
    # 헤더 출력
    header_format = ""
    for i, width in enumerate(col_widths):
        header_format += f"{{:{width}}}"
    print("\n" + header_format.format(*headers))
    
    # 구분선 출력
    separator = "-" * sum(col_widths)
    print(separator)
    
    # 데이터 행 출력
    row_format = ""
    for i, width in enumerate(col_widths):
        row_format += f"{{:{width}}}"
    
    for i, doc in enumerate(response['hits']['hits']):
        source = doc['_source']
        
        # 타임스탬프 변환
        timestamp = source.get(time_field, "No timestamp")
        if isinstance(timestamp, str) and timestamp.endswith('Z'):
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        # 값 준비 및 길이 제한
        obj_id = str(source.get("objId", "N/A"))[:19]
        output_current = str(source.get("OUTPUT_CURRENT", "N/A"))[:14]
        output_power = str(source.get("OUTPUT_POWER", "N/A"))[:14]
        output_voltage = str(source.get("OUTPUT_VOLTAGE", "N/A"))[:14]
        output_factor = str(source.get("OUTPUT_FACTOR", "N/A"))[:14]
        rsc_id = str(source.get("rscId", "N/A"))[:19]
        
        # 행 출력
        row = [str(i+1), timestamp, obj_id, output_current, output_power, output_voltage, output_factor, rsc_id]
        print(row_format.format(*row))
    
    # 추가 정보
    if response['hits']['hits']:
        print(f"\n* 인덱스: {response['hits']['hits'][0]['_index']}")
        print(f"* rsctypeId: {rsc_type}")

if __name__ == "__main__":
    index_pattern = "perfhist-fms*"
    get_latest_documents(
        index_pattern=index_pattern,
        rsc_type="FPDUS",
        size=10,
        time_field="@timestamp"
    )