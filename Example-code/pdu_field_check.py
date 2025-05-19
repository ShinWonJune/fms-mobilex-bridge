from elasticsearch import Elasticsearch
import json

def get_fields_for_rsctype(
    index_pattern,
    rsc_type='FPDUS',
    host='10.20.2.21',
    port=59200,
    sample_size=1000
):
    """
    특정 rsctypeId 문서에서 실제로 사용 중인 필드를 조사합니다.
    (샘플 문서 _source 기준)
    
    Args:
        index_pattern (str): 조회할 인덱스 패턴 (예: "perfhist-fms*").
        rsc_type (str): 필터링할 rsctypeId (예: "FPDUS").
        host (str): Elasticsearch/Kibana host
        port (int): Elasticsearch/Kibana port
        sample_size (int): 가져올 샘플 문서 수 (최대)
    
    Returns:
        set: rsctypeId=rsc_type 문서에서 발견된 필드들의 집합
    """
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])

    # rsctypeId로 필터링해서 sample_size 만큼 문서 가져오기
    query = {
        "size": sample_size,
        "_source": True,
        "query": {
            "term": {
                "rsctypeId": rsc_type
            }
        }
    }

    response = es.search(index=index_pattern, body=query)

    # 실제 사용 필드를 저장할 집합
    fields_set = set()

    # hits에서 _source를 확인하며 필드명 추출
    hits = response["hits"]["hits"]
    for hit in hits:
        source = hit["_source"]
        for field_name in source.keys():
            fields_set.add(field_name)

    return fields_set

if __name__ == "__main__":
    index_pattern = "perfhist-fms*"
    rsc_type = "FPDUS"
    
    fields = get_fields_for_rsctype(
        index_pattern=index_pattern,
        rsc_type=rsc_type,
        sample_size=1000
    )

    print(f"[rsctypeId={rsc_type}] 샘플 문서 기준 발견된 필드 목록 (총 {len(fields)}개):")
    for f in sorted(fields):
        print(f" - {f}")
