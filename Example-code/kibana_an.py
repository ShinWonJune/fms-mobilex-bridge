"""

제미나이랑 Chatgpt로 작성해달라고 요청힌 Kibana Anomaly Detection 테스트 코드
지울려다가 참고용으로 남겨둠 

"""

from elasticsearch import Elasticsearch

def check_objId_overlap(index_pattern, host="10.20.2.21", port=59200):  # Kibana 포트 지울려다가 남김
    """
    Elasticsearch 인덱스에서 rsctypeId 별로 objId 값이 겹치는지 확인합니다.

    Args:
        index_pattern (str): Elasticsearch 인덱스 패턴 (예: "perfhist-fms*").
        host (str): Elasticsearch/Kibana host
        port (int): Elasticsearch/Kibana port

    Returns:
        dict: rsctypeId를 키로, 중복되지 않는 objId 목록을 값으로 하는 딕셔너리.
              objId가 중복되면 None.
    """

    # scheme="http" 추가
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])

    # 1. 모든 rsctypeId 값 가져오기
    rsc_types_query = {
        "size": 0,
        "aggs": {
            "rsctypeId_values": {
                "terms": {
                    "field": "rsctypeId",
                    "size": 10000  # 충분히 큰 값
                }
            }
        }
    }
    rsc_types_response = es.search(index=index_pattern, body=rsc_types_query)
    rsctype_ids = [bucket["key"] for bucket in rsc_types_response["aggregations"]["rsctypeId_values"]["buckets"]]

    # 2. 각 rsctypeId 별로 objId 값 가져오기
    objId_by_rsctype = {}
    for rsctype_id in rsctype_ids:
        objId_query = {
            "size": 0,
            "query": {
                "term": {
                    "rsctypeId": rsctype_id
                }
            },
            "aggs": {
                "object_id_values": {
                    "terms": {
                        "field": "objId",
                        "size": 10000  # objId 개수만큼 충분히 크게
                    }
                }
            }
        }
        objId_response = es.search(index=index_pattern, body=objId_query)
        objId_by_rsctype[rsctype_id] = [bucket["key"] for bucket in objId_response["aggregations"]["object_id_values"]["buckets"]]

    # 3. objId 중복 검사
    all_objIds = set()
    duplicates_found = False
    for rsctype_id, objId_list in objId_by_rsctype.items():
        for objId in objId_list:
            if objId in all_objIds:
                print(f"objId '{objId}' is duplicated across rsctypeIds.")
                duplicates_found = True
                objId_by_rsctype[rsctype_id] = None  # 중복 표시
            else:
                all_objIds.add(objId)
    if not duplicates_found:
        print("No objId duplication found across rsctypeIds.")

    return objId_by_rsctype



if __name__ == "__main__":
    index_pattern = "perfhist-fms*"  # 실제 인덱스 패턴으로 변경
    result = check_objId_overlap(index_pattern)

    for rsctype, obj_ids in result.items():
      if obj_ids is not None:
        print(f"rsctypeId: {rsctype}, Unique objIds: {obj_ids}")
      else:
        print(f"rsctypeId: {rsctype}, objIds: DUPLICATED")