from elasticsearch import Elasticsearch
import json

def check_field_caps_for_fpuds(index_pattern, fields, host="10.20.2.21", port=59200):
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])

    # field_caps API로 특정 필드만 조회 (fields=["OUTPUT_CURRENT","OUTPUT_POWER", ...])
    field_caps = es.field_caps(index=index_pattern, fields=fields)

    # 깔끔하게 출력
    print(json.dumps(field_caps, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    index_pattern = "perfhist-fms*"
    # 위에서 찾은 관심 필드들 중 숫자형으로 보이는 4개만 예시
    fields_of_interest = ["OUTPUT_CURRENT", "OUTPUT_FACTOR", "OUTPUT_POWER", "OUTPUT_VOLTAGE"]
    check_field_caps_for_fpuds(index_pattern, fields_of_interest)
