from elasticsearch import Elasticsearch
import json

def get_stats_for_numeric_fields(
    index_pattern,
    rsc_type="FPDUS",
    numeric_fields=("OUTPUT_CURRENT", "OUTPUT_POWER", "OUTPUT_VOLTAGE"),
    time_field="@timestamp",
    time_range="now-1d",
    host="10.20.2.21",
    port=59200
):
    """
    특정 rsctypeId와 시간 범위를 만족하는 문서에서,
    numeric_fields에 대한 stats (min, max, avg, count 등) 조회
    """
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])

    # stats 집계를 위해, 각 필드를 agg에 추가
    aggs_body = {}
    for f in numeric_fields:
        aggs_body[f"_stats_{f}"] = {
            "stats": {
                "field": f
            }
        }

    query_body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"rsctypeId": rsc_type}},
                    {"range": {time_field: {"gte": time_range}}}
                ]
            }
        },
        "aggs": aggs_body
    }

    response = es.search(index=index_pattern, body=query_body)

    # 결과 출력
    for f in numeric_fields:
        stats_agg_key = f"_stats_{f}"
        if stats_agg_key in response["aggregations"]:
            agg_result = response["aggregations"][stats_agg_key]
            print(f"\n=== Field: {f} ===")
            print(f"Count: {agg_result['count']}")
            print(f"Min:   {agg_result['min']}")
            print(f"Max:   {agg_result['max']}")
            print(f"Avg:   {agg_result['avg']}")
            print(f"Sum:   {agg_result['sum']}")

if __name__ == "__main__":
    index_pattern = "perfhist-fms*"
    fields_to_check = ("OUTPUT_CURRENT", "OUTPUT_FACTOR", "OUTPUT_POWER", "OUTPUT_VOLTAGE")
    get_stats_for_numeric_fields(
        index_pattern=index_pattern,
        rsc_type="FPDUS",
        numeric_fields=fields_to_check,
        time_field="@timestamp",
        time_range="now-1d"
    )
