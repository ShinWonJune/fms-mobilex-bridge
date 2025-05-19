"""
해당 코드를 실행하면 각 rscstatrawweek-fms- 파일이 어느 날짜부터 얼마나 저장되었나를 시각화 가능함함

이런 패턴은 월별별
(elasticsearch_env) netai@netai-NUC10i7FNH:~$ /home/netai/elasticsearch_env/bin/python /home/netai/fms-analy/fmsoverall4.py
[rscstatrawmonth-fms-2024.03] earliest=2024-02-29T15:00:00.000Z, latest=2024-03-31T13:00:00.000Z, total_days=31일 정도
[rscstatrawmonth-fms-2024.04] earliest=2024-03-31T15:00:00.000Z, latest=2024-04-30T13:00:00.000Z, total_days=30일 정도
[rscstatrawmonth-fms-2024.05] earliest=2024-04-30T15:00:00.001Z, latest=2024-05-31T13:00:00.000Z, total_days=31일 정도

이란 패턴은 주간간

rscstatrawweek-fms-2024.36          | size= 41.3mb, docs= 196560, earliest=2024-08-31T15:00:00.000Z, latest=2024-09-07T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.37          | size= 41.7mb, docs= 196560, earliest=2024-09-07T15:00:00.000Z, latest=2024-09-14T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.38          | size= 41.4mb, docs= 195318, earliest=2024-09-14T15:00:00.000Z, latest=2024-09-21T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.39          | size= 41.7mb, docs= 195768, earliest=2024-09-21T15:00:00.000Z, latest=2024-09-28T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.40          | size= 41.7mb, docs= 196560, earliest=2024-09-28T15:00:00.000Z, latest=2024-10-05T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.41          | size= 41.6mb, docs= 196560, earliest=2024-10-05T15:00:00.000Z, latest=2024-10-12T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.42          | size= 41.7mb, docs= 196560, earliest=2024-10-12T15:00:00.000Z, latest=2024-10-19T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.43          | size= 41.8mb, docs= 196560, earliest=2024-10-19T15:00:00.000Z, latest=2024-10-26T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.44          | size=   41mb, docs= 195930, earliest=2024-10-26T15:00:00.001Z, latest=2024-11-02T14:30:00.001Z, days=7
rscstatrawweek-fms-2024.45          | size=   41mb, docs= 194814, earliest=2024-11-02T15:00:00.000Z, latest=2024-11-09T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.46          | size= 42.6mb, docs= 196560, earliest=2024-11-09T15:00:00.001Z, latest=2024-11-16T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.47          | size= 42.4mb, docs= 196560, earliest=2024-11-16T15:00:00.000Z, latest=2024-11-23T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.48          | size= 45.6mb, docs= 196560, earliest=2024-11-23T15:00:00.000Z, latest=2024-11-30T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.49          | size= 52.3mb, docs= 196560, earliest=2024-11-30T15:00:00.000Z, latest=2024-12-07T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.50          | size= 42.3mb, docs= 196560, earliest=2024-12-07T15:00:00.000Z, latest=2024-12-14T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.51          | size= 53.4mb, docs= 195576, earliest=2024-12-14T15:00:00.000Z, latest=2024-12-21T14:30:00.000Z, days=7
rscstatrawweek-fms-2024.52          | size= 52.7mb, docs= 194544, earliest=2024-12-21T15:00:00.000Z, latest=2024-12-28T14:30:00.000Z, days=7
rscstatrawweek-fms-2025.01          | size= 55.9mb, docs= 194544, earliest=2024-12-28T15:00:00.000Z, latest=2025-01-04T14:30:00.000Z, days=7


"""

import json
import math
from datetime import datetime
from elasticsearch import Elasticsearch

def cat_indices_info(index_pattern, es_client):
    """
    cat.indices API를 호출해, 
     - 인덱스명
     - store.size
     - docs.count
    를 가져온다.
    """
    cat_response = es_client.cat.indices(index=index_pattern, h="index,store.size,docs.count", s="index")
    if not cat_response:
        return []
    
    lines = cat_response.strip().split("\n")
    results = []
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        idx_name, size_str, docs_str = parts[0], parts[1], parts[2]
        try:
            docs_count = int(docs_str)
        except ValueError:
            docs_count = 0
        results.append({
            "index": idx_name,
            "store_size": size_str,
            "docs_count": docs_count
        })
    return results

def get_earliest_latest_ts(index_name, es_client, time_field="@timestamp"):
    """
    해당 인덱스에서 time_field 기준 가장 빠른/가장 늦은 문서의 타임스탬프를 각각 반환
    (문서가 없으면 (None, None))
    """
    # Earliest
    earliest_res = es_client.search(
        index=index_name,
        size=1,
        query={"match_all": {}},
        sort=[{time_field: {"order": "asc"}}]
    )
    earliest_hits = earliest_res["hits"]["hits"]
    if not earliest_hits:
        return (None, None)
    earliest_ts = earliest_hits[0]["_source"].get(time_field)

    # Latest
    latest_res = es_client.search(
        index=index_name,
        size=1,
        query={"match_all": {}},
        sort=[{time_field: {"order": "desc"}}]
    )
    latest_hits = latest_res["hits"]["hits"]
    if not latest_hits:
        return (None, None)
    latest_ts = latest_hits[0]["_source"].get(time_field)

    return (earliest_ts, latest_ts)

def days_between(ts1, ts2):
    """
    두 ISO8601 시간 문자열의 일수 차이(절대값).
    예: 2024-06-01T03:00:00.000Z -> datetime 변환
    """
    if not ts1 or not ts2:
        return None
    try:
        # Z(UTC) -> +00:00 변환
        dt1 = datetime.fromisoformat(ts1.replace("Z", "+00:00"))
        dt2 = datetime.fromisoformat(ts2.replace("Z", "+00:00"))
        diff_days = abs((dt2 - dt1).total_seconds() / 86400.0)
        return math.floor(diff_days)
    except:
        return None

if __name__ == "__main__":
    es = Elasticsearch([{"host": "10.20.2.21", "port": 59200, "scheme": "http"}])

    # 조사할 인덱스 패턴들 (월별, 주별, 연별 등)
    index_patterns = [
        "rscstatrawmonth-fms-*",
        "rscstatrawweek-fms-*",
        "rscstatrawyear-fms-*"
    ]

    # 결과를 저장할 리스트
    results_all = []

    for pat in index_patterns:
        # cat.indices 해서 인덱스 목록+size+docs 가져오기
        idx_info_list = cat_indices_info(pat, es)
        for idx_info in idx_info_list:
            idx_name = idx_info["index"]
            store_size = idx_info["store_size"]
            docs_count = idx_info["docs_count"]

            # earliest/latest timestamp
            e_ts, l_ts = get_earliest_latest_ts(idx_name, es)
            if not e_ts or not l_ts:
                # 문서가 없거나 time_field가 없는 경우
                results_all.append({
                    "index": idx_name,
                    "store_size": store_size,
                    "docs_count": docs_count,
                    "earliest_ts": None,
                    "latest_ts": None,
                    "days_covered": 0
                })
            else:
                day_diff = days_between(e_ts, l_ts)
                if day_diff is not None:
                    # day_diff+1 하면 “실질적인 날짜 수”로 볼 수도 있음
                    days_covered = day_diff + 1
                else:
                    days_covered = 0
                results_all.append({
                    "index": idx_name,
                    "store_size": store_size,
                    "docs_count": docs_count,
                    "earliest_ts": e_ts,
                    "latest_ts": l_ts,
                    "days_covered": days_covered
                })

    # 인덱스명 기준 정렬
    results_all.sort(key=lambda x: x["index"])

    # 출력
    print("\n=== rscstatraw(월/주/년) 인덱스 현황 종합 ===")
    for r in results_all:
        print(f"{r['index']:35} | size={r['store_size']:>7}, docs={r['docs_count']:>7}, "
              f"earliest={r['earliest_ts']}, latest={r['latest_ts']}, days={r['days_covered']}")
