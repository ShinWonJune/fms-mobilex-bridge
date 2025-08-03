import requests
import json

# Elasticsearch 접속 정보
ES_HOST = "http://10.20.2.21:59200"  # 실제 주소로 변경
INDEX_PATTERN = "perfhist-fms*"

# 검색할 날짜 범위 (UTC 기준)
start_time = "2025-05-26T00:00:00Z"
end_time = "2025-05-27T00:00:00Z"

# 검색 쿼리 정의
query_body = {
    "size": 10,
    "_source": ["@timestamp", "objId", "TEMPERATURE", "rsctypeId"],
    "query": {
        "bool": {
            "must": [
                {"range": {"@timestamp": {"gte": start_time, "lt": end_time}}},
                {"term": {"rsctypeId": "FTH"}}
            ]
        }
    },
    "sort": [{"@timestamp": {"order": "asc"}}]
}

# Elasticsearch 요청
url = f"{ES_HOST.rstrip('/')}/{INDEX_PATTERN}/_search"
headers = {"Content-Type": "application/json"}
response = requests.post(url, headers=headers, data=json.dumps(query_body))

# 결과 출력
if response.status_code == 200:
    data = response.json()
    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        print("❌ 데이터가 없습니다.")
    else:
        print(f"✅ {len(hits)}개 문서가 검색되었습니다:")
        for doc in hits:
            print(json.dumps(doc["_source"], indent=2, ensure_ascii=False))
else:
    print(f"❌ 요청 실패: {response.status_code} - {response.text}")
