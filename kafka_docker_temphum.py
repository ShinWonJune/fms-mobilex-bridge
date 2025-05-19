#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
docker run 예시:
  docker run --name fms-temphum \
    --es-url {Google Slide} \
    --index-pattern perfhist-fms* \
    --kafka-brokers {Google Slide} \
    --kafka-topic fms-temphum \
    --poll-interval 60 \
    --checkpoint-file checkpoint_filter.json \
    --fields TEMPERATURE,HUMIDITY,TEMPERATURE1,HUMIDITY1,objId,rsctypeId
"""

import os
import json
import time
import requests
import argparse
from datetime import datetime
from kafka import KafkaProducer

def parse_args():
    parser = argparse.ArgumentParser(description="FTH (Temp/Hum) Data to Kafka")
    parser.add_argument("--es-url", 
        default=os.environ.get("ES_URL", "http://localhost:9200"),
        help="Elasticsearch URL (default: http://localhost:9200 or ES_URL env)"
    )
    parser.add_argument("--index-pattern", 
        default=os.environ.get("ES_INDEX_PATTERN", "perfhist-fms*"),
        help="Elasticsearch index pattern (default: perfhist-fms*)"
    )
    parser.add_argument("--kafka-brokers", 
        default=os.environ.get("KAFKA_BROKERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument("--kafka-topic", 
        default=os.environ.get("KAFKA_TOPIC", "fms-temphum"),  # 바뀐 토픽명
        help="Kafka topic name (default: fms-temphum)"
    )
    parser.add_argument("--poll-interval", 
        type=int, 
        default=int(os.environ.get("POLL_INTERVAL_SEC", "60")),
        help="Polling interval in seconds (default: 60)"
    )
    parser.add_argument("--checkpoint-file", 
        default=os.environ.get("CHECKPOINT_FILE", "checkpoint_filter.json"),
        help="Local file path to store last @timestamp (default: checkpoint_filter.json)"
    )
    parser.add_argument("--fetch-size", 
        type=int, 
        default=int(os.environ.get("FETCH_SIZE", "500")),
        help="Number of docs to fetch per request (default: 500)"
    )
    parser.add_argument("--start-timestamp",
        default=os.environ.get("START_TIMESTAMP", "1970-01-01T00:00:00Z"),
        help="If checkpoint file not found, start from this timestamp (default: 1970-01-01T00:00:00Z)"
    )
    parser.add_argument("--fields",
        default=os.environ.get("FIELDS", ""), 
        help="Comma-separated list of fields to keep in the document. "
             "If empty, all fields are returned. (default: '')"
    )
    return parser.parse_args()

def load_checkpoint(checkpoint_file: str, default_ts: str) -> str:
    if not os.path.exists(checkpoint_file):
        return default_ts
    with open(checkpoint_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data.get("last_timestamp", default_ts)

def save_checkpoint(checkpoint_file: str, last_ts: str):
    data = {"last_timestamp": last_ts}
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch_new_docs(es_url: str, index_pattern: str, since_timestamp: str, size: int = 500) -> list:
    """
    FTH(rsctypeId=FTH) 문서만 가져오기 위해 bool 쿼리로 @timestamp + term(rsctypeId='FTH') 필터.
    """
    url = f"{es_url.rstrip('/')}/{index_pattern}/_search"
    query_body = {
        "size": size,
        "sort": [
            {"@timestamp": {"order": "asc"}}
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gt": since_timestamp
                            }
                        }
                    },
                    {
                        "term": {
                            "rsctypeId": "FTH"
                        }
                    }
                ]
            }
        }
    }
    resp = requests.post(url, json=query_body)
    resp.raise_for_status()
    data = resp.json()
    
    hits = data.get("hits", {}).get("hits", [])
    docs = []
    for h in hits:
        src = h.get("_source", {})
        docs.append(src)
    return docs

def filter_fields(doc: dict, keep_fields: list) -> dict:
    """
    --fields 를 지정한 경우 그 필드만 추출.
    """
    if not keep_fields:
        return doc  # 필드 목록이 비어있으면 전체 전달
    
    filtered = {}
    for field in keep_fields:
        if field in doc:
            filtered[field] = doc[field]
    # @timestamp는 항상 포함하고 싶다면:
    if "@timestamp" not in filtered and "@timestamp" in doc:
        filtered["@timestamp"] = doc["@timestamp"]
    return filtered

# base_schema: TEMPERATURE, HUMIDITY, TEMPERATURE1, HUMIDITY1, objId, rsctypeId, ...
base_schema = {
    "@timestamp": None,
    "objId": None,
    "rsctypeId": None,
    
    "TEMPERATURE": None,
    "TEMPERATURE1": None,
    "HUMIDITY": None,
    "HUMIDITY1": None
}

def apply_base_schema(doc: dict, base_schema: dict) -> dict:
    new_doc = dict(base_schema)
    for k, v in doc.items():
        if k in new_doc:
            new_doc[k] = v
    return new_doc

def is_effectively_empty(doc: dict) -> bool:
    """
    @timestamp 외에 전부 None이면 True
    """
    for k, v in doc.items():
        if k == "@timestamp":
            continue
        if v is not None:
            return False
    return True

def main():
    args = parse_args()
    
    es_url = args.es_url
    index_pattern = args.index_pattern
    kafka_brokers = args.kafka_brokers
    kafka_topic = args.kafka_topic
    poll_interval = args.poll_interval
    checkpoint_file = args.checkpoint_file
    fetch_size = args.fetch_size
    default_start_ts = args.start_timestamp
    
    # --fields 파싱
    if args.fields.strip():
        keep_fields = [f.strip() for f in args.fields.split(",")]
    else:
        keep_fields = []
    
    # Kafka Producer
    producer = KafkaProducer(bootstrap_servers=kafka_brokers)
    
    # 체크포인트 로드
    last_timestamp = load_checkpoint(checkpoint_file, default_start_ts)
    print(f"[INFO] Loaded last timestamp from checkpoint: {last_timestamp}")
    
    while True:
        try:
            # 1) FTH 문서만 가져오기
            new_docs = fetch_new_docs(es_url, index_pattern, last_timestamp, size=fetch_size)
            if not new_docs:
                print("[INFO] No new FTH documents found.")
            else:
                print(f"[INFO] Fetched {len(new_docs)} new FTH documents.")
                
                local_max_ts = last_timestamp
                for doc in new_docs:
                    doc_ts = doc.get("@timestamp")

                    # --fields 필터
                    filtered_doc = filter_fields(doc, keep_fields)

                    # base_schema 적용
                    unified_doc = apply_base_schema(filtered_doc, base_schema)

                    # 모두 None이면 skip
                    if is_effectively_empty(unified_doc):
                        continue

                    # 카프카 전송
                    payload = json.dumps(unified_doc, ensure_ascii=False).encode("utf-8")
                    producer.send(kafka_topic, payload)

                    # timestamp 갱신
                    if doc_ts and doc_ts > local_max_ts:
                        local_max_ts = doc_ts

                producer.flush()

                if local_max_ts != last_timestamp:
                    save_checkpoint(checkpoint_file, local_max_ts)
                    last_timestamp = local_max_ts
                    print(f"[INFO] Updated checkpoint to {local_max_ts}")
        
        except Exception as e:
            print(f"[ERROR] {e}")
        
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()
