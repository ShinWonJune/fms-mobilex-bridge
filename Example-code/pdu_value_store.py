from elasticsearch import Elasticsearch
from datetime import datetime
import os
import csv

def get_all_documents_to_csv(
    index_pattern,
    rsc_type="FPDUS",
    batch_size=1000,
    time_field="@timestamp",
    host="10.20.2.21",
    port=59200,
    out_csv="all_pdu_readings.csv"
):
    es = Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}])

    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"rsctypeId": rsc_type}}
                ]
            }
        }
    }

    # kick off the scroll
    page = es.search(
        index=index_pattern,
        body=query_body,
        scroll='2m',
        size=batch_size,
        _source_includes=[time_field, "objId", "OUTPUT_CURRENT", "OUTPUT_POWER", "OUTPUT_VOLTAGE", "OUTPUT_FACTOR", "rscId"]
    )
    scroll_id = page['_scroll_id']
    total = page['hits']['total']
    total_count = total['value'] if isinstance(total, dict) else total
    print(f"총 {total_count}개 문서 중 scroll로 모두 조회, batch size={batch_size}")

    # prepare CSV
    headers = ["#", "Timestamp", "objId", "OUTPUT_CURRENT", "OUTPUT_POWER", "OUTPUT_VOLTAGE", "OUTPUT_FACTOR", "rscId"]
    with open(out_csv, mode="w", newline="", encoding="utf-8") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(headers)

        idx = 0
        hits = page['hits']['hits']
        while hits:
            for doc in hits:
                idx += 1
                src = doc['_source']

                # timestamp normalization
                ts = src.get(time_field, "")
                if isinstance(ts, str) and ts.endswith('Z'):
                    try:
                        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                        ts = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass

                row = [
                    idx,
                    ts,
                    src.get("objId", ""),
                    src.get("OUTPUT_CURRENT", ""),
                    src.get("OUTPUT_POWER", ""),
                    src.get("OUTPUT_VOLTAGE", ""),
                    src.get("OUTPUT_FACTOR", ""),
                    src.get("rscId", ""),
                ]
                writer.writerow(row)

            # get next batch
            page = es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = page['_scroll_id']
            hits = page['hits']['hits']

    # clear the scroll context
    es.clear_scroll(scroll_id=scroll_id)
    print(f"* CSV saved to: {os.path.abspath(out_csv)}  (total rows: {idx})")


if __name__ == "__main__":
    get_all_documents_to_csv(index_pattern="perfhist-fms*")
