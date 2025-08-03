import json
from datetime import datetime

def sort_jsonl_by_timestamp_and_humidity(
    input_file: str,
    output_file: str,
    timestamp_field: str = "@timestamp",
    humidity_field: str = "HUMIDITY1"
):
    """
    input_file의 JSONL 데이터를
    1) timestamp_field 기준 오름차순으로 정렬하고,
    2) timestamp가 동일할 경우 humidity_field 기준 오름차순으로 정렬하여
    output_file에 저장합니다.
    ISO8601 형식의 밀리초(.%f) 유무를 모두 지원합니다.
    """
    # 1. 모든 레코드 읽어오기
    with open(input_file, 'r', encoding='utf-8') as f:
        records = [json.loads(line) for line in f if line.strip()]

    # 2. timestamp 파싱 함수 정의
    def parse_ts(ts: str) -> datetime:
        # Z 제거
        if ts.endswith("Z"):
            ts = ts[:-1]
        # 밀리초 처리
        try:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

    # 3. 복합 정렬: (timestamp, humidity)
    records.sort(
        key=lambda rec: (
            parse_ts(rec.get(timestamp_field, "")),
            rec.get(humidity_field, 0)
        )
    )

    # 4. 정렬된 레코드 다시 쓰기
    with open(output_file, 'w', encoding='utf-8') as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')


def main():
    input_file = "rscstatrawday-fms-2025.06.11_FTH.jsonl"
    output_file = "rscstatrawday-fms-2025.06.11_FTH_sorted.jsonl"
    sort_jsonl_by_timestamp_and_humidity(input_file, output_file)
    print(f"[완료] '{output_file}'에 timestamp 및 humidity 기준 정렬 결과가 저장되었습니다.")


if __name__ == "__main__":
    main()
