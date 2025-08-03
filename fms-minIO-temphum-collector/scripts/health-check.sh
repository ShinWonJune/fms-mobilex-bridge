#!/bin/bash

# 헬스체크 스크립트
# Docker 컨테이너가 정상 동작하는지 확인

set -e

# 체크포인트 파일 존재 확인
if [ ! -f "/app/streaming_checkpoint.json" ]; then
    echo "UNHEALTHY: Checkpoint file not found"
    exit 1
fi

# 체크포인트 파일이 최근 5분 내에 업데이트되었는지 확인
CHECKPOINT_AGE=$(( $(date +%s) - $(stat -c %Y /app/streaming_checkpoint.json) ))
if [ $CHECKPOINT_AGE -gt 300 ]; then
    echo "UNHEALTHY: Checkpoint file too old (${CHECKPOINT_AGE}s)"
    exit 1
fi

# Python 프로세스 확인
if ! pgrep -f "streaming_pipeline.py" > /dev/null; then
    echo "UNHEALTHY: Python process not running"
    exit 1
fi

# MinIO 연결 테스트 (선택사항)
if [ -n "$MINIO_ENDPOINT" ]; then
    python3 -c "
import os
from minio import Minio
try:
    client = Minio(
        os.environ['MINIO_ENDPOINT'],
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False
    )
    if not client.bucket_exists(os.environ['BUCKET_NAME']):
        raise Exception('Bucket not accessible')
    print('MinIO connection OK')
except Exception as e:
    print(f'MinIO connection failed: {e}')
    exit(1)
"
fi

echo "HEALTHY: All checks passed"
exit 0