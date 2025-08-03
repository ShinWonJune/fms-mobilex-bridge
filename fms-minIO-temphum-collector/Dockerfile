# Python 3.9 베이스 이미지 사용
FROM python:3.9-slim

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 패키지 업데이트 및 필수 도구 설치
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 파일 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 소스 코드 복사
COPY src/ ./src/
COPY scripts/ ./scripts/

# 스크립트 실행 권한 부여
RUN chmod +x scripts/*.sh

# 데이터 디렉토리 생성
RUN mkdir -p /app/data/checkpoints

# 비루트 사용자 생성 및 권한 설정
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# 환경변수 설정
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# 헬스체크 설정
HEALTHCHECK --interval=2m --timeout=30s --start-period=30s --retries=3 \
  CMD /app/scripts/health-check.sh

# 기본 명령어
CMD ["python", "src/streaming_pipeline.py", "--mode", "streaming"]