# Dockerfile

FROM python:3.11-slim

# 작업 디렉토리 생성
WORKDIR /app

# 파이썬 의존성 사전 설치 (requirements.txt가 있는 경우)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 메인 스크립트 복사
COPY kafka_docker_temphum.py .

# 컨테이너 시작 시, 스크립트 실행 (인자는 ENTRYPOINT 뒤에 붙음)
ENTRYPOINT ["python", "kafka_docker_temphum.py"]
