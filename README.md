# fms-mobilex-bridge
25년도 데이터 센터 디지털 트윈화를 위한 센터내 데이터 센싱용 서버 및 예제용 분석 코드 모음 ,  Collection of servers for data sensing in the center and sample analysis code for digital twinning of data centers in 2025


해당 도커 파일을 브릿지 서버에서 실행하면 주기적으로 FMS 서버에서 사용되는 일랙트릭 서치 API 를 호출해서 MobileX Clusteer 내 카프카로 전송
앞으로 문제 생기면 알아서들 직접 점검 및 고쳐서 사용해야함

해당 REPO는 코드를 복사하고 새로 REPO를 만들어서 운영하든지 해서 개발할 것
Docker Repo도 개인 Docker Repo에 배포했는데, 유지보수 어려울테니 새로 빌드해서 주도하는 사람의 Repo에 Push 작업 진행해야함

When this Docker file is executed on the bridge server, it periodically calls the Elasticsearch API used by the FMS server and transmits it to Kafka within the MobileX Cluster. If problems occur in the future, you'll need to inspect and fix it yourselves.
For this REPO, you should copy the code and create a new REPO for development and operation.


FROM INYONG

"""


"""

