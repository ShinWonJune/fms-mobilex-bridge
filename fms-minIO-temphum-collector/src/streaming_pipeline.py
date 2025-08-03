#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
개선된 실시간 파이프라인:
- 매분 정각에 실시간 데이터 수집
- 매분 10초에 이전 분 데이터 병합
- 완전한 시간적 분리로 동시성 문제 해결
- 2025년 5월 데이터를 1주일씩 묶어서 배치 처리
- 도커 컴포즈 환경 지원
"""

import os
import json
import time
import requests
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import tempfile
import logging
import threading
import schedule
from concurrent.futures import ThreadPoolExecutor
import hashlib
import pytz
from dateutil import parser as date_parser

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Streaming FTH Data Pipeline")
    
    # Elasticsearch 설정
    parser.add_argument("--es-url", 
        default=os.environ.get("ES_URL", "http://localhost:9200"))
    parser.add_argument("--index-pattern", 
        default=os.environ.get("ES_INDEX_PATTERN", "perfhist-fms*"))
    
    # MinIO 설정
    parser.add_argument("--minio-endpoint", 
        default=os.environ.get("MINIO_ENDPOINT", "localhost:9000"))
    parser.add_argument("--minio-access-key", 
        default=os.environ.get("MINIO_ACCESS_KEY", ""))
    parser.add_argument("--minio-secret-key", 
        default=os.environ.get("MINIO_SECRET_KEY", ""))
    parser.add_argument("--bucket-name", 
        default=os.environ.get("BUCKET_NAME", "fms-data"))
    
    # 실행 설정
    parser.add_argument("--mode", 
        choices=["streaming", "batch"],
        default="streaming",
        help="Operation mode")
    parser.add_argument("--target-month", 
        default="2025-06",
        help="Target month for batch processing")
    parser.add_argument("--fields",
        default="TEMPERATURE,HUMIDITY,TEMPERATURE1,HUMIDITY1,objId,rsctypeId")
    
    return parser.parse_args()

class StreamingPipeline:
    def __init__(self, args):
        self.args = args
        self.minio_client = Minio(
            args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
            secure=False
        )
        self.bucket_name = args.bucket_name
        self.es_url = args.es_url
        self.index_pattern = args.index_pattern
        self.keep_fields = [f.strip() for f in args.fields.split(",")]
        
        # 시간대 설정
        self.utc_tz = pytz.UTC
        self.kst_tz = pytz.timezone('Asia/Seoul')  # 한국 표준시
        
        # 체크포인트 디렉토리 설정 (도커 볼륨 마운트 고려)
        self.checkpoint_dir = self._get_checkpoint_dir()
        
        self._ensure_bucket()
        self._setup_checkpoint()
        
        # 스레드풀 설정
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pipeline")
        
        logger.info(f"Streaming pipeline initialized with KST timezone conversion")
        logger.info(f"Checkpoint directory: {self.checkpoint_dir}")
    
    def _get_checkpoint_dir(self):
        """체크포인트 디렉토리 경로 결정 (도커 환경 고려)"""
        # 도커 환경에서는 /app/data/checkpoints 사용
        if os.path.exists('/app/data/checkpoints'):
            return '/app/data/checkpoints'
        # 개발 환경에서는 현재 디렉토리의 data/checkpoints 사용
        elif os.path.exists('./data/checkpoints'):
            return './data/checkpoints'
        # 기본값으로 현재 디렉토리 사용
        else:
            checkpoint_dir = './checkpoints'
            os.makedirs(checkpoint_dir, exist_ok=True)
            return checkpoint_dir
    
    def convert_to_kst(self, utc_timestamp_str):
        """UTC 시간 문자열을 KST로 변환"""
        try:
            # UTC 시간으로 파싱
            if isinstance(utc_timestamp_str, str):
                utc_dt = date_parser.parse(utc_timestamp_str)
            else:
                utc_dt = utc_timestamp_str
            
            # UTC 시간대 정보가 없으면 추가
            if utc_dt.tzinfo is None:
                utc_dt = self.utc_tz.localize(utc_dt)
            
            # KST로 변환
            kst_dt = utc_dt.astimezone(self.kst_tz)
            
            return kst_dt
        except Exception as e:
            logger.error(f"Failed to convert timestamp {utc_timestamp_str}: {e}")
            return None

    def get_kst_now(self):
        """현재 KST 시간 반환 (일관된 방법)"""
        utc_now = datetime.utcnow()
        kst_time = self.convert_to_kst(utc_now.isoformat() + 'Z')
        return kst_time.replace(tzinfo=None) if kst_time else utc_now + timedelta(hours=9)

    def utc_to_kst_offset(self, utc_dt):
        """UTC 시간에 9시간 더해서 KST로 변환 (간단한 방법)"""
        return utc_dt + timedelta(hours=9)
    
    def process_dataframe_timestamps(self, df):
        """DataFrame의 타임스탬프를 KST로 변환"""
        if df.empty or '@timestamp' not in df.columns:
            return df
        
        try:
            df = df.copy()  # 원본 변경 방지
            
            # 원본 UTC 타임스탬프 백업
            df['@timestamp_utc'] = df['@timestamp'].astype(str)
            
            # @timestamp가 Unix timestamp(밀리초)인지 ISO 문자열인지 확인
            sample_timestamp = df['@timestamp'].iloc[0]
            
            if isinstance(sample_timestamp, (int, float)) or (isinstance(sample_timestamp, str) and sample_timestamp.isdigit()):
                # Unix timestamp (밀리초) 처리
                logger.debug("Processing Unix timestamp format")
                df['@timestamp'] = pd.to_datetime(df['@timestamp'], unit='ms', utc=True)  
            else:
                # ISO 문자열 처리
                logger.debug("Processing ISO string format")
                df['@timestamp'] = pd.to_datetime(df['@timestamp'], utc=True)
            
            # KST로 변환 (UTC + 9시간)
            df_kst = df['@timestamp'].dt.tz_convert('Asia/Seoul')
            
            # KST를 읽기 쉬운 문자열 형식으로 변환
            df['@timestamp'] = df_kst.dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
            
            # 디버깅을 위한 샘플 출력
            logger.debug(f"Sample conversions:")
            logger.debug(f"  Original: {df['@timestamp_utc'].iloc[0]}")
            logger.debug(f"  KST: {df['@timestamp'].iloc[0]}")
            
            logger.debug(f"Converted timestamps to KST for {len(df)} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to process timestamps: {e}")
            logger.error(f"Sample timestamp: {df['@timestamp'].iloc[0] if len(df) > 0 else 'No data'}")
            return df
    
    def _ensure_bucket(self):
        """버킷 생성"""
        try:
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to ensure bucket: {e}")
            raise
    
    def _setup_checkpoint(self):
        """체크포인트 초기화"""
        self.checkpoint_file = os.path.join(self.checkpoint_dir, "streaming_checkpoint.json")
        if not os.path.exists(self.checkpoint_file):
            # 최근 1시간 전부터 시작 (너무 오래된 데이터 방지)
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            checkpoint = {
                "last_timestamp": one_hour_ago.isoformat() + "Z",
                "last_processed_minute": None
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
    
    def _load_checkpoint(self):
        """체크포인트 로드"""
        try:
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return {"last_timestamp": (datetime.utcnow() - timedelta(hours=1)).isoformat() + "Z"}
    
    def _save_checkpoint(self, checkpoint):
        """체크포인트 저장"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def object_exists(self, object_name):
        """MinIO 객체 존재 여부 확인"""
        try:
            self.minio_client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            raise

    def load_parquet_safe(self, object_name):
        """안전한 Parquet 파일 로드"""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            try:
                self.minio_client.fget_object(self.bucket_name, object_name, tmp_file.name)
                return pd.read_parquet(tmp_file.name)
            finally:
                if os.path.exists(tmp_file.name):
                    os.unlink(tmp_file.name)
    
    def fetch_elasticsearch_data_paginated(self, start_time, end_time, max_retries=5):
        """Elasticsearch에서 모든 데이터를 Scroll API로 가져오기 (10,000개 제한 해결)"""
        all_docs = []
        
        url = f"{self.es_url.rstrip('/')}/{self.index_pattern}/_search"
        
        # 첫 번째 스크롤 요청
        initial_query = {
            "size": 1000,  # 스크롤에서는 더 큰 배치 크기 사용 가능
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_time,
                                    "lt": end_time
                                }
                            }
                        },
                        {
                            "term": {"rsctypeId": "FTH"}
                        }
                    ]
                }
            },
            "timeout": "60s"
        }
        
        # Scroll 시작
        scroll_url = url + "?scroll=2m"  # 2분 scroll timeout
        
        for attempt in range(max_retries):
            try:
                resp = requests.post(scroll_url, json=initial_query, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                
                scroll_id = data.get("_scroll_id")
                hits = data.get("hits", {}).get("hits", [])
                total_hits = data.get("hits", {}).get("total", {})
                
                if isinstance(total_hits, dict):
                    total_count = total_hits.get("value", 0)
                else:
                    total_count = total_hits
                
                logger.info(f"Started scroll: {len(hits)} records (total: {total_count})")
                
                if not hits:
                    logger.info("No data found")
                    return []
                
                # 첫 번째 배치 처리
                page_docs = []
                for hit in hits:
                    source = hit.get("_source", {})
                    filtered = {}
                    for field in self.keep_fields:
                        if field in source:
                            filtered[field] = source[field]
                    if "@timestamp" in source:
                        filtered["@timestamp"] = source["@timestamp"]
                    page_docs.append(filtered)
                
                all_docs.extend(page_docs)
                batch_num = 1
                
                # 스크롤 계속
                scroll_search_url = f"{self.es_url.rstrip('/')}/_search/scroll"
                
                while True:
                    scroll_query = {
                        "scroll": "2m",
                        "scroll_id": scroll_id
                    }
                    
                    try:
                        scroll_resp = requests.post(scroll_search_url, json=scroll_query, timeout=60)
                        scroll_resp.raise_for_status()
                        scroll_data = scroll_resp.json()
                        
                        scroll_hits = scroll_data.get("hits", {}).get("hits", [])
                        scroll_id = scroll_data.get("_scroll_id")  # 새로운 scroll_id 업데이트
                        
                        if not scroll_hits:
                            logger.info(f"Scroll completed. Total fetched: {len(all_docs)} records")
                            break
                        
                        batch_num += 1
                        logger.info(f"Fetched batch {batch_num}: {len(scroll_hits)} records "
                                   f"(progress: {len(all_docs)}/{total_count})")
                        
                        # 배치 데이터 처리
                        batch_docs = []
                        for hit in scroll_hits:
                            source = hit.get("_source", {})
                            filtered = {}
                            for field in self.keep_fields:
                                if field in source:
                                    filtered[field] = source[field]
                            if "@timestamp" in source:
                                filtered["@timestamp"] = source["@timestamp"]
                            batch_docs.append(filtered)
                        
                        all_docs.extend(batch_docs)
                        
                        # 진행률 출력 (매 10배치마다)
                        if batch_num % 10 == 0:
                            logger.info(f"Progress: Fetched {len(all_docs)} records so far...")
                        
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Scroll request failed: {e}")
                        break
                
                # 스크롤 정리
                try:
                    if scroll_id:
                        clear_url = f"{self.es_url.rstrip('/')}/_search/scroll"
                        clear_query = {"scroll_id": [scroll_id]}
                        requests.delete(clear_url, json=clear_query, timeout=10)
                        logger.debug("Scroll context cleared")
                except:
                    pass  # 정리 실패해도 계속 진행
                
                return all_docs
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"All scroll attempts failed: {e}")
                    return all_docs
                
                wait_time = min(2 ** attempt, 10)
                logger.warning(f"Scroll attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
        
        return all_docs

    def fetch_elasticsearch_data(self, start_time, end_time):
        """기존 호환성을 위한 래퍼 메서드"""
        return self.fetch_elasticsearch_data_paginated(start_time, end_time)
    
    def save_minute_data(self, df, minute_timestamp):
        """분 단위 데이터를 MinIO에 저장 (KST 시간 기준)"""
        if df.empty:
            return None
        
        # 시간대 변환 적용
        df_processed = self.process_dataframe_timestamps(df)
        
        # KST 기준으로 파일명 생성
        kst_minute = self.convert_to_kst(minute_timestamp.isoformat() + 'Z')
        if kst_minute is None:
            kst_minute = minute_timestamp + timedelta(hours=9)  # fallback
        else:
            kst_minute = kst_minute.replace(tzinfo=None)  # timezone-naive로 변환
        
        minute_str = kst_minute.strftime("%Y%m%d_%H%M")
        object_name = f"realtime/rt_{minute_str}_kst.parquet"
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            try:
                df_processed.to_parquet(tmp_file.name, compression='snappy', index=False)
                
                self.minio_client.fput_object(
                    self.bucket_name,
                    object_name,
                    tmp_file.name,
                    content_type='application/octet-stream'
                )
                
                logger.info(f"Saved {len(df_processed)} records to {object_name} (KST)")
                return object_name
                
            finally:
                if os.path.exists(tmp_file.name):
                    os.unlink(tmp_file.name)
    
    def collect_current_minute_data(self):
        """이전 분의 데이터 수집 (데이터 누락 방지)"""
        # 현재 시간을 KST로 계산
        utc_now = datetime.utcnow()
        kst_now = self.convert_to_kst(utc_now.isoformat() + 'Z')
        
        if kst_now is None:
            kst_now = utc_now + timedelta(hours=9)  # fallback
        else:
            kst_now = kst_now.replace(tzinfo=None)  # timezone-naive로 변환
        
        # 안전을 위해 이전 분의 데이터를 수집 (완전히 끝난 분)
        target_minute = kst_now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        
        # UTC 기준으로 해당 분의 시작/끝 시간 계산
        utc_minute_start = target_minute - timedelta(hours=9)  # KST -> UTC
        utc_minute_end = utc_minute_start + timedelta(minutes=1)
        
        logger.info(f"Collecting data for KST minute: {target_minute} (1 minute ago)")
        logger.info(f"UTC range: {utc_minute_start.isoformat()}Z to {utc_minute_end.isoformat()}Z")
        
        # 해당 분의 데이터만 정확히 조회
        docs = self.fetch_elasticsearch_data(
            utc_minute_start.isoformat() + "Z",
            utc_minute_end.isoformat() + "Z"
        )
        
        if docs:
            df = pd.DataFrame(docs)
            if '@timestamp' in df.columns:
                # KST로 변환
                df_kst = self.process_dataframe_timestamps(df.copy())
                
                logger.info(f"Found {len(df_kst)} records for KST minute {target_minute}")
                
                # 해당 분 데이터 저장 (KST 기준)
                saved_file = self.save_minute_data(df_kst, target_minute)
                
                # 체크포인트 업데이트 (UTC 기준)
                checkpoint = self._load_checkpoint()
                checkpoint["last_timestamp"] = utc_minute_end.isoformat() + "Z"
                checkpoint["last_processed_minute"] = target_minute.isoformat()
                self._save_checkpoint(checkpoint)
                
                logger.info(f"Successfully processed {len(df_kst)} records for minute {target_minute}")
            else:
                logger.warning("No @timestamp field in documents")
        else:
            logger.info(f"No data found for KST minute {target_minute}")
    
    def merge_previous_minute_data(self):
        """이전 분의 데이터들을 병합 (KST 기준)"""
        utc_now = datetime.utcnow()
        kst_now = self.convert_to_kst(utc_now.isoformat() + 'Z')
        
        if kst_now is None:
            kst_now = utc_now + timedelta(hours=9)
        else:
            kst_now = kst_now.replace(tzinfo=None)
        
        # 이전 분 계산 (KST 기준, 안전을 위해 2분 전)
        target_minute = kst_now.replace(second=0, microsecond=0) - timedelta(minutes=2)
        target_date = target_minute.date()
        
        # KST 기준 파일명 패턴
        minute_str = target_minute.strftime("%Y%m%d_%H%M")
        target_files = []
        
        try:
            # 실시간 파일들 목록 조회 (KST 파일명 패턴)
            for obj in self.minio_client.list_objects(self.bucket_name, prefix="realtime/"):
                if f"rt_{minute_str}" in obj.object_name and "_kst.parquet" in obj.object_name:
                    target_files.append(obj.object_name)
        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            return
        
        if not target_files:
            logger.debug(f"No KST files to merge for minute {target_minute}")
            return
        
        logger.info(f"Merging {len(target_files)} KST files for minute {target_minute}")
        
        # 파일들 로드 및 병합
        dfs = []
        processed_files = []
        
        for file_name in target_files:
            try:
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    self.minio_client.fget_object(self.bucket_name, file_name, tmp_file.name)
                    df = pd.read_parquet(tmp_file.name)
                    
                    if not df.empty:
                        dfs.append(df)
                        processed_files.append(file_name)
                    
                    os.unlink(tmp_file.name)
                    
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {e}")
                continue
        
        if dfs:
            # 데이터 병합
            merged_df = pd.concat(dfs, ignore_index=True)
            merged_df = merged_df.drop_duplicates().sort_values('@timestamp')
            
            # 일일 파일명 (KST 날짜 기준)
            daily_file = f"/daily/daily_{target_date.strftime('%Y%m%d')}_kst.parquet"
            
            # 기존 일일 파일이 있다면 함께 병합
            try:
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                    self.minio_client.fget_object(self.bucket_name, daily_file, tmp_file.name)
                    existing_df = pd.read_parquet(tmp_file.name)
                    
                    # 기존 데이터와 합치기
                    final_df = pd.concat([existing_df, merged_df], ignore_index=True)
                    final_df = final_df.drop_duplicates().sort_values('@timestamp')
                    os.unlink(tmp_file.name)
                    
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    # 파일이 없으면 새로 생성
                    final_df = merged_df
                else:
                    logger.error(f"Error accessing daily file: {e}")
                    return
            
            # 병합된 데이터 저장
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                try:
                    final_df.to_parquet(tmp_file.name, compression='snappy', index=False)
                    
                    self.minio_client.fput_object(
                        self.bucket_name,
                        daily_file,
                        tmp_file.name,
                        content_type='application/octet-stream'
                    )
                    
                    logger.info(f"Merged {len(merged_df)} records into {daily_file} "
                               f"(total: {len(final_df)} records, KST timezone)")
                    
                    # 성공적으로 병합된 실시간 파일들 삭제
                    for file_name in processed_files:
                        try:
                            self.minio_client.remove_object(self.bucket_name, file_name)
                            logger.debug(f"Removed merged KST file: {file_name}")
                        except Exception as e:
                            logger.warning(f"Failed to remove {file_name}: {e}")
                    
                finally:
                    if os.path.exists(tmp_file.name):
                        os.unlink(tmp_file.name)
    
    def run_streaming_mode(self):
        """스트리밍 모드 실행"""
        logger.info("Starting streaming mode")
        
        # 스케줄 설정
        # 매분 5초에 데이터 수집 (여유시간 확보)
        schedule.every().minute.at(":05").do(
            lambda: self.executor.submit(self.collect_current_minute_data)
        )
        
        # 매분 35초에 이전 데이터 병합
        schedule.every().minute.at(":35").do(
            lambda: self.executor.submit(self.merge_previous_minute_data)
        )
        
        logger.info("Scheduled tasks:")
        logger.info("  - Data collection: every minute at :05")
        logger.info("  - Data merging: every minute at :35")
        
        # 메인 루프
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                self.executor.shutdown(wait=True)
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)

def process_batch_mode(args, pipeline):
    """배치 모드: 2025년 5월 데이터를 1주일씩 묶어서 처리 (체크포인트 및 실패 추적 지원)"""
    logger.info(f"Starting batch processing for {args.target_month}")
    
    year, month = map(int, args.target_month.split('-'))
    start_date = datetime(year, month, 1)
    
    # 다음 달 첫날을 구해서 이번 달 마지막날 계산
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    # 배치 체크포인트 파일 (도커 볼륨 고려)
    batch_checkpoint_file = os.path.join(pipeline.checkpoint_dir, f"batch_checkpoint_{args.target_month}.json")
    
    # 체크포인트 로드
    if os.path.exists(batch_checkpoint_file):
        with open(batch_checkpoint_file, 'r') as f:
            checkpoint = json.load(f)
        completed_weeks = set(checkpoint.get("completed_weeks", []))
        failed_weeks = checkpoint.get("failed_weeks", {})
        partial_weeks = checkpoint.get("partial_weeks", {})
        
        logger.info(f"Resuming from checkpoint:")
        logger.info(f"  - Completed weeks: {sorted(completed_weeks)}")
        logger.info(f"  - Failed weeks: {list(failed_weeks.keys())}")
        logger.info(f"  - Partial weeks: {list(partial_weeks.keys())}")
    else:
        completed_weeks = set()
        failed_weeks = {}
        partial_weeks = {}
        checkpoint = {
            "completed_weeks": [],
            "failed_weeks": {},
            "partial_weeks": {}
        }
    
    current_date = start_date
    week_num = 1
    total_weeks_expected = ((end_date - start_date).days + 6) // 7  # 전체 주차 수 계산
    
    logger.info(f"Processing period: {start_date.date()} to {end_date.date()}")
    logger.info(f"Expected total weeks: {total_weeks_expected}")
    
    while current_date < end_date:
        # 1주일 배치 (7일)
        week_end = min(current_date + timedelta(days=7), end_date)
        
        # 이미 완료된 주차는 건너뛰기
        if week_num in completed_weeks:
            logger.info(f"Week {week_num} already completed, skipping...")
            current_date = week_end
            week_num += 1
            continue
        
        # 실패한 주차 정보 표시
        if week_num in failed_weeks:
            failed_info = failed_weeks[week_num]
            logger.warning(f"Week {week_num} previously failed {failed_info['attempts']} times. "
                          f"Last error: {failed_info.get('last_error', 'Unknown')}")
        
        # 부분 완료된 주차 정보 표시
        if week_num in partial_weeks:
            partial_info = partial_weeks[week_num]
            logger.info(f"Week {week_num} has partial data: {partial_info['current_count']}/{partial_info.get('expected_count', '?')} records")
        
        logger.info(f"Processing week {week_num}: {current_date.date()} to {(week_end - timedelta(days=1)).date()}")
        
        # 파일명 생성 (KST 기준)
        start_str = current_date.strftime('%Y%m%d')
        end_str = (week_end - timedelta(days=1)).strftime('%Y%m%d')
        object_name = f"{args.target_month}/week_{week_num:02d}_{start_str}_{end_str}_kst.parquet"
        
        # 기존 파일이 있으면 현재 상황 확인
        existing_record_count = 0
        if pipeline.object_exists(object_name):
            try:
                existing_df = pipeline.load_parquet_safe(object_name)
                existing_record_count = len(existing_df)
                logger.info(f"Week {week_num} file already exists with {existing_record_count} records")
            except Exception as e:
                logger.warning(f"Could not load existing file: {e}, will create new file")
        
        # 먼저 전체 데이터 개수 확인 (한 번만 조회)
        expected_count = 0
        try:
            logger.info("Checking expected data count...")
            count_query = {
                "size": 0,  # 실제 데이터는 가져오지 않고 카운트만
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": current_date.isoformat() + "Z",
                                        "lt": week_end.isoformat() + "Z"
                                    }
                                }
                            },
                            {
                                "term": {"rsctypeId": "FTH"}
                            }
                        ]
                    }
                }
            }
            
            url = f"{pipeline.es_url.rstrip('/')}/{pipeline.index_pattern}/_search"
            resp = requests.post(url, json=count_query, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            total_hits = data.get("hits", {}).get("total", {})
            if isinstance(total_hits, dict):
                expected_count = total_hits.get("value", 0)
            else:
                expected_count = total_hits
                
            logger.info(f"Expected data count for week {week_num}: {expected_count}")
            
        except Exception as e:
            logger.warning(f"Could not get expected count: {e}")
        
        # 여러 번 시도하여 모든 데이터 수집
        max_attempts = 3
        all_docs = []
        attempt_successful = False
        last_error = None
        
        for attempt in range(max_attempts):
            logger.info(f"Attempt {attempt + 1}/{max_attempts} - Fetching data from {current_date.isoformat()}Z to {week_end.isoformat()}Z")
            
            try:
                docs = pipeline.fetch_elasticsearch_data_paginated(
                    current_date.isoformat() + "Z",
                    week_end.isoformat() + "Z"
                )
                
                if docs:
                    all_docs.extend(docs)
                    logger.info(f"Attempt {attempt + 1}: Fetched {len(docs)} documents (total so far: {len(all_docs)})")
                    
                    # 기대하는 데이터 개수와 비교
                    if expected_count > 0:
                        fetch_ratio = len(all_docs) / expected_count
                        logger.info(f"Data fetch progress: {len(all_docs)}/{expected_count} ({fetch_ratio:.1%})")
                        
                        # 80% 이상 가져왔으면 성공으로 간주
                        if fetch_ratio >= 0.8:
                            attempt_successful = True
                            break
                    else:
                        # 예상 개수를 모르는 경우, 데이터가 있으면 성공으로 간주
                        if len(all_docs) > 0:
                            attempt_successful = True
                            break
                else:
                    logger.warning(f"Attempt {attempt + 1}: No data returned")
                
            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt + 1} failed with error: {e}")
            
            if attempt < max_attempts - 1:
                logger.info("Waiting 10 seconds before next attempt...")
                time.sleep(10)  # 30초 -> 10초로 단축
        
        # 중복 제거
        if all_docs:
            # DataFrame으로 변환하여 중복 제거
            temp_df = pd.DataFrame(all_docs)
            all_docs = temp_df.drop_duplicates().to_dict('records')
            logger.info(f"After deduplication: {len(all_docs)} unique documents for week {week_num}")
        
        # 결과 처리
        if not attempt_successful and len(all_docs) == 0:
            # 완전 실패
            logger.error(f"Week {week_num} completely failed after {max_attempts} attempts")
            failed_weeks[week_num] = {
                "attempts": max_attempts,
                "last_error": last_error or "No data retrieved",
                "expected_count": expected_count,
                "timestamp": datetime.now().isoformat()
            }
            
            # 체크포인트 업데이트
            checkpoint["failed_weeks"] = failed_weeks
            with open(batch_checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
            
            logger.warning(f"Week {week_num} marked as failed and will be retried later")
            
        elif not attempt_successful and len(all_docs) > 0:
            # 부분 성공
            logger.warning(f"Week {week_num} partially successful: got {len(all_docs)}/{expected_count} records")
            partial_weeks[week_num] = {
                "current_count": len(all_docs) + existing_record_count,
                "expected_count": expected_count,
                "last_attempt": datetime.now().isoformat()
            }
            
            # 체크포인트 업데이트
            checkpoint["partial_weeks"] = partial_weeks
            with open(batch_checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
            
            # 부분 데이터라도 저장
            process_and_save_week_data(pipeline, all_docs, week_num, current_date, week_end, 
                                     object_name, existing_record_count, args.target_month)
            
        else:
            # 성공
            logger.info(f"Week {week_num} successfully completed: {len(all_docs)} documents")
            
            # 성공한 경우 실패/부분 완료 기록에서 제거
            if week_num in failed_weeks:
                del failed_weeks[week_num]
                checkpoint["failed_weeks"] = failed_weeks
            if week_num in partial_weeks:
                del partial_weeks[week_num]
                checkpoint["partial_weeks"] = partial_weeks
            
            # 데이터 처리 및 저장
            if process_and_save_week_data(pipeline, all_docs, week_num, current_date, week_end, 
                                        object_name, existing_record_count, args.target_month):
                # 성공한 주차를 체크포인트에 추가
                completed_weeks.add(week_num)
                checkpoint["completed_weeks"] = list(completed_weeks)
                with open(batch_checkpoint_file, 'w') as f:
                    json.dump(checkpoint, f)
                
                logger.info(f"Week {week_num} completed and checkpointed")
        
        current_date = week_end
        week_num += 1
    
    # 배치 처리 완료 후 요약
    logger.info("="*60)
    logger.info("BATCH PROCESSING SUMMARY")
    logger.info("="*60)
    logger.info(f"Completed weeks: {sorted(completed_weeks)}")
    
    if failed_weeks:
        logger.warning(f"Failed weeks: {list(failed_weeks.keys())}")
        for week, info in failed_weeks.items():
            logger.warning(f"  Week {week}: {info['attempts']} attempts, last error: {info['last_error']}")
    
    if partial_weeks:
        logger.warning(f"Partial weeks: {list(partial_weeks.keys())}")
        for week, info in partial_weeks.items():
            logger.warning(f"  Week {week}: {info['current_count']}/{info['expected_count']} records")
    
    # 재시도 제안
    if failed_weeks or partial_weeks:
        logger.info("="*60)
        logger.info("RETRY SUGGESTIONS")
        logger.info("="*60)
        logger.info("To retry failed/partial weeks, run:")
        logger.info(f"docker-compose --profile batch up fms-batch")
        logger.info("The system will automatically retry failed and partial weeks.")
    else:
        # 모든 주차 완료 시 체크포인트 파일 삭제
        try:
            os.remove(batch_checkpoint_file)
            logger.info("All weeks completed successfully. Checkpoint file removed.")
        except:
            pass

def process_and_save_week_data(pipeline, all_docs, week_num, current_date, week_end, 
                              object_name, existing_record_count, target_month):
    """주차 데이터 처리 및 저장 (공통 함수)"""
    if not all_docs:
        logger.info(f"No new data to process for week {week_num}")
        return False
    
    try:
        # DataFrame 생성 및 시간대 변환
        df = pd.DataFrame(all_docs)
        logger.info(f"Created DataFrame with {len(df)} rows and columns: {list(df.columns)}")
        
        new_record_count = len(df)  # 새로 가져온 데이터 개수 저장
        
        if '@timestamp' in df.columns:
            # 시간 변환 전 샘플 확인
            logger.info(f"Sample original timestamps: {df['@timestamp'].head(3).tolist()}")
            
            df = pipeline.process_dataframe_timestamps(df)
            logger.info(f"After timezone conversion: {len(df)} records")
            
            # 시간 변환 후 샘플 확인  
            logger.info(f"Sample KST timestamps: {df['@timestamp'].head(3).tolist() if len(df) > 0 else 'No data'}")
            if '@timestamp_utc' in df.columns:
                logger.info(f"Sample UTC timestamps: {df['@timestamp_utc'].head(3).tolist()}")
            logger.info(f"Full time range: {df['@timestamp'].min()} to {df['@timestamp'].max()} (KST)")
            
            # 해당 주차에 속하는 데이터만 필터링 (KST 기준)
            week_start_kst = pipeline.utc_to_kst_offset(current_date)
            week_end_kst = pipeline.utc_to_kst_offset(week_end)
            
            logger.info(f"Week filter range: {week_start_kst} to {week_end_kst} (KST)")
            
            # pandas datetime 변환 및 필터링 - 문자열을 다시 datetime으로 변환
            df['@timestamp_dt'] = pd.to_datetime(df['@timestamp'])
            week_start_dt = pd.to_datetime(week_start_kst)
            week_end_dt = pd.to_datetime(week_end_kst)
            
            week_df = df[
                (df['@timestamp_dt'] >= week_start_dt) & 
                (df['@timestamp_dt'] < week_end_dt)
            ].copy()
            
            # 임시 컬럼 제거
            week_df = week_df.drop(columns=['@timestamp_dt'])
            
            logger.info(f"After week filtering: {len(week_df)} records for week {week_num}")
            if len(week_df) > 0:
                logger.info(f"Week time range: {week_df['@timestamp'].min()} to {week_df['@timestamp'].max()} (KST)")
            
            if len(week_df) == 0:
                logger.warning(f"No data in target week range after filtering")
                return False
            
            final_df = week_df
        else:
            logger.warning("No @timestamp field in documents")
            final_df = df
        
        # 기존 파일과 병합 처리
        if existing_record_count > 0:
            logger.info(f"Merging with existing {existing_record_count} records")
            try:
                existing_df = pipeline.load_parquet_safe(object_name)
                # 기존 데이터와 새 데이터 합치기
                combined_df = pd.concat([existing_df, final_df], ignore_index=True)
                # 중복 제거 (전체 행 기준)
                final_df = combined_df.drop_duplicates().sort_values('@timestamp')
                
                logger.info(f"Merged: {existing_record_count} existing + {new_record_count} new = {len(final_df)} final records")
            except Exception as e:
                logger.error(f"Failed to merge with existing file: {e}, using new data only")
        
        # MinIO에 저장
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            try:
                final_df.to_parquet(tmp_file.name, compression='snappy', index=False)
                
                pipeline.minio_client.fput_object(
                    pipeline.bucket_name,
                    object_name,
                    tmp_file.name,
                    content_type='application/octet-stream'
                )
                
                logger.info(f"Saved {len(final_df)} records to {object_name}")
                return True
                
            finally:
                if os.path.exists(tmp_file.name):
                    os.unlink(tmp_file.name)
    
    except Exception as e:
        logger.error(f"Failed to process and save week {week_num} data: {e}")
        return False

def main():
    args = parse_args()
    
    # 파이프라인 초기화
    pipeline = StreamingPipeline(args)
    
    if args.mode == "streaming":
        pipeline.run_streaming_mode()
    elif args.mode == "batch":
        process_batch_mode(args, pipeline)
    else:
        logger.error(f"Unknown mode: {args.mode}")

if __name__ == "__main__":
    main()