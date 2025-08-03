#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Parquet 파일 검사 스크립트
- 파일 기본 정보 확인
- 첫 번째/마지막 레코드 출력
- 시간 범위 확인
"""

import pandas as pd
import argparse
from minio import Minio
import tempfile
import os

def inspect_parquet_file(minio_client, bucket_name, object_name):
    """Parquet 파일 내용 검사"""
    print(f"\n{'='*60}")
    print(f"파일: {object_name}")
    print(f"{'='*60}")
    
    try:
        # MinIO에서 파일 다운로드
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            minio_client.fget_object(bucket_name, object_name, tmp_file.name)
            
            # Parquet 파일 읽기
            df = pd.read_parquet(tmp_file.name)
            
            # 기본 정보
            print(f"총 레코드 수: {len(df):,}")
            print(f"컬럼 수: {len(df.columns)}")
            print(f"컬럼 목록: {list(df.columns)}")
            
            if len(df) > 0:
                # 첫 번째 레코드
                print(f"\n📍 첫 번째 레코드:")
                first_record = df.iloc[0]
                for col, val in first_record.items():
                    print(f"  {col}: {val}")
                
                # 마지막 레코드  
                print(f"\n📍 마지막 레코드:")
                last_record = df.iloc[-1]
                for col, val in last_record.items():
                    print(f"  {col}: {val}")
                
                # 시간 범위 분석 (@timestamp가 있는 경우)
                if '@timestamp' in df.columns:
                    print(f"\n⏰ 시간 범위:")
                    print(f"  시작: {df['@timestamp'].min()}")
                    print(f"  종료: {df['@timestamp'].max()}")
                    
                    # 시간 순으로 정렬되어 있는지 확인
                    df_sorted = df.sort_values('@timestamp')
                    is_sorted = df['@timestamp'].equals(df_sorted['@timestamp'])
                    print(f"  정렬 상태: {'✅ 정렬됨' if is_sorted else '❌ 정렬 안됨'}")
                
                # 중복 확인
                duplicates = df.duplicated().sum()
                print(f"\n🔍 데이터 품질:")
                print(f"  중복 레코드: {duplicates:,}개")
                print(f"  고유 레코드: {len(df) - duplicates:,}개")
                
                # 각 컬럼별 샘플 값들
                print(f"\n📊 컬럼별 샘플 값:")
                for col in df.columns:
                    if col != '@timestamp':  # 타임스탬프는 이미 위에서 표시
                        unique_vals = df[col].unique()
                        if len(unique_vals) <= 5:
                            print(f"  {col}: {list(unique_vals)}")
                        else:
                            print(f"  {col}: {list(unique_vals[:3])} ... (총 {len(unique_vals)}개 고유값)")
            
            # 임시 파일 정리
            os.unlink(tmp_file.name)
            
    except Exception as e:
        print(f"❌ 파일 검사 실패: {e}")

def main():
    parser = argparse.ArgumentParser(description="Parquet 파일 검사")
    parser.add_argument("--minio-endpoint", 
        default=os.environ.get("MINIO_ENDPOINT", "localhost:9000"))
    parser.add_argument("--minio-access-key", 
        default=os.environ.get("MINIO_ACCESS_KEY", ""))
    parser.add_argument("--minio-secret-key", 
        default=os.environ.get("MINIO_SECRET_KEY", ""))
    parser.add_argument("--bucket-name", 
        default=os.environ.get("BUCKET_NAME", "fms-data"))
    parser.add_argument("--files", nargs='+', 
        default=[
            "fth-data/2025-05/week_04_20250522_20250528_kst.parquet",
            "fth-data/2025-05/week_05_20250529_20250531_kst.parquet"
        ],
        help="검사할 파일 목록")
    
    args = parser.parse_args()
    
    # MinIO 클라이언트 생성
    minio_client = Minio(
        args.minio_endpoint,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=False
    )
    
    print("🔍 Parquet 파일 검사 시작")
    print(f"MinIO: {args.minio_endpoint}")
    print(f"Bucket: {args.bucket_name}")
    
    # 각 파일 검사
    for file_path in args.files:
        inspect_parquet_file(minio_client, args.bucket_name, file_path)
    
    print(f"\n✅ 검사 완료!")

if __name__ == "__main__":
    main()