import tarfile
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os
from typing import Optional

def tar_csv_to_parquet(
    tar_path: str,
    output_dir: str,
    compression: str = 'snappy',
    batch_size: int = 1048576,  # 1M rows per batch
    extract_to_temp: bool = False,
    temp_dir: Optional[str] = None
):
    """
    대용량 tar 파일 안의 CSV들을 Parquet로 변환
    
    Args:
        tar_path: tar 파일 경로
        output_dir: parquet 파일들을 저장할 디렉토리
        compression: 압축 방식 ('snappy', 'gzip', 'brotli', 'zstd', 'none')
        batch_size: 한 번에 읽을 행 수
        extract_to_temp: 임시 디렉토리에 먼저 압축 해제할지 여부
        temp_dir: 임시 디렉토리 경로 (extract_to_temp=True일 때)
    """
    # 출력 디렉토리 생성
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"TAR 파일 열기: {tar_path}")
    print(f"출력 디렉토리: {output_dir}")
    print(f"압축 방식: {compression}\n")
    
    if extract_to_temp:
        # 방법 1: 임시로 압축 해제 후 처리 (매우 큰 파일에 유리)
        temp_extract_dir = temp_dir or "temp_extracted"
        os.makedirs(temp_extract_dir, exist_ok=True)
        
        with tarfile.open(tar_path, 'r:*') as tar:
            tar.extractall(temp_extract_dir)
        
        # 압축 해제된 CSV 파일들 처리
        for csv_file in Path(temp_extract_dir).rglob('*.csv'):
            print(f"처리 중: {csv_file.name}")
            convert_csv_to_parquet(
                str(csv_file), 
                output_dir, 
                compression,
                batch_size
            )
        
        # 임시 파일 정리
        import shutil
        shutil.rmtree(temp_extract_dir)
        
    else:
        # 방법 2: 스트리밍 방식으로 직접 처리
        with tarfile.open(tar_path, 'r:*') as tar:
            for member in tar.getmembers():
                if member.name.endswith('.csv') and member.isfile():
                    print(f"처리 중: {member.name} (크기: {member.size / 1024 / 1024:.2f} MB)")
                    
                    csv_file = tar.extractfile(member)
                    
                    if csv_file:
                        base_name = Path(member.name).stem
                        parquet_path = os.path.join(output_dir, f"{base_name}.parquet")
                        
                        try:
                            # PyArrow로 CSV 스트리밍 읽기 및 Parquet 변환
                            convert_stream_to_parquet(
                                csv_file,
                                parquet_path,
                                compression,
                                batch_size
                            )
                            
                        except Exception as e:
                            print(f"오류 발생: {member.name} - {str(e)}\n")
                            continue

def convert_csv_to_parquet(
    csv_path: str,
    output_dir: str,
    compression: str,
    batch_size: int
):
    """파일 경로에서 CSV를 Parquet로 변환"""
    base_name = Path(csv_path).stem
    parquet_path = os.path.join(output_dir, f"{base_name}.parquet")
    
    # PyArrow의 스트리밍 reader 사용
    read_options = csv.ReadOptions(
        block_size=batch_size
    )
    parse_options = csv.ParseOptions(
        delimiter=','
    )
    convert_options = csv.ConvertOptions(
        strings_can_be_null=True
    )
    
    # CSV를 배치로 읽기
    table = csv.read_csv(
        csv_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options
    )
    
    # Parquet로 저장
    pq.write_table(
        table,
        parquet_path,
        compression=compression,
        row_group_size=batch_size,
        use_dictionary=True,
        write_statistics=True
    )
    
    print(f"✓ 저장 완료: {parquet_path}")
    print(f"  행 수: {table.num_rows:,}, 열 수: {table.num_columns}")
    print(f"  파일 크기: {os.path.getsize(parquet_path) / 1024 / 1024:.2f} MB\n")

def convert_stream_to_parquet(
    csv_stream,
    parquet_path: str,
    compression: str,
    batch_size: int
):
    """스트림에서 CSV를 Parquet로 변환"""
    
    # CSV 스트림을 PyArrow 테이블로 읽기
    read_options = csv.ReadOptions(
        block_size=batch_size
    )
    parse_options = csv.ParseOptions(
        delimiter=','
    )
    convert_options = csv.ConvertOptions(
        strings_can_be_null=True
    )
    
    table = csv.read_csv(
        csv_stream,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options
    )
    
    # Parquet로 저장
    pq.write_table(
        table,
        parquet_path,
        compression=compression,
        row_group_size=batch_size,
        use_dictionary=True,
        write_statistics=True
    )
    
    print(f"✓ 저장 완료: {parquet_path}")
    print(f"  행 수: {table.num_rows:,}, 열 수: {table.num_columns}")
    print(f"  파일 크기: {os.path.getsize(parquet_path) / 1024 / 1024:.2f} MB\n")


# 사용 예시
if __name__ == "__main__":
    # 예시 1: 스트리밍 방식 (메모리 효율적)
    tar_csv_to_parquet(
        tar_path="data/awos.tar",
        output_dir="data/output2",
        compression='snappy',  # 'snappy', 'gzip', 'zstd' 등
        batch_size=1048576,  # 100만 행씩 처리
        extract_to_temp=False
    )
    
    # 예시 2: 임시 압축 해제 방식 (매우 큰 파일)
    # tar_csv_to_parquet(
    #     tar_path="huge_data.tar.gz",
    #     output_dir="parquet_files",
    #     compression='zstd',  # 더 높은 압축률
    #     batch_size=1048576,
    #     extract_to_temp=True,
    #     temp_dir="temp"
    # )