import pandas as pd
import s3fs
from datetime import datetime
from deltalake import DeltaTable
from src.config.settings import PANDAS_STORAGE_OPTIONS, DELTA_STORAGE_OPTIONS, MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY

def get_s3_fs():
    """Khởi tạo filesystem để quét danh sách file"""
    return s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={'endpoint_url': MINIO_ENDPOINT}
    )

def list_raw_files(site, bucket='data-lake'):
    """Lấy danh sách các file json.gz của một site nhất định"""
    fs = get_s3_fs()
    now = datetime.utcnow()
    folder_path = f"s3://{bucket}/{site}/raw/{now.year}-{now.month:02d}-{now.day:02d}/"
    print(folder_path)
    files = fs.glob(folder_path + "*.json.gz")
    print(files)
    return ["s3://" + f for f in files]

def read_raw_json_gz(file_path):
    """Đọc một file json.gz cụ thể từ MinIO"""
    return pd.read_json(
        file_path, 
        lines=True, 
        compression='gzip',
        storage_options=PANDAS_STORAGE_OPTIONS 
    )

def read_delta_table(site, layer, bucket='data-lake'):
    """Đọc bảng Delta (Parsed hoặc Processed)"""
    now = datetime.utcnow()
    uri = f"s3://{bucket}/{site}/{layer}/{now.year}-{now.month:02d}-{now.day:02d}"
    dt = DeltaTable(uri, storage_options=DELTA_STORAGE_OPTIONS)
    return dt.to_pandas()