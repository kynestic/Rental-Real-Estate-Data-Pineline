import io
import gzip
import json
import time
import pyarrow as pa
from datetime import datetime
from deltalake import write_deltalake
from minio import Minio

from qdrant_client import QdrantClient
from qdrant_client.http import models
from src.config.settings import (
    MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, 
    PANDAS_STORAGE_OPTIONS, DELTA_STORAGE_OPTIONS,
    QDRANT_URL, QDRANT_COLLECTION
)

def _get_minio_client():
    host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(
        host,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False # Set True nếu dùng https
    )

def load_raw(data_list, site, bucket='data-lake'):
    """Nén dữ liệu và đẩy lên MinIO"""
    client = _get_minio_client()
    mem_file = io.BytesIO()

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Bucket '{bucket}' không tồn tại. Đã tự động tạo!")
    except Exception as e:
        print(f"Cảnh báo khi kiểm tra bucket: {e}")
    
    with gzip.GzipFile(fileobj=mem_file, mode='wb') as gz:
        for item in data_list:
            line = json.dumps(item, ensure_ascii=False) + "\n"
            gz.write(line.encode('utf-8'))
    
    mem_file.seek(0)
    now = datetime.utcnow()
    file_path = f"{site}/raw/{now.year}-{now.month:02d}-{now.day:02d}/batch_{int(time.time())}.json.gz"
    print(file_path)
    client.put_object(
        bucket_name=bucket,
        object_name=file_path,
        data=mem_file,
        length=mem_file.getbuffer().nbytes,
        content_type="application/json"
    )
    return file_path

def load_to_delta(df, site, layer, bucket='data-lake'):
    """
    layer: 'parsed' hoặc 'processed'
    """
    now = datetime.utcnow()
    uri = f"s3://{bucket}/{site}/{layer}/{now.year}-{now.month:02d}-{now.day:02d}"
    table = pa.Table.from_pandas(df)
    write_deltalake(
        uri,
        table,
        mode="append",
        storage_options=DELTA_STORAGE_OPTIONS,
        schema_mode="merge"
    )
    return uri

def load_to_qdrant(points_data, collection_name=QDRANT_COLLECTION):
    # 1. Kết nối tới Qdrant
    client = QdrantClient(url=QDRANT_URL, timeout=120)
    
    if not client.collection_exists(collection_name=collection_name):
        print(f"📁 Collection '{collection_name}' chưa tồn tại. Đang tạo mới...")
        
        # 2. Tạo nếu chưa có
        client.create_collection(
            collection_name=collection_name,
            vectors_config={}, # Cấu hình không dùng vector
            optimizers_config=models.OptimizersConfigDiff(indexing_threshold=40000)
        )
        print(f"✅ Đã tạo thành công collection: {collection_name}")
    else:
        print(f"ℹ️ Collection '{collection_name}' đã tồn tại. Bỏ qua bước tạo mới.")
        
        client.update_collection(
            collection_name=collection_name,
            optimizer_config=models.OptimizersConfigDiff(indexing_threshold=40000)
        )

    ids = [p['id'] for p in points_data]
    payloads = [p['payload'] for p in points_data]
        
    empty_vectors = [{}] * len(points_data) 

    print(f"🚀 Đang nạp {len(points_data)} bản ghi bằng upload_collection...")

    # 3. Thực hiện Bulk Upload
    client.upload_collection(
        collection_name=collection_name,
        vectors=empty_vectors, 
        payload=payloads,
        ids=ids,
        batch_size=1000,  
        parallel=1,    
        wait=True  
    )

    client.update_collection(
        collection_name=collection_name,
        optimizer_config=models.OptimizersConfigDiff(indexing_threshold=20000)
    )

    client.create_payload_index(
        collection_name=collection_name,
        field_name="location",
        field_schema=models.PayloadSchemaType.GEO,
        wait=True
    )
    print("✅ Hoàn tất nạp dữ liệu siêu tốc!")