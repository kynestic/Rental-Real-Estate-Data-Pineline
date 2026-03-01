import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

# Cấu hình dùng chung cho s3fs/Pandas
PANDAS_STORAGE_OPTIONS = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

# Cấu hình dùng chung cho Delta Lake
DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true"
}

# Cấu hình để sử dụng Qdrant
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")

