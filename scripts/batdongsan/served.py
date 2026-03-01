import uuid
from tqdm import tqdm
from deltalake import DeltaTable
from src.config.settings import DELTA_STORAGE_OPTIONS
from src.transform.batdongsan.cleaning import format_payload
from src.load.loaders import load_to_qdrant
import datetime as dt
s3_path = f"s3://data-lake/batdongsan.com.vn/processed/{dt.datetime.now().strftime('%Y-%m-%d')}"

def serve():
    # 1. Đọc dữ liệu từ Delta Lake
    print("⏳ Đang tải dữ liệu từ Delta Lake...")
    print(s3_path)
    dt = DeltaTable(s3_path, storage_options=DELTA_STORAGE_OPTIONS)
    df = dt.to_pandas()
    records = df.to_dict(orient='records')

    # 2. Chuẩn bị dữ liệu (Bỏ hoàn toàn Embedding)
    all_points = []
    print(f"📦 Đang đóng gói {len(records)} bản ghi vào Payload...")
    
    for row in tqdm(records):
        # Tạo ID và format lại payload, không cần tạo vector/embedding
        all_points.append({
            "id": str(uuid.uuid4()),
            "payload": format_payload(row)
        })

    # 3. Nạp thẳng vào Qdrant
    if all_points:
        # Hàm load_to_qdrant này phải là phiên bản "không vector" đã sửa ở trên
        load_to_qdrant(all_points)
