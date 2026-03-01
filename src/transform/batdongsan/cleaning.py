import pandas as pd
import numpy as np

def clean_numpy_pandas(value):
    """Chuyển đổi kiểu dữ liệu về dạng chuẩn."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
        return str(value) if not pd.isnull(value) else None
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer, np.int64, np.int32)):
        return int(value)
    if isinstance(value, (np.floating, np.float64, np.float32)):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value

def format_payload(row):
    """Chuẩn bị payload sạch và xử lý trường location cho Qdrant."""
    # Các trường loại ra khỏi payload chính để xử lý riêng
    exclude = ['lat', 'lon']
    payload = {k: clean_numpy_pandas(v) for k, v in row.items() if k not in exclude}

    # Xử lý chuẩn bị sẵn tọa độ cho qdrant indexing
    lat = clean_numpy_pandas(row.get('lat'))
    lon = clean_numpy_pandas(row.get('lon'))
    if lat and lon and -90 <= lat <= 90 and -180 <= lon <= 180:
        payload['location'] = {"lat": lat, "lon": lon}
    else:
        payload['location'] = None
        
    return payload