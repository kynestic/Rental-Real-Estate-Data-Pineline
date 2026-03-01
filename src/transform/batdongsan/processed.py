import re
import pandas as pd
from urllib.parse import urlparse, parse_qs
# import json
# import requests
# import unicodedata
# from thefuzz import process, fuzz


# ==========================================
# 1. CLASS XỬ LÝ VIỆC TRÍCH XUẤT ĐỊA CHỈ THÀNH ĐƯỜNG/PHỐ, XÃ/PHƯỜNG, HUYỆN/QUẬN,...
# ==========================================
# class AddressParser:
#     def __init__(self):
#         print("⏳ Đang tải dữ liệu hành chính VN...")
#         # Load Master Data
#         url = "https://raw.githubusercontent.com/madnh/hanhchinhvn/master/dist/tree.json"
#         try:
#             self.tree_data = requests.get(url).json()
#             print("✅ Đã tải xong dữ liệu hành chính!")
#         except:
#             self.tree_data = {}
#             print("❌ Lỗi kết nối đến dữ liệu hành chính!")

#         # Indexing dữ liệu
#         self.prov_lookup = {}
#         for code, p_data in self.tree_data.items():
#             names = [p_data['name'], p_data['slug'], p_data['name_with_type']]
#             if p_data['slug'] == 'ho-chi-minh': names.append('hcm')
#             if p_data['slug'] == 'ha-noi': names.append('hn')
            
#             for n in names:
#                 self.prov_lookup[self._clean_key(n)] = p_data

#     def _clean_key(self, text):
#         """Chuẩn hóa chuỗi để làm key lookup"""
#         if not text: return ""
#         text = unicodedata.normalize('NFC', text).lower()
#         text = re.sub(r'\s+', ' ', text).strip()
#         return text

#     def parse(self, raw_address):
#         if not raw_address or not isinstance(raw_address, str):
#             return None, None, None, None

#         parts = [p.strip() for p in re.split(r',', raw_address) if p.strip()]
#         search_parts = parts[-4:] if len(parts) >= 4 else parts
#         street_parts = parts[:-4] if len(parts) >= 4 else []

#         found_city = None
#         found_dist = None
#         found_ward = None

#         current_scope_districts = {} 
#         current_scope_wards = {}
#         indices_to_remove = [] 

#         # --- A. TÌM TỈNH/THÀNH PHỐ ---
#         if search_parts:
#             last_part = search_parts[-1]
#             best_prov, score = process.extractOne(self._clean_key(last_part), list(self.prov_lookup.keys()), scorer=fuzz.ratio)
            
#             if score >= 85:
#                 prov_data = self.prov_lookup[best_prov]
#                 found_city = prov_data['name_with_type']
#                 indices_to_remove.append(len(search_parts) - 1)
                
#                 # Tạo lookup cho Quận
#                 for d_code, d_val in prov_data['quan-huyen'].items():
#                     current_scope_districts[self._clean_key(d_val['name'])] = d_val
#                     current_scope_districts[self._clean_key(d_val['name_with_type'])] = d_val
#                     if 'quận' in d_val['name_with_type'].lower():
#                         short = d_val['name_with_type'].lower().replace('quận', 'q').replace(' ', '')
#                         current_scope_districts[short] = d_val

#         # --- B. TÌM QUẬN/HUYỆN ---
#         if current_scope_districts:
#             for i in range(len(search_parts) - 1, -1, -1):
#                 if i in indices_to_remove: continue
#                 part = search_parts[i]
#                 best_dist, score = process.extractOne(self._clean_key(part), list(current_scope_districts.keys()), scorer=fuzz.ratio)
                
#                 if score >= 85:
#                     dist_data = current_scope_districts[best_dist]
#                     found_dist = dist_data['name_with_type']
#                     indices_to_remove.append(i)
                    
#                     # Tạo lookup cho Phường
#                     for w_code, w_val in dist_data['xa-phuong'].items():
#                         current_scope_wards[self._clean_key(w_val['name'])] = w_val
#                         current_scope_wards[self._clean_key(w_val['name_with_type'])] = w_val
#                         if 'phường' in w_val['name_with_type'].lower():
#                             short = w_val['name_with_type'].lower().replace('phường', 'p').replace(' ', '')
#                             current_scope_wards[short] = w_val
#                     break 

#         # --- C. TÌM PHƯỜNG/XÃ ---
#         if current_scope_wards:
#             for i in range(len(search_parts) - 1, -1, -1):
#                 if i in indices_to_remove: continue
#                 part = search_parts[i]
#                 best_ward, score = process.extractOne(self._clean_key(part), list(current_scope_wards.keys()), scorer=fuzz.ratio)
                
#                 if score >= 85:
#                     found_ward = current_scope_wards[best_ward]['name_with_type']
#                     indices_to_remove.append(i)
#                     break

#         # --- D. TÌM ĐƯỜNG ---
#         remaining_search_parts = [search_parts[i] for i in range(len(search_parts)) if i not in indices_to_remove]
#         final_street_parts = street_parts + remaining_search_parts
#         street = ", ".join(final_street_parts).strip()
#         street = re.sub(r'^,+,?', '', street).strip()

#         return street, found_ward, found_dist, found_city


# ==========================================
# 2. HÀM XỬ LÝ VIỆC CHUYỂN XÂU CHỨA SỐ VỀ SỐ
# ==========================================
def parse_number_raw(text, is_area=False):
    """Trích xuất giá trị số thô."""

    # Không phải xâu thì bỏ qua
    if not isinstance(text, str) or not text:
        return None
    
    # Nếu không đúng định dạng chỉ bao gồm số và dấu , . thì thôi bỏ qua
    match = re.search(r"([\d.,]+)", text)
    if not match:
        return None

    # Lấy nhóm đầu tiên khớp định dạng
    raw_num = match.group(1)

    # Diện tích thì sẽ phải xử lý khác một chút
    if is_area:
        clean_num = raw_num.replace('.', '').replace(',', '.') # 1.000 -> 1000 Diện tích thì chỉ phải xóa dấu . thôi
    else:
        clean_num = raw_num.replace(',', '.') # 1,5 -> 1.5 Nếu là số thì sẽ phải đưa dấu , về dấu .
    try:
        return float(clean_num)
    except ValueError:
        return None
    
# ==========================================
# 3. HÀM XỬ LÝ VIỆC CHUYỂN TRƯỜNG GIÁ VỀ ĐÚNG ĐỊNH DẠNG
# ==========================================
def handlePrice(price):
    """Tách xâu và đưa về đúng định dạng giá trị và đơn vị của giá cả ví dụ "17" và "triệu" trong "17 triệu"; "3.5" và "tỷ" trong "3.5 tỷ"."""
    if not isinstance(price, str) or not price:
        return pd.Series([None, None])
    text = price.lower()
    unit = re.sub(r'^[\d.,\s]+', '', text).strip()
    match_num = re.search(r"([\d.,]+)", text)
    if not match_num:
        return pd.Series([None, unit]) 
    raw_num_str = match_num.group(1).replace(',', '.')
    try:
        price_val = float(raw_num_str)
    except ValueError:
        return pd.Series([None, unit]) 
    return pd.Series([price_val, unit])

# ==========================================
# 4. HÀM XỬ LÝ TRƯỜNG TỌA ĐỘ
# ==========================================
def handleLatLon(url: str):
    """Trả về các giá trị lat lon trong googlemap iframe"""
    if not isinstance(url, str) or not url:
        return pd.Series([None, None])

    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        if "q" not in params:
            return pd.Series([None, None])

        lat, lon = map(float, params["q"][0].split(","))

        return pd.Series([lat, lon])

    except Exception:
        return pd.Series([None, None])

# ==========================================
# 5. HÀM XỬ LÝ DIỆN TÍCH
# ==========================================
def handleArea(area_str):
    """Tách xâu và đưa về đúng định dạng giá trị và đơn vị của diện tích ví dụ 2.350 m² về "2350" và "m²"."""
    if not isinstance(area_str, str) or not area_str:
        return pd.Series([None, None])
    text = area_str.lower()
    area_val = parse_number_raw(text, is_area=True)
    unit = re.sub(r'^[\d.,\s]+', '', text).strip()
    return pd.Series([area_val, unit])

# ==========================================
# 6. HÀM XỬ LÝ SỐ PHÒNG (TÁCH RA ĐỂ CHO RÕ RÀNG HƠN)
# ==========================================
def handleRooms(room_str):
    """Đưa về số phòng tương ứng (Hàm này gọi parse_number_raw chứ không xử lý gì hơn)"""
    return parse_number_raw(room_str, is_area=False)


# ==========================================
# 7. HÀM XỬ LÝ TIỆN ÍCH GỒM CAMERA, BẢO VỆ, PHÒNG CHÁY CHỮA CHÁY
# ==========================================
def handleUtilities(ulities):
    """Trả về các giá trị tương ứng kiểm tra các từ khóa trong xâu tiện ích có gồm "camera", "bảo vệ" hoặc "pccc" hay không."""
    ulities = str(ulities).lower()
    camera = True if "camera" in ulities else None
    security = True if "bảo vệ" in ulities else None
    pccc = True if "pccc" in ulities else None
    return pd.Series([camera, security, pccc])

# ==========================================
# 8. HÀM XỬ LÝ TRƯỜNG TỌA ĐỘ
# ==========================================
def handleDescription(text):
    """
    Xử lý mô tả đơn giản:
    1. Xóa các icon/emoji.
    2. Xóa cụm "thông tin mô tả" ở đầu chuỗi.
    """
    # Là null thì trả về xâu rỗng để tránh lỗi các bước kế tiếp.
    if not text:
        return ""
    
    # Đảm bảo input là chuỗi ký tự và chuẩn hóa unicode
    text = str(text)
    
    # 1. Xóa các icon/emoji và ký tự đặc biệt không phải chữ/số/dấu câu thông thường
        # Xóa các ký tự Unicode ngoài phạm vi thông thường (Emoji, Icon...)
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text, flags=re.UNICODE) 
        # Xóa các block icon thường dùng khác (Dingbats, Emoticons block, etc.)
    text = re.sub(r'[\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF]', '', text)
    
    # 2. Xóa cụm "thông tin mô tả" ở đầu chuỗi
    text = re.sub(
        r'^\s*thông tin mô tả\s*:?\s*',
        '',
        text,
        flags=re.IGNORECASE
    ).strip()
    
    # Làm sạch khoảng trắng thừa ở cuối đoạn
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


# ==========================================
# 8. HÀM XỬ LÝ TỔNG
# ==========================================
def transform_real_estate_df(df_input: pd.DataFrame, parser=None) -> pd.DataFrame:
    """
    Hàm biến đổi dữ liệu Bất động sản.
    Args:
        df_input (pd.DataFrame): DataFrame thô (đã qua json_normalize).
        parser (AddressParser, optional): Object sử dụng để phân tích địa chỉ. 
    Returns:
        pd.DataFrame: DataFrame đã được làm sạch và chuẩn hóa.
    """
    
    # 1. Kiểm tra input
    if df_input is None or df_input.empty:
        print("⚠️ DataFrame rỗng! Hãy kiểm tra lại đầu vào!")
        return pd.DataFrame()
    print(f"⚙️ Đang xử lý {len(df_input)} dòng dữ liệu...")

    # 2. Khởi tạo AddressParser nếu chưa có (Tránh download lại dữ liệu nhiều lần)
    # if parser is None:
    #     parser = AddressParser()

    # 3. Tạo DataFrame kết quả để tổng hợp lại
    df_out = pd.DataFrame()

    # --- NHÓM 1: GÍA VÀ DIỆN TÍCH ---
    if 'specs.Khoảng giá' in df_input.columns:
        df_out[['num_price', 'unit_price']] = df_input['specs.Khoảng giá'].apply(handlePrice)
    if 'specs.Diện tích' in df_input.columns:
        df_out[['num_area', 'unit_area']] = df_input['specs.Diện tích'].apply(handleArea)

    # --- NHÓM 2: SỐ PHÒNG/TÀNG ---
    cols_mapping_rooms = {
        'specs.Số phòng ngủ': 'num_bedroom',
        'specs.Số phòng tắm, vệ sinh': 'num_bathroom',
        'specs.Số tầng': 'floor_count'
    }
    for input_col, output_col in cols_mapping_rooms.items():
        if input_col in df_input.columns:
            df_out[output_col] = df_input[input_col].apply(handleRooms)

    # --- NHÓM 3: KÍCH THƯỚC MẶT TIỀN ---
    if 'specs.Mặt tiền' in df_input.columns:
        df_out[['num_front_width', 'unit_front_width']] = df_input['specs.Mặt tiền'].apply(handleArea)
    if 'specs.Đường vào' in df_input.columns:
        df_out[['num_road_width', 'unit_road_width']] = df_input['specs.Đường vào'].apply(handleArea)

    # --- NHÓM 4: TIỆN ÍCH NHƯ ĐIỆN, NƯỚC, MẠNG,..... CÁC CỘT MÀ KHÔNG CẦN QUA XỬ LÝ ---
    simple_copy_cols = {
        'specs.Thời gian dự kiến vào ở': 'move_in',
        'specs.Mức giá internet': 'iprice',
        'specs.Mức giá điện': 'eprice',
        'specs.Mức giá nước': 'wprice',
        'images': 'img',
        'specs.Nội thất': 'furniture'
    }
    for input_col, output_col in simple_copy_cols.items():
        df_out[output_col] = df_input.get(input_col) # .get trả về NaN nếu ko có cột

    # --- NHÓM 5: XỬ LÝ VÀ LÀM SẠCH CÁC ĐOẠN VĂN BẢN ---
    if 'description' in df_input.columns:
        df_out['description'] = df_input['description'].apply(handleDescription)
    if 'specs.Pháp lý' in df_input.columns:
        df_out['policy'] = df_input['specs.Pháp lý'].apply(handleDescription)

    # --- NHÓM 6: TIỆN ÍCH NHƯ PCCC, BẢO VỆ VÀ CAM GIÁM SÁT ---
    if 'specs.Tiện ích' in df_input.columns:
        df_out[['camera', 'security', 'pccc']] = df_input['specs.Tiện ích'].apply(handleUtilities)

    if 'place' in df_input.columns:
        df_out[['lat', 'lon']] = df_input['place'].apply(handleLatLon)

    # --- NHÓM 7: XỬ LÝ ĐỊA CHỈ THÀNH CÁC TRƯỜNG NHƯ XÃ/PHUONGF, QUẬN/HUYỆN, TỈNH/THÀNH PHỐ ---
    # if 'address' in df_input.columns:
    #     print("📍 Đang phân tích địa chỉ...")
    #     # Sử dụng parser được truyền vào
    #     address_parsed = df_input['address'].apply(lambda x: pd.Series(parser.parse(x)))
    #     df_out[['street', 'ward', 'district', 'city']] = address_parsed
    df_out['title'] = df_input.get('title').fillna('')
    df_out['address'] = df_input.get('address').fillna('')
    df_out['url'] = df_input['url']
    print("✅ Đã xử lý xong!")
    return df_out