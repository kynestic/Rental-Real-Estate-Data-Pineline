from selectolax.parser import HTMLParser
import json

import requests

def get_lat_lon(address):
    try:
        url = "https://geocode.maps.co/search"
        params = {"q": address, "api_key": "696c2bdaf05b3813511490yrz091f7c"}
        
        # Gọi API và parse JSON
        data = requests.get(url, params=params).json()
        
        # Trả về lat, lon của kết quả đầu tiên nếu có dữ liệu
        return (data[0]['lat'], data[0]['lon']) if data else (None, None)
    except:
        return None, None

def parse_minimal(html_content):
    # 1. Đọc nội dung file HTML
    # try:
    #     with open(file_path, 'r', encoding='utf-8') as f:
    #         html_content = f.read()
    # except FileNotFoundError:
    #     print(f"Không tìm thấy file {file_path}")
    #     return None

    # Parse HTML bằng Selectolax (nhanh hơn BS4)
    tree = HTMLParser(html_content)

    # --- Helper function để lấy text an toàn ---
    def get_text_safe(node_tree, selector):
        node = node_tree.css_first(selector)
        return node.text(strip=True) if node else ""

    # 2. Parse các trường đơn
    # Title
    title = get_text_safe(tree, 'div.sc-6orc5o-8.bzqDYr > h1')

    # Address
    address = get_text_safe(tree, 'div.sc-6orc5o-8.bzqDYr > div.address')

    # Description
    # Selectolax hỗ trợ separator trong hàm text() ở các phiên bản mới
    desc_node = tree.css_first('div.sc-6orc5o-10.eRboKF')
    description = desc_node.text(separator='\n', strip=True) if desc_node else ""

    # 3. Parse Specs (Thông số)
    specs = {}
    specs_ul = tree.css_first('ul.sc-6orc5o-16')
    if specs_ul:
        # css() trả về list các node con khớp selector
        for li in specs_ul.css('li'):
            spans = li.css('span')
            # Kiểm tra độ dài list spans
            if len(spans) >= 2:
                key = spans[0].text(strip=True).replace(':', '')
                value = spans[1].text(strip=True)
                if key and value:
                    specs[key] = value
    price = get_text_safe(tree, 'div.sc-6orc5o-8.bzqDYr > div.price')
    specs['Giá'] = price
    # 4. Parse Dates (Ngày đăng/hết hạn)
    # Logic tương đương recursive=False trong BS4 là dùng .iter(include_text=False)
    dates = {}
    dates_div = tree.css_first('div.sc-6orc5o-21.ebxmhG')
    if dates_div:
        # Lặp qua các node con trực tiếp (bỏ qua text node)
        for child in dates_div.iter(include_text=False):
            spans = child.css('span')
            if len(spans) >= 2:
                key = spans[0].text(strip=True).replace(':', '')
                value = spans[1].text(strip=True)
                if key and value:
                    dates[key] = value

    # 5. Parse Images
    images = []
    # img class="thumb-l"
    for img in tree.css('img.thumb-l'):
        # attributes là một dictionary trong selectolax
        src = img.attributes.get('src') or img.attributes.get('data-src')
        if src:
            images.append(src)
    
    # 6. Place
    place = None

    # 7. Trả về kết quả
    location = {
        "lat": 0,
        "lon": 0,
    }
    try:
        lat, lon =  get_lat_lon(address.replace(",", ""))
        location = {
            "lat": lat,
            "lon": lon,
        }
    except:
        print("Lỗi khi lấy tọa độ của địa chỉ trong muaban.net")
    return {
        "title": title,
        "address": address,
        "description": description,
        "specs": specs,
        "dates": dates,
        "place": location,
        "images": images
    }



        