from datetime import datetime

now = datetime.utcnow()
year, month, day = now.year, now.month, now.day
date_str = f"{year}-{month:02d}-{day:02d}"

from selectolax.parser import HTMLParser

def parse_minimal(html_text):
    """Logic trích xuất dữ liệu từ HTML sử dụng Selectolax cho Batdongsan.com.vn"""
    if not html_text or not isinstance(html_text, str): 
        return {}
    
    tree = HTMLParser(html_text)
    
    def get_text(selector):
        node = tree.css_first(selector)
        return node.text(strip=True) if node else ""

    # 1. Thông tin cơ bản như địa chỉ
    title = get_text(".re__pr-title, .pr-title, .js__pr-title")
    address = get_text(".re__pr-short-description, .js__pr-address")
    
    # 2. Mô tả
    desc_node = tree.css_first(".re__section.re__pr-description, .re__pr-description, .js__li-description")
    if desc_node:
        description = desc_node.text(strip=True)
    else:
        paragraphs = [p.text(strip=True) for p in tree.css("p")]
        description = max(paragraphs, key=len) if paragraphs else ""

    # 3. Thông tin chi tiết
    specs = {}
    for item in tree.css(".re__pr-specs-content-item"):
        t_node = item.css_first(".re__pr-specs-content-item-title")
        v_node = item.css_first(".re__pr-specs-content-item-value")
        if t_node and v_node:
            specs[t_node.text(strip=True)] = v_node.text(strip=True)

    # 4. Ngày tháng đăng bài
    dates = {}
    for b in tree.css(".re__pr-short-info-item.js__pr-config-item"):
        k_node = b.css_first(".title")
        v_node = b.css_first(".value")

        key = k_node.text(strip=True) if k_node else (b.attributes.get("title") or "").strip()
        val = v_node.text(strip=True) if v_node else (b.attributes.get("value") or "").strip()
        
        if key and val: 
            dates[key] = val

    # 5. Hình ảnh và địa chỉ
    imgs = [img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy")
            for img in tree.css(".swiper-wrapper img")]
    images = list(dict.fromkeys([i.strip() for i in imgs if i]))
    
    map_node = tree.css_first("iframe[data-src]")
    place = map_node.attributes.get("data-src") if map_node else ""

    return {
        "title": title,
        "address": address,
        "description": description,
        "specs": specs,
        "dates": dates,
        "place": place,
        "images": images
    }

        



