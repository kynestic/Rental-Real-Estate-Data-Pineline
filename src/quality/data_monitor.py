import pandas as pd
import requests
import pandas as pd

def monitor_parsed(df, site_name):
    """
    Giám sát dữ liệu sau khi vừa trích xuất từ HTML.
    """
    total = len(df)
    if total == 0:
        return f"⚠️ *Cảnh báo:* Dữ liệu của {site_name} trống (0 dòng)!"

    # 1. Khởi tạo báo cáo
    report = f"📊 *BÁO CÁO DỮ LIỆU SAU KHI TRÍCH XUẤT: {site_name.upper()}*\n"
    report += f"🔢 Tổng số bản ghi: `{total}`\n\n"

    # 2. Kiểm tra các trường chính
    # Danh sách các trường cần kiểm tra (nếu có trong DataFrame)
    main_fields = ["title", "address", "description", "dates", "place", "images"]
    report += "📍 *Thông tin chung:*\n"

    for col in main_fields:
        if col in df.columns:
            # Kiểm tra xâu rỗng và list rỗng (theo yêu cầu không có null)
            # len(x) > 0 hoạt động cho cả str, list, dict
            non_empty_count = df[col].apply(lambda x: len(x) > 0 if hasattr(x, '__len__') else False).sum()
            fill_rate = (non_empty_count / total) * 100
            
            icon = "✅" if fill_rate > 80 else "⚠️"
            if fill_rate == 0: icon = "❌"
            
            report += f"{icon} {col}: `{fill_rate:.1f}%`\n"

    # 3. Kiểm tra các trường Specs (những cột bắt đầu bằng 'specs.')
    report += "\n🛠 *Thông tin chi tiết :*\n"
    
    # Lấy danh sách các cột specs
    spec_cols = [c for c in df.columns if c.startswith('specs.')]
    
    if spec_cols:
        for col in spec_cols:
            # Vì đã flatten, các giá trị thường là String hoặc Số
            # Kiểm tra: không rỗng, không phải xâu "nan", không phải xâu "None"
            non_empty_count = df[col].apply(lambda x: str(x).strip() not in ["", "nan", "None", "NaN"]).sum()
            fill_rate = (non_empty_count / total) * 100
            
            # Chỉ hiển thị các field thực sự tồn tại trong dữ liệu
            short_name = col.replace("specs.", "")
            if fill_rate > 0:
                report += f"🔹 {short_name}: `{fill_rate:.1f}%`\n"
            else:
                report += f"💀 {short_name}: `0%` (Trống)\n"
    else:
        report += "❌ Không tìm thấy các cột specs!\n"

    return report


def monitor_processed(df, site_name):
    """
    Giám sát dữ liệu đã flatten & rename.
    Sử dụng Unicode Escape để tránh lỗi SyntaxError với emoji.
    Ngưỡng: Xanh (>75%), Vàng (25-75%), Đỏ (<25%).
    """
    total = len(df)
    if total == 0:
        return f"⚠️ *Cảnh báo:* Dữ liệu {site_name} trống rỗng!"

    # 1. Danh sách giá trị coi là "Rỗng"
    invalid_values = ["", "nan", "none", "null", "nan", "nan"]

    # Định nghĩa mã Unicode cho các icon màu
    ICON_GREEN = "\U0001F7E2"  # 🟢
    ICON_YELLOW = "\U0001F7E1" # 🟡
    ICON_RED = "\U0001F7E0"    # 🔴
    ICON_SKULL = "\U0001F480"  # 💀

    report = f"📊 *BÁO CÁO CHẤT LƯỢNG DỮ LIỆU SAU XỬ LÝ: {site_name.upper()}*\n"
    report += f"🔢 Tổng số bản ghi: `{total}`\n"
    report += "---"
    report += "\n📋 *Tỷ lệ đầy đủ giá trị:*\n"

    # 2. Lấy danh sách tất cả các cột và sắp xếp theo bảng chữ cái A-Z
    all_cols = sorted(df.columns.tolist())

    for col in all_cols:
        # Chuyển về string, xóa khoảng trắng và viết thường để kiểm tra
        # fillna('') để đảm bảo không còn giá trị None/NaN thực thụ
        clean_s = df[col].astype(str).replace('nan', '').str.strip().str.lower()
        
        # Tính số lượng hợp lệ (không nằm trong invalid_values)
        valid_count = clean_s[~clean_s.isin(invalid_values)].count()
        rate = (valid_count / total) * 100 if total > 0 else 0

        # 3. Xác định icon màu dựa trên ngưỡng yêu cầu
        if rate > 75:
            icon = ICON_GREEN
        elif 25 <= rate <= 75:
            icon = ICON_YELLOW
        else:
            icon = ICON_RED
            
        # Nếu rỗng hoàn toàn 0% thì dùng icon đầu lâu để nhấn mạnh lỗi bóc tách
        if rate == 0:
            icon = ICON_SKULL

        # 4. Trình bày: Icon + Tên trường (bọc backtick) + Tỷ lệ
        # Backtick giúp bảo vệ dấu gạch dưới _ không bị in nghiêng
        report += f"{icon} `{col}`: `{rate:.1f}%`\n"

    report += "---"
    return report


