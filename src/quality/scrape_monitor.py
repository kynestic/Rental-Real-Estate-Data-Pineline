import os
import time
import csv
import requests
import datetime as dt
from dotenv import load_dotenv

# Nạp cấu hình từ .env
load_dotenv()

token       = os.getenv("TELEGRAM_TOKEN")
chat_id     = os.getenv("TELEGRAM_CHAT_ID")
rb_user     = os.getenv("RB_USER")
rb_pass     = os.getenv("RB_PASS")
rb_url      = os.getenv("RB_API_URL")
tg_base_url = os.getenv("TG_BASE_URL")

def get_log_path(web_name):
    """Tạo thư mục crawl_log/{web_name} và trả về đường dẫn file csv theo ngày"""
    # Tạo cấu trúc crawl_log/muaban.net/
    folder_path = os.path.join("crawl_log", web_name)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    # Tên file: queue_stats_2026-01-16.csv
    filename = f"queue_stats_{dt.datetime.now().strftime('%Y-%m-%d')}.csv"
    return os.path.join(folder_path, filename)

def save_to_csv(web_name, all_queues):
    """Ghi lại thông tin toàn bộ queue vào file CSV"""
    file_path = get_log_path(web_name)
    file_exists = os.path.isfile(file_path)
    
    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Header: Thời gian, Tên Queue, Số lượng tin nhắn, Tốc độ (nếu có)
        if not file_exists:
            writer.writerow(["Timestamp", "Queue_Name", "Messages_Ready", "Messages_Unacked", "Total_Messages"])
        
        timestamp = dt.datetime.now().strftime('%H:%M:%S')
        for q in all_queues:
            writer.writerow([
                timestamp,
                q.get('name'),
                q.get('messages_ready', 0),
                q.get('messages_unacknowledged', 0),
                q.get('messages', 0)
            ])

def start_monitor(web_name, total_items):
    tg_full_url = f"{tg_base_url}{token}"
    session = requests.Session()
    session.auth = (rb_user, rb_pass)
    message_id = None
    
    last_dlq_alert = 0
    last_retry_alert = 0

    print(f"🚀 Bắt đầu giám sát RabbitMQ cho: {web_name}")
    print(f"📁 Logs sẽ được lưu tại: crawl_log/{web_name}/")

    while True:
        try:
            # 1. Lấy dữ liệu từ RabbitMQ Management API
            resp = session.get(rb_url, timeout=10).json()
            
            # --- GHI LOG TOÀN BỘ QUEUE ---
            save_to_csv(web_name, resp)
            
            # --- XỬ LÝ DỮ LIỆU RIÊNG CHO WEBSITE (TELEGRAM UI) ---
            stats = {q['name']: q.get('messages', 0) for q in resp if web_name in q['name']}
            
            completed_html = sum(count for name, count in stats.items() if name.endswith('_html'))
            current_dlq = sum(v for k, v in stats.items() if 'dlq' in k.lower())
            current_retry = sum(v for k, v in stats.items() if 'retry' in k.lower())
            
            ratio = min(completed_html / total_items, 1.0)
            
            # 2. Kiểm tra Alert (DLQ/Retry)
            alert_msgs = []
            if current_dlq >= last_dlq_alert + 10:
                alert_msgs.append(f"⚠️ *ALERT:* DLQ đã tăng lên `{current_dlq}` item!")
                last_dlq_alert = (current_dlq // 10) * 10
                
            if current_retry >= last_retry_alert + 20:
                alert_msgs.append(f"🔄 *ALERT:* Retry đã tăng lên `{current_retry}` item!")
                last_retry_alert = (current_retry // 20) * 20

            for alert_txt in alert_msgs:
                session.post(f"{tg_full_url}/sendMessage", json={"chat_id": chat_id, "text": alert_txt, "parse_mode": "Markdown"})

            # 3. Render giao diện Telegram
            bar = f"[{'█' * int(15 * ratio)}{'░' * (15 - int(15 * ratio))}] {int(ratio * 100)}%"
            details = "\n".join(f"• `{k}`: {v:,}" for k, v in stats.items()) or "Không thấy queue"
            
            msg = (
                f"🌐 *WEBSITE:* `{web_name.upper()}`\n━━━━━━━━━━━━━━━\n{bar}\n"
                f"✅ Đã xong: `{completed_html:,} / {total_items:,}`\n\n"
                f"📥 *Queues:*\n{details}\n━━━━━━━━━━━━━━━\n"
                f"🕒 Cập nhật: `{dt.datetime.now().strftime('%H:%M:%S')}`\n"
                f"💾 Log: `Saved to crawl_log/{web_name}/`"
            )

            # 4. Cập nhật Telegram
            payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
            if message_id:
                payload["message_id"] = message_id
                session.post(f"{tg_full_url}/editMessageText", json=payload)
            else:
                r = session.post(f"{tg_full_url}/sendMessage", json=payload).json()
                message_id = r.get('result', {}).get('message_id')

            print(f"✅ Đã log và cập nhật lúc {dt.datetime.now().strftime('%H:%M:%S')}")

        except Exception as e:
            print(f"⚠️ Lỗi Pipeline Monitor: {e}")

        time.sleep(10)

if __name__ == "__main__":
    start_monitor(web_name='muaban.net', total_items=48000)