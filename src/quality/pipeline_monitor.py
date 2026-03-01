import os
import time
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

def start_monitor(web_name, total_items):
    # Khởi tạo URL và Session
    tg_full_url = f"{tg_base_url}{token}"
    session = requests.Session()
    session.auth = (rb_user, rb_pass)
    message_id = None
    
    # Biến theo dõi để gửi alert (Thêm mới)
    last_dlq_alert = 0
    last_retry_alert = 0

    while True:
        try:
            # 1. Lấy dữ liệu từ RabbitMQ
            resp = session.get(rb_url, timeout=5).json()
            stats = {q['name']: q.get('messages', 0) for q in resp if web_name in q['name']}
            
            completed_html = sum(count for name, count in stats.items() if name.endswith('_html'))
            # Lấy tổng số dlq và retry hiện tại (Thêm mới)
            current_dlq = sum(v for k, v in stats.items() if 'dlq' in k.lower())
            current_retry = sum(v for k, v in stats.items() if 'retry' in k.lower())
            
            ratio = min(completed_html / total_items, 1.0)
            
            # 2. Kiểm tra điều kiện gửi Alert phụ (Thêm mới)
            alert_msgs = []
            if current_dlq >= last_dlq_alert + 10:
                alert_msgs.append(f"⚠️ *ALERT:* DLQ đã tăng lên `{current_dlq}` item!")
                last_dlq_alert = (current_dlq // 10) * 10 # Cập nhật mốc alert tiếp theo
                
            if current_retry >= last_retry_alert + 20:
                alert_msgs.append(f"🔄 *ALERT:* Retry đã tăng lên `{current_retry}` item!")
                last_retry_alert = (current_retry // 20) * 20

            for alert_txt in alert_msgs:
                session.post(f"{tg_full_url}/sendMessage", json={"chat_id": chat_id, "text": alert_txt, "parse_mode": "Markdown"})

            # 3. Render giao diện chính
            bar = f"[{'█' * int(15 * ratio)}{'░' * (15 - int(15 * ratio))}] {int(ratio * 100)}%"
            details = "\n".join(f"• `{k}`: {v:,}" for k, v in stats.items()) or "Không thấy queue"
            
            msg = (
                f"🌐 *WEBSITE:* `{web_name.upper()}`\n━━━━━━━━━━━━━━━\n{bar}\n"
                f"✅ Đã xong: `{completed_html:,} / {total_items:,}`\n\n"
                f"📥 *Queues:*\n{details}\n━━━━━━━━━━━━━━━\n"
                f"🕒 Cập nhật: `{dt.datetime.now().strftime('%H:%M:%S')}`"
            )

            # 4. Gửi/Cập nhật Telegram qua Session
            payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
            if message_id:
                payload["message_id"] = message_id
                session.post(f"{tg_full_url}/editMessageText", json=payload)
            else:
                r = session.post(f"{tg_full_url}/sendMessage", json=payload).json()
                message_id = r.get('result', {}).get('message_id')

        except Exception as e:
            print(f"⚠️ Lỗi: {e}")

        time.sleep(10)

# if __name__ == "__main__":
#     start_monitor(
#         web_name    = 'muaban.net',
#         total_items = 48000,
#     )