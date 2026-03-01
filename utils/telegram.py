import requests
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_message(message):
    token = TELEGRAM_TOKEN
    chat_id = TELEGRAM_CHAT_ID
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown" # Giúp tin nhắn có đậm, nghiêng, code block
    }
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"Lỗi gửi Telegram: {response.text}")
    except Exception as e:
        print(f"Lỗi kết nối Telegram: {e}")