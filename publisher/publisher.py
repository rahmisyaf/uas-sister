import time
import random
import uuid
import requests
import logging
from datetime import datetime, timezone

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - PUBLISHER - %(message)s")
logger = logging.getLogger(__name__)

TARGET_URL = "http://aggregator:8080/publish"
TOPICS = ["auth", "payment", "user_activity", "system_alert"]
SOURCES = ["web_frontend", "mobile_app", "billing_service", "auth_service"]

# Menyimpan history event untuk simulasi pengiriman ulang
sent_events = []

def generate_event(is_duplicate=False):
    if is_duplicate and sent_events:
        # Ambil event lama, biarkan topic & event_id persis sama untuk menguji deduplikasi
        event = random.choice(sent_events).copy()
        event["timestamp"] = datetime.now(timezone.utc).isoformat()
        return event
    
    # Buat event baru sepenuhnya
    event_id = str(uuid.uuid4())
    event = {
        "topic": random.choice(TOPICS),
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": random.choice(SOURCES),
        "payload": {
            "action": "user_click",
            "status": random.choice(["success", "failed", "pending"]),
            "value": random.randint(1, 100)
        }
    }
    sent_events.append(event)
    return event

def start_simulation():
    logger.info("Publisher simulator memanaskan mesin...")
    
    # Block sampai Aggregator benar-benar siap
    while True:
        try:
            # Karena di dalam Docker Compose network, kita tembak 8080
            res = requests.get("http://aggregator:8080/")
            if res.status_code == 200:
                logger.info("Aggregator API sudah siap! Memulai pengiriman event...")
                break
        except requests.exceptions.RequestException:
            logger.warning("Menunggu Aggregator siap...")
            time.sleep(2)

    counter = 1
    while True:
        # 30% kemungkinan untuk mengirim data yang sama persis (Retry simulation)
        is_duplicate = random.random() < 0.30
        event = generate_event(is_duplicate)
        
        try:
            response = requests.post(TARGET_URL, json=event, timeout=2)
            if response.status_code == 200:
                if is_duplicate:
                    logger.warning(f"[{counter}] MENGIRIM DUPLIKAT -> Topik: {event['topic']} | ID: {event['event_id']}")
                else:
                    logger.info(f"[{counter}] MENGIRIM BARU -> Topik: {event['topic']} | ID: {event['event_id']}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Gagal mengirim event: {e}")
        
        counter += 1
        time.sleep(0.5) # Jeda setengah detik agar log enak dibaca saat demo video

if __name__ == "__main__":
    start_simulation()