import pytest
import requests
import uuid
import time
import concurrent.futures
import subprocess

BASE_URL = "http://localhost:9000"

# --- HELPER FUNCTION ---
def get_payload(topic="pytest_topic", event_id=None):
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": "2026-06-18T10:00:00Z",
        "source": "pytest",
        "payload": {"data": "test"}
    }

def wait_for_api():
    """Helper untuk menunggu API menyala kembali setelah container di-restart"""
    for _ in range(15):
        try:
            if requests.get(f"{BASE_URL}/").status_code == 200:
                time.sleep(2) # Beri jeda ekstra agar worker connect ke DB
                return True
        except:
            time.sleep(2)
    return False

# --- 1. HEALTH CHECK ---
def test_01_health_check():
    res = requests.get(f"{BASE_URL}/")
    assert res.status_code == 200

# --- 2-5. VALIDASI SKEMA EVENT ---
def test_02_schema_valid():
    res = requests.post(f"{BASE_URL}/publish", json=get_payload())
    assert res.status_code == 200

def test_03_schema_missing_topic():
    payload = get_payload()
    del payload["topic"]
    res = requests.post(f"{BASE_URL}/publish", json=payload)
    assert res.status_code == 422 # 422 Unprocessable Entity dari FastAPI

def test_04_schema_missing_event_id():
    payload = get_payload()
    del payload["event_id"]
    res = requests.post(f"{BASE_URL}/publish", json=payload)
    assert res.status_code == 422

def test_05_schema_missing_payload():
    payload = get_payload()
    del payload["payload"]
    res = requests.post(f"{BASE_URL}/publish", json=payload)
    assert res.status_code == 422

# --- 6-7. KONSISTENSI GET /stats & GET /events ---
def test_06_events_consistency():
    event_id = str(uuid.uuid4())
    requests.post(f"{BASE_URL}/publish", json=get_payload("consistency_topic", event_id))
    time.sleep(1.5) # Tunggu worker memproses
    
    events = requests.get(f"{BASE_URL}/events?topic=consistency_topic").json()
    assert any(e["event_id"] == event_id for e in events)

def test_07_stats_consistency():
    stats_before = requests.get(f"{BASE_URL}/stats").json()
    requests.post(f"{BASE_URL}/publish", json=get_payload("stats_topic"))
    time.sleep(1.5)
    stats_after = requests.get(f"{BASE_URL}/stats").json()
    
    # Memastikan received dan unique_processed bertambah
    assert stats_after["received"] > stats_before["received"]
    assert stats_after["unique_processed"] > stats_before["unique_processed"]

# --- 8. DEDUPLIKASI (DUPLICATE EVENT) ---
def test_08_deduplication():
    event_id = str(uuid.uuid4())
    payload = get_payload("dedup_topic", event_id)
    
    res1 = requests.post(f"{BASE_URL}/publish", json=payload)
    res2 = requests.post(f"{BASE_URL}/publish", json=payload)
    
    assert res1.status_code == 200
    assert res2.status_code == 200
    time.sleep(1.5)
    
    events = requests.get(f"{BASE_URL}/events?topic=dedup_topic").json()
    # Walaupun dikirim 2x, yang tersimpan di DB dengan ID tersebut hanya 1
    count = sum(1 for e in events if e["event_id"] == event_id)
    assert count == 1

# --- 9. TRANSAKSI/KONKURENSI (RACE CONDITION) ---
def test_09_race_condition_multi_worker():
    event_id = str(uuid.uuid4())
    payload = get_payload("race_topic", event_id)
    
    def send_request():
        return requests.post(f"{BASE_URL}/publish", json=payload)
    
    # Menembakkan 20 request duplikat secara paralel di milidetik yang sama
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(lambda _: send_request(), range(20)))
        
    assert all(r.status_code == 200 for r in results)
    time.sleep(2) # Tunggu 5 worker berebutan data ini
    
    events = requests.get(f"{BASE_URL}/events?topic=race_topic").json()
    count = sum(1 for e in events if e["event_id"] == event_id)
    # Bukti multi-worker sukses mengatasi race condition dengan ON CONFLICT
    assert count == 1 

# --- 10. STRESS KECIL (BATCH EVENT & WAKTU EKSEKUSI) ---
def test_10_small_stress_test():
    start_time = time.time()
    for _ in range(50):
        requests.post(f"{BASE_URL}/publish", json=get_payload("stress_topic"))
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"\nWaktu eksekusi 50 event: {execution_time:.2f} detik")
    # Memastikan API bisa merespons 50 request dalam waktu kurang dari 2 detik
    assert execution_time < 2.0 

# --- 11 & 12. PERSISTENSI & DEDUP SETELAH CONTAINER RECREATE ---
GLOBAL_PERSISTENT_ID = str(uuid.uuid4())

def test_11_container_recreate_persistence():
    # 1. Masukkan data sebelum crash
    requests.post(f"{BASE_URL}/publish", json=get_payload("persistent_topic", GLOBAL_PERSISTENT_ID))
    time.sleep(2)
    
    # 2. Matikan paksa (restart) container aggregator & storage dari dalam Python!
    print("\n[Simulasi Server Crash - Merestart Container...]")
    subprocess.run(["docker", "compose", "restart", "aggregator", "storage"], check=True)
    
    # 3. Tunggu sampai sistem hidup lagi
    assert wait_for_api() == True
    
    # 4. Buktikan data sebelum crash masih ada di database
    events = requests.get(f"{BASE_URL}/events?topic=persistent_topic").json()
    assert any(e["event_id"] == GLOBAL_PERSISTENT_ID for e in events)

def test_12_dedup_after_recreate():
    # Mencoba memasukkan duplikat dari data sebelum server crash
    requests.post(f"{BASE_URL}/publish", json=get_payload("persistent_topic", GLOBAL_PERSISTENT_ID))
    time.sleep(1.5)
    
    events = requests.get(f"{BASE_URL}/events?topic=persistent_topic").json()
    # Harus tetap 1, membuktikan Unique Constraint di storage persisten tetap jalan
    count = sum(1 for e in events if e["event_id"] == GLOBAL_PERSISTENT_ID)
    assert count == 1