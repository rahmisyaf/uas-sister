# UAS Sistem Terdistribusi - Idempotent Log Aggregator

Proyek ini adalah implementasi *Pub-Sub Log Aggregator* asinkron untuk memenuhi tugas UAS mata kuliah Sistem Terdistribusi. Sistem ini dibangun menggunakan FastAPI (Python), Redis (Broker), dan PostgreSQL (Storage) yang diorkestrasi menggunakan Docker Compose.

---

## 🔗 Tautan Pengumpulan
* **Video Demo:** https://youtu.be/B10BX5fEJ1M
* **Laporan PDF:** https://drive.google.com/drive/folders/1d-YYVJ7FYRT4Fzh_uT8ASUlr3bOVnUQd?usp=sharing

---

## 💻 Lingkungan Pengembangan (Environment)
* **OS:** Ubuntu (via Windows Subsystem for Linux / WSL)
* **Engine:** Docker & Docker Compose
* **Lokal Environment:** Python 3 (untuk mengeksekusi virtual environment & Pytest di host)

## 🔌 Konfigurasi Port
Aplikasi berjalan dalam *bridge network* lokal bernama `sister_network`.
* **Aggregator API:** Port `9000` (Satu-satunya port yang diekspos ke `localhost` komputer)
* **PostgreSQL (Storage):** Port `5432` (Internal Docker Network)
* **Redis (Broker):** Port `6379` (Internal Docker Network)

---

## 🚀 Cara Menjalankan Aplikasi (via Terminal WSL)

1. Pastikan Anda berada di dalam direktori proyek:
```bash
   cd "/mnt/d/school/Semester 6/SisTer/uts-sister"
```

2. Build dan jalankan seluruh container (API, Broker, DB, dan 5 Worker) di background:
```bash
   docker compose up --build -d
```

3. Untuk melihat log aktivitas secara real-time (terutama melihat antrean diproses oleh worker):
```bash
   docker compose logs -f
```

4. Untuk mematikan aplikasi:
```bash
   docker compose down
```

5. Untuk mematikan sekaligus menghapus data database (reset volume):
```bash
   docker compose down -v
```

---

## 🌐 Endpoint API (Akses di http://localhost:9000)

Sistem memiliki tiga endpoint utama. Anda dapat mengujinya via Postman atau Curl.

### 1. Publish Event
Endpoint untuk mengirim data log ke antrean.

* Method: `POST`
* URL: `http://localhost:9000/publish`
* Payload JSON:
```json
  {
    "topic": "test_topic",
    "event_id": "uuid-1234-5678",
    "timestamp": "2026-06-18T10:00:00Z",
    "source": "klien_lokal",
    "payload": {
      "data": "bebas"
    }
  }
```

### 2. Cek Event yang Tersimpan
Endpoint untuk melihat data unik yang berhasil melewati deduplikasi database.

* Method: `GET`
* URL: `http://localhost:9000/events?topic=test_topic`

### 3. Cek Statistik
Endpoint untuk memantau metrik performa worker dan jumlah deduplikasi.

* Method: `GET`
* URL: `http://localhost:9000/stats`

---

## 🧪 Cara Menjalankan Automated Test (Pytest)

Test suite berisi 12 test cases untuk menguji validasi, deduplikasi, reliabilitas multi-worker (race condition), dan tes persistensi crash container. Eksekusi dilakukan di WSL.

1. Aktifkan virtual environment:
```bash
   source venv/bin/activate
```
   (Jika belum ada, buat dengan: `python3 -m venv venv`)

2. Install dependency (jika belum):
```bash
   python3 -m pip install pytest requests
```

3. Jalankan testing:
```bash
   python3 -m pytest tests/test_app.py -v -s
```
