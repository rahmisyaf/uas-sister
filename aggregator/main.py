import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as aioredis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://user:pass@storage:5432/db")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379")
QUEUE_NAME = "log_queue"

db_pool = None
redis_client = None
# PERUBAHAN: Menggunakan list untuk menampung banyak worker
worker_tasks = []

class LogEvent(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]

async def consumer_worker(worker_id: int):
    logger.info(f"Worker-{worker_id} sukses berjalan dan mendengarkan antrean Redis...")
    while True:
        try:
            result = await redis_client.blpop(QUEUE_NAME, timeout=1)
            if not result:
                continue
            
            _, message_str = result
            event_data = json.loads(message_str)
            
            topic = event_data["topic"]
            event_id = event_data["event_id"]
            timestamp_str = event_data["timestamp"]
            source = event_data["source"]
            payload = json.dumps(event_data["payload"])
            
            try:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                timestamp = dt.replace(tzinfo=None)
            except ValueError:
                timestamp = datetime.utcnow()

            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    query = """
                        INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
                        VALUES ($1, $2, $3, $4, $5::jsonb)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        RETURNING id;
                    """
                    res = await conn.fetchval(query, topic, event_id, timestamp, source, payload)
                    
                    if res is not None:
                        await redis_client.incr("stats:processed")
                        logger.info(f"[Worker-{worker_id} BERHASIL] Event Unik Disimpan -> ID: {event_id[:8]}... | Topik: {topic}")
                    else:
                        await redis_client.incr("stats:dropped")
                        logger.warning(f"[Worker-{worker_id} DEDUPLIKASI] Event Ganda Ditolak -> ID: {event_id[:8]}... | Topik: {topic}")
                            
        except asyncio.CancelledError:
            logger.info(f"Worker-{worker_id} dimatikan secara graceful.")
            break
        except Exception as e:
            logger.error(f"Error pada Worker-{worker_id}: {e}")
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, redis_client, worker_tasks
    logger.info("Menginisialisasi pool koneksi PostgreSQL dan Redis...")
    
    retries = 5
    while retries > 0:
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            logger.info("Berhasil terhubung ke PostgreSQL!")
            break
        except Exception as e:
            retries -= 1
            logger.warning(f"Menunggu PostgreSQL siap... ({retries} percobaan tersisa).")
            await asyncio.sleep(3)
            
    if not db_pool:
        logger.error("Gagal terhubung ke Database. Mematikan aplikasi.")
        raise Exception("Database connection failed")

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    
    # PERUBAHAN: Menjalankan 5 Consumer Worker sekaligus
    num_workers = 5
    for i in range(1, num_workers + 1):
        task = asyncio.create_task(consumer_worker(worker_id=i))
        worker_tasks.append(task)
        
    yield
    
    # Cleanup semua worker
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    await db_pool.close()
    await redis_client.close()

app = FastAPI(title="Log Aggregator API", lifespan=lifespan)

@app.get("/")
async def health_check():
    return {"status": "ok"}

@app.post("/publish")
async def publish_event(event: LogEvent):
    try:
        await redis_client.incr("stats:received")
        event_dict = event.model_dump()
        await redis_client.rpush(QUEUE_NAME, json.dumps(event_dict))
        return {"status": "queued", "event_id": event.event_id}
    except Exception as e:
        logger.error(f"Gagal mempublikasikan event: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/events")
async def get_events(topic: str = None):
    async with db_pool.acquire() as conn:
        if topic:
            rows = await conn.fetch("SELECT topic, event_id, timestamp, source, payload FROM processed_events WHERE topic = $1 ORDER BY timestamp DESC", topic)
        else:
            rows = await conn.fetch("SELECT topic, event_id, timestamp, source, payload FROM processed_events ORDER BY timestamp DESC")
        
        return [
            {
                "topic": r["topic"],
                "event_id": r["event_id"],
                "timestamp": r["timestamp"].isoformat(),
                "source": r["source"],
                "payload": json.loads(r["payload"]) if r["payload"] else {}
            }
            for r in rows
        ]

@app.get("/stats")
async def get_stats():
    received = int(await redis_client.get("stats:received") or 0)
    processed = int(await redis_client.get("stats:processed") or 0)
    dropped = int(await redis_client.get("stats:dropped") or 0)
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT topic FROM processed_events")
        topics = [r["topic"] for r in rows]
        
    return {
        "received": received,
        "unique_processed": processed,
        "duplicate_dropped": dropped,
        "topics": topics
    }