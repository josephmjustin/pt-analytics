from src.api.celery_app import app
from scripts.continuous_poller import poll_and_ingest
from scripts.run_analysis import run_analysis
from redis import Redis
import os
from dotenv import load_dotenv  

load_dotenv()

redis = Redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1"))

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def run_ingestion(self):
    lock = redis.set("lock:ingestion", "running", ex=60, nx=True)
    if not lock:
        return "skipped - previous run active"
    try:
        result = poll_and_ingest()
        redis.delete("lock:ingestion")
        return result
    except Exception as exc:
        raise self.retry(exc=exc)

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def run_analysis_task(self):
    lock = redis.set("lock:analysis", "running", ex=60, nx=True)
    if not lock:
        return "skipped - previous run active"
    try:
        result = run_analysis()
        redis.delete("lock:analysis")
        return result
    except Exception as exc:
        raise self.retry(exc=exc)
