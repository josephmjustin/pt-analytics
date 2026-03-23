from src.api.celery_app import app
from scripts.continuous_poller import poll_and_ingest
from scripts.run_analysis import run_analysis

@app.task(bind=True, max_retries=3, default_retry_delay=30)
def run_ingestion(self):
    try:
        result = poll_and_ingest()
        return result
    except Exception as exc:
        raise self.retry(exc=exc)
    
@app.task(bind=True, max_retries=3, default_retry_delay=30)
def run_analysis_task(self):
    try:
        result = run_analysis()
        return result
    except Exception as exc:
        raise self.retry(exc=exc)