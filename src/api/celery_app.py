from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

app = Celery('pt_analytics', broker=os.getenv('CELERY_BROKER_URL'), backend=os.getenv('CELERY_RESULT_BACKEND'), include=['scripts.tasks'])

app.conf.beat_schedule = {
    'ingest-every-10-seconds': {
        'task': 'scripts.tasks.run_ingestion',
        'schedule': 10.0,
    },
    'run-analysis-every-600-seconds': {
        'task': 'scripts.tasks.run_analysis_task',
        'schedule': 600.0,
    },
}

app.conf.update(
    result_expires=3600,
)