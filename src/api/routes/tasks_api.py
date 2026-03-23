from scripts.tasks import run_ingestion
from scripts.tasks import run_analysis_task
from src.api.celery_app import app as celery_app
from fastapi import APIRouter

router = APIRouter(prefix="/tasks", tags=["tasks"])

@router.post("/ingest")
def trigger_ingestion():
    response = run_ingestion.delay()
    return {"task_id": response.id, "status": "Ingestion task triggered"}

@router.post("/analyze")
def trigger_analysis():
    response = run_analysis_task.delay()
    return {"task_id": response.id, "status": "Analysis task triggered"}

@router.get("/{task_id}")
def get_task_status(task_id: str):
    result = celery_app.AsyncResult(task_id)
    return {"task_id": task_id, "status": result.status, "result": result.result if result.ready() else None}