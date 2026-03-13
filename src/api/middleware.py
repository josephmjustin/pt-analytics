import logging
import time
from fastapi import Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pt_analytics_api")

async def log_requests(request: Request, call_next):
    start_time = time.time()
    try:
        response = await call_next(request)
    except Exception as e:
        logger.error(f"{request.method} {request.url} failed: {e}", exc_info=True)
        raise
    process_time = time.time() - start_time
    logger.info(f"{request.method} {request.url} completed in {process_time:.2f}s with status {response.status_code}")
    return response

async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "Something went wrong", "type": type(exc).__name__}
    )