from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded
from src.api.routes import stops, vehicles, routes, dwell_time, admin
from src.api.database import create_pool, close_pool
from src.api.middleware import log_requests, global_exception_handler
from .rate_limiter import limiter

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_pool()
    yield
    await close_pool()

app = FastAPI(title="Passenger Activity Analytics API", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.middleware("http")(log_requests)
app.exception_handler(Exception)(global_exception_handler)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://josephmjustin.github.io",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)

# Include routers
app.include_router(stops.router)
app.include_router(vehicles.router)
app.include_router(routes.router)
app.include_router(dwell_time.router)
app.include_router(admin.router)

@app.get("/")
def root():
    return {
        "message": "Passenger Activity Analytics API - Dwell Time Analysis",
        "version": "1.0.0",
        "features": ["demand_proxy", "temporal_patterns", "hotspot_detection"]
    }