from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from src.api.routes import stops, vehicles, routes, dwell_time
from src.api.database import create_pool, close_pool

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_pool()
    yield
    await close_pool()

app = FastAPI(title="Passenger Activity Analytics API", version="1.0.0", lifespan=lifespan)

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

@app.get("/")
def root():
    return {
        "message": "Passenger Activity Analytics API - Dwell Time Analysis",
        "version": "1.0.0",
        "features": ["demand_proxy", "temporal_patterns", "hotspot_detection"]
    }