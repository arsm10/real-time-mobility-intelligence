"""FastAPI application entry point."""

from fastapi import FastAPI

from mobility.api.routes.analytics import router as analytics_router
from mobility.api.routes.health import router as health_router

app = FastAPI(
    title="Real-Time Mobility Intelligence API",
    description="REST API for public transport mobility analytics.",
    version="0.1.0",
)

app.include_router(health_router)
app.include_router(analytics_router)
