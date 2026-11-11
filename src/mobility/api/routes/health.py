"""Health and readiness endpoints."""

from fastapi import APIRouter, HTTPException, status

from mobility.analytics.database import get_analytics_client

router = APIRouter(tags=["Health"])


@router.get("/health")
def health_check() -> dict[str, str]:
    """Check whether the API process is running."""
    return {"status": "healthy"}


@router.get("/ready")
def readiness_check() -> dict[str, str]:
    """Check whether ClickHouse is reachable."""
    try:
        is_connected = get_analytics_client().ping()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ClickHouse is unavailable.",
        ) from exc

    if not is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ClickHouse is unavailable.",
        )

    return {"status": "ready"}
