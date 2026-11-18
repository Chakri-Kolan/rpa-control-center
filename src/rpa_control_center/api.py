from __future__ import annotations

import hmac
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status

from .database import QueueStore
from .logging_config import configure_logging
from .models import CompleteRequest, EnqueueRequest, FailRequest, LeaseRequest
from .settings import Settings

logger = logging.getLogger("rpa_control_center.api")


def create_app(settings: Settings | None = None) -> FastAPI:
    active_settings = settings or Settings.from_env()
    store = QueueStore(active_settings.database_path)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        configure_logging()
        store.initialize()
        yield

    app = FastAPI(
        title="RPA Control Center",
        version="0.1.0",
        description="Local work-queue API for UiPath, Power Automate, and custom robots.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.settings = active_settings

    @app.middleware("http")
    async def request_logging(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        started = time.perf_counter()
        response = await call_next(request)
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request_completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        return response

    def authorize(x_api_token: str | None = Header(default=None)) -> None:
        expected = active_settings.api_token
        if expected and (x_api_token is None or not hmac.compare_digest(x_api_token, expected)):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid API token"
            )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v1/items", dependencies=[Depends(authorize)])
    def enqueue(item: EnqueueRequest, response: Response) -> dict[str, Any]:
        stored, created = store.enqueue(**item.model_dump())
        response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return {"created": created, "item": stored}

    @app.post("/api/v1/queues/{queue_name}/lease", dependencies=[Depends(authorize)])
    def lease(queue_name: str, lease_request: LeaseRequest, response: Response) -> dict[str, Any]:
        item = store.lease(queue_name=queue_name, **lease_request.model_dump())
        if item is None:
            response.status_code = status.HTTP_204_NO_CONTENT
            return {}
        return {"item": item}

    @app.post("/api/v1/items/{item_id}/complete", dependencies=[Depends(authorize)])
    def complete(item_id: str, completion: CompleteRequest) -> dict[str, Any]:
        try:
            return {"item": store.complete(item_id=item_id, **completion.model_dump())}
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/v1/items/{item_id}/fail", dependencies=[Depends(authorize)])
    def fail(item_id: str, failure: FailRequest) -> dict[str, Any]:
        try:
            return {"item": store.fail(item_id=item_id, **failure.model_dump())}
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/v1/items/{item_id}", dependencies=[Depends(authorize)])
    def get_item(item_id: str) -> dict[str, Any]:
        item = store.get(item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="work item not found")
        return {"item": item}

    @app.get("/api/v1/items/{item_id}/audit", dependencies=[Depends(authorize)])
    def audit(item_id: str) -> dict[str, Any]:
        if store.get(item_id) is None:
            raise HTTPException(status_code=404, detail="work item not found")
        return {"events": store.audit(item_id)}

    @app.get("/api/v1/metrics", dependencies=[Depends(authorize)])
    def metrics() -> dict[str, Any]:
        return store.metrics()

    return app


app = create_app()
