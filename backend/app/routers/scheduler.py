"""调度结算接口：暂停/恢复、页面回执、轮次核对与结算明细。

这些是种子调度和待结算页面分块两个模块的真实业务差异，通用资源路由表达不了，
所以单独挂一组固定路径接口；路径段数多于 /api/{module}/{entry_id}，不会抢匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.feed_scheduler import FeedSchedulerService
from app.services.pending_segment import PendingSegmentService

router = APIRouter(prefix="/api", tags=["调度结算"])


def _scheduler() -> FeedSchedulerService:
    service = service_for("feed_scheduler")
    if not isinstance(service, FeedSchedulerService):
        raise HTTPException(status_code=500, detail="种子调度服务未注册")
    return service


def _pending() -> PendingSegmentService:
    service = service_for("pending_segment")
    if not isinstance(service, PendingSegmentService):
        raise HTTPException(status_code=500, detail="待结算分块服务未注册")
    return service


class ReceiptPayload(BaseModel):
    page_uri: str
    verified: bool = True
    receipt_id: str | None = None
    raw_html: str = ""


class PausePayload(BaseModel):
    reason: str = ""


class RoundSummaryResult(BaseModel):
    ok: bool
    task_id: int
    task_name: str | None = None
    task_status: str | None = None
    current_round: int | None = None
    rounds: list[dict[str, Any]] = Field(default_factory=list)
    totals: dict[str, Any] = Field(default_factory=dict)


@router.post("/feed_scheduler/{task_id}/start")
def start_scheduling(task_id: int) -> dict[str, Any]:
    return _scheduler().start_scheduling(task_id)


@router.post("/feed_scheduler/{task_id}/pause")
def pause_scheduling(task_id: int, payload: PausePayload | None = None) -> dict[str, Any]:
    return _scheduler().pause_scheduling(task_id, (payload.reason if payload else ""))


@router.post("/feed_scheduler/{task_id}/resume")
def resume_scheduling(task_id: int) -> dict[str, Any]:
    return _scheduler().resume_scheduling(task_id)


@router.post("/feed_scheduler/{task_id}/receipts")
def receive_page_receipt(task_id: int, payload: ReceiptPayload) -> dict[str, Any]:
    return _scheduler().receive_page_receipt(
        task_id,
        payload.page_uri,
        verified=payload.verified,
        receipt_id=payload.receipt_id,
        raw_html=payload.raw_html,
    )


@router.post("/feed_scheduler/{task_id}/settle")
def settle_pending(task_id: int) -> dict[str, Any]:
    return _scheduler().settle_pending(task_id)


@router.get("/feed_scheduler/{task_id}/rounds", response_model=RoundSummaryResult)
def round_summary(task_id: int) -> dict[str, Any]:
    return _scheduler().compute_round_summary(task_id)


@router.get("/pending_segment/{entry_id}/detail")
def pending_detail(entry_id: int) -> dict[str, Any]:
    return _pending().detail(entry_id)
