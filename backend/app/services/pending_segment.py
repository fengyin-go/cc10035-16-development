"""待结算页面分块领域服务：承接暂停前已派发页面的回执与文本块。

停跑（暂停）之后，已派出的页面仍可能交付回执。这些回执对应的文本块先落到
本数据集，状态为「待结算」或「保留核对」；恢复调度时由种子调度服务结算，
结算结果回写到调度清单，并在这里留下「完成轮次」和一句可核对的说明。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class PendingSegmentService(BaseModuleService):
    MODULE = "pending_segment"
    ENTITY = "待结算分块"
    OPERATION_TERM = "结算"
    LIST_FIELDS = [
        "结算单号", "关联任务", "页面URI", "分块ID", "块类型", "分块文本",
        "派发轮次", "回执轮次", "完成轮次", "验证结果", "完成轮次说明",
    ]
    REQUIRED_FIELDS = ["结算单号", "页面URI", "分块ID"]
    STATUS_ORDER = ["待结算", "已结算", "保留核对"]
    # 动作 -> 流转后的状态；「确认结算」由本类接管，因为要联动调度清单回写。
    # 已结算的记录只允许「恢复归档」回到待结算，其余动作不再受理。
    ACTION_RULES = {"保留核对": "保留核对", "恢复归档": "待结算"}
    NEGATIVE_ACTIONS = ["保留核对"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "确认结算":
            return self.settle_entry(entry_id)
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in (*self.ACTION_RULES, "确认结算"):
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        if entry.get("status") == "已结算" and action != "恢复归档":
            return None, "该分块已结算，重复结算请求不再受理"
        result = super().run_action(entry_id, action)
        entry, _ = result
        if entry is not None:
            # 「保留核对」是待人工处理的终点态，仍算待办；只有已结算才算办结。
            entry["pending"] = entry.get("status") != "已结算"
            entry["abnormal"] = entry.get("验证结果") != "通过"
            if action == "恢复归档":
                self._sync_dispatch_status(entry, "已回传")
            elif action == "保留核对":
                self._sync_dispatch_status(entry, "保留核对")
        return result

    @staticmethod
    def _sync_dispatch_status(entry: dict[str, Any], dispatch_status: str) -> None:
        """明细上的状态变更同步回派发记录，避免两边对同一页给出不同结论。"""
        for dispatch in store.rows("_page_dispatches"):
            if dispatch.get("结算单号") == entry.get("结算单号") and dispatch.get("状态") != "已结算":
                dispatch["状态"] = dispatch_status

    def _task_id_for(self, entry: dict[str, Any]) -> int:
        try:
            return int(entry.get("任务ID", 0) or 0)
        except (TypeError, ValueError):
            return 0

    def settle_entry(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """确认结算：交给种子调度统一结算，保证调度清单同步回写。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry.get("status") == "已结算":
            return None, "该分块已结算，重复结算请求不再受理"
        if entry.get("status") != "待结算":
            return None, f"分块当前状态为{entry.get('status')}，需先恢复归档后才能结算"

        from app.services.feed_scheduler import FeedSchedulerService
        result = FeedSchedulerService().settle_pending(
            self._task_id_for(entry), page_uri=entry.get("页面URI"),
        )
        if not result.get("ok"):
            return None, result.get("reason", "结算失败")
        return entry, (
            f"已结算，归属第{entry.get('完成轮次') or result.get('round')}轮；"
            f"本次共结算{result.get('settled_blocks', 0)}个文本块"
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "确认结算":
            return self.settle_entry(entry_id)
        entry = store.find(self.MODULE, entry_id)
        if entry is not None and entry.get("status") == "已结算" and action != "恢复归档":
            return None, "该分块已结算，不能再变更结算状态"
        return super().run_action(entry_id, action)

    def detail(self, entry_id: int) -> dict[str, Any]:
        """结算明细：说明该页在哪一轮派发、哪一轮回执、哪一轮才算完成。"""
        from app.services.feed_scheduler import FeedSchedulerService
        return FeedSchedulerService().settlement_detail(entry_id)

    def compute_pending_stats(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        settled = [r for r in rows if r.get("status") == "已结算"]
        pending = [r for r in rows if r.get("status") == "待结算"]
        held = [r for r in rows if r.get("status") == "保留核对"]
        return {
            "total_blocks": len(rows),
            "settled_blocks": len(settled),
            "pending_blocks": len(pending),
            "held_blocks": len(held),
            "pages": len({
                (r.get("任务ID"), r.get("页面URI"))
                for r in rows if r.get("status") in ("待结算", "保留核对")
            }),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["pending_stats"] = self.compute_pending_stats()
        return base
