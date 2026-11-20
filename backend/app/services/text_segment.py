"""文本分块领域服务：将页面长文本切分为语义单元。"""
from __future__ import annotations

import hashlib
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


BLOCK_TYPES = ["paragraph", "heading", "list_item", "table_cell", "code_block", "quote"]

PENDING_MODULE = "pending_segment"


class TextSegmentService(BaseModuleService):
    MODULE = "text_segment"
    ENTITY = "文本块"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["块ID", "来源页面", "块类型", "起始位置", "结束位置", "字符长度", "语言标识"]
    REQUIRED_FIELDS = ["块ID", "来源页面", "块类型"]
    STATUS_ORDER = ["已切分", "已标注", "已入库", "已丢弃"]
    ACTION_RULES = {"标注语义": "已切分", "确认入库": "已标注", "丢弃无用": "已切分"}
    NEGATIVE_ACTIONS = []

    def segment_page(self, page_url: str, raw_html: str = "") -> dict[str, Any]:
        """模拟页面文本分块：从调度模块获得URL后切分为语义块。"""
        if not raw_html:
            raw_html = f"<html><body><h1>Page Title</h1><p>Content paragraph for {page_url}</p><ul><li>Item A</li><li>Item B</li></ul></body></html>"

        blocks = []
        positions = [("h1", 0, 50), ("p", 60, 300), ("li", 310, 350), ("li", 355, 395)]
        for i, (tag, start, end) in enumerate(positions):
            btype = "heading" if tag == "h1" else ("list_item" if tag == "li" else "paragraph")
            block_id = hashlib.md5(f"{page_url}|{tag}|{i}".encode()).hexdigest()[:8]
            char_len = end - start
            blocks.append({
                "块ID": block_id,
                "来源页面": page_url,
                "块类型": btype,
                "起始位置": str(start),
                "结束位置": str(end),
                "字符长度": str(char_len),
                "语言标识": "zh-CN",
                "text_sample": f"[{btype}] Content from {page_url} position {start}-{end}",
            })
        return {
            "ok": True,
            "page_url": page_url,
            "block_count": len(blocks),
            "total_chars": sum(int(b["字符长度"]) for b in blocks),
            "blocks": blocks,
        }

    def receive_page_receipt(
        self,
        task_id: int,
        page_url: str,
        *,
        verified: bool = True,
        receipt_id: str | None = None,
        raw_html: str = "",
    ) -> dict[str, Any]:
        """页面完成回执 + 验证结果 -> 待结算页面分块数据集。

        文本分块是页面文本的产出方：调度把回执交过来，这里切块后同步到
        待结算数据集；结算结果由调度回写到调度清单，两边共用同一份结算单号。
        """
        from app.services.feed_scheduler import FeedSchedulerService
        return FeedSchedulerService().receive_page_receipt(
            task_id, page_url, verified=verified, receipt_id=receipt_id, raw_html=raw_html,
        )

    def list_pending_blocks(
        self, task_id: int | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        """查看待结算页面分块，供停跑后的续跑归档核对。"""
        rows = store.rows(PENDING_MODULE)
        if task_id is not None:
            rows = [row for row in rows if int(row.get("任务ID", 0) or 0) == task_id]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        return rows

    def settle_page_blocks(self, task_id: int) -> dict[str, Any]:
        """结算某任务下已回传的待结算分块，并把结果回写到调度清单。"""
        from app.services.feed_scheduler import FeedSchedulerService
        return FeedSchedulerService().settle_pending(task_id)

    def feed_sentiment_tag(self, block_ids: list[str]) -> dict[str, Any]:
        """将文本块提供给情感标注进行情感分析。"""
        blocks = []
        for bid in block_ids:
            row = next((r for r in store.rows(self.MODULE) if r.get("块ID") == bid), None)
            if row:
                blocks.append({
                    "block_id": bid,
                    "source_page": row.get("来源页面"),
                    "block_type": row.get("块类型"),
                    "text_sample": row.get("text_sample", row.get("块ID", "")),
                })
        return {
            "ok": True,
            "input_blocks": len(block_ids),
            "matched_blocks": len(blocks),
            "blocks": blocks,
        }

    def compute_segment_stats(self) -> dict[str, Any]:
        """分块统计，供抽取指标消费。"""
        rows = store.rows(self.MODULE)
        type_dist: dict[str, int] = {}
        total_chars = 0
        for row in rows:
            btype = row.get("块类型", "unknown")
            type_dist[btype] = type_dist.get(btype, 0) + 1
            try:
                total_chars += int(row.get("字符长度", "0"))
            except (ValueError, TypeError):
                pass
        return {
            "total_blocks": len(rows),
            "total_chars": total_chars,
            "avg_chars_per_block": round(total_chars / len(rows)) if rows else 0,
            "block_type_distribution": type_dist,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["segment_stats"] = self.compute_segment_stats()
        return base