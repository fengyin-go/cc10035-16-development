"""种子调度领域服务：管理URL种子的批量爬取调度。

除了原来的任务级状态流转，这里补上了「暂停时结算在途页面」的完整链路：

* 每次开始/恢复调度都会开一个新的采集轮次（round）；
* 页面的派发、回执、结算都落在内部派发记录上，对外仍以调度清单的一组汇总量呈现；
* 暂停只停止派发，不丢弃已派发页面的回执；暂停边界之后到达的回执进入
  待结算页面分块数据集，等恢复时归档结算；
* 同一轮里暂停申请与回执同时到达时，以暂停序号为边界，回执顺延到恢复轮完成，
  结算明细会写明该页到底在第几轮算完成；
* 同一份派发记录只会接受第一份回执，重复回执不再生成结算数据。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.text_segment import TextSegmentService
from app.store import store


# 所有服务实例共享同一把锁：暂停和页面回执可能同时操作同一份派发记录。
_LOCK = threading.RLock()

PENDING_MODULE = "pending_segment"
ROUNDS_TABLE = "_scheduler_rounds"
DISPATCHES_TABLE = "_page_dispatches"
META_TABLE = "_scheduler_meta"

DISPATCH_DISPATCHED = "已派发"
DISPATCH_RECEIVED = "已回传"
DISPATCH_SETTLED = "已结算"
DISPATCH_HELD = "保留核对"
PENDING_STATUSES = ("待结算", "保留核对")


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _now() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


class FeedSchedulerService(BaseModuleService):
    MODULE = "feed_scheduler"
    ENTITY = "调度任务"
    OPERATION_TERM = "抓取"
    LIST_FIELDS = [
        "任务名称", "URL种子", "抓取深度", "爬取频率", "最大页面数", "域名限制", "优先级",
        "当前轮次", "页面处理状态", "已结算页面数", "待结算页面数",
    ]
    REQUIRED_FIELDS = ["任务名称", "URL种子", "抓取深度"]
    STATUS_ORDER = ["待调度", "执行中", "已暂停", "已完成", "部分失败"]
    # 该映射沿用基类约定：动作 -> 流转后的状态；开始/暂停/恢复由本类的领域流程接管。
    ACTION_RULES = {
        "开始抓取": "执行中",
        "暂停调度": "已暂停",
        "恢复调度": "执行中",
        "完成任务": "已完成",
        "标记失败": "部分失败",
    }
    NEGATIVE_ACTIONS = ["标记失败"]

    # ------------------------------------------------------------------ 基础工具
    def extract_urls_from_seed(self, seed_url: str, depth: int) -> dict[str, Any]:
        """从种子URL推导子页面列表，模拟爬取链路。"""
        urls = []
        seed_stripped = seed_url.rstrip("/")
        for d in range(1, depth + 1):
            for p in range(1, min(d * 3 + 1, 6)):
                urls.append(f"{seed_stripped}/page/{d}/{p}")
        return {
            "ok": True,
            "seed_url": seed_url,
            "depth": depth,
            "generated_url_count": len(urls),
            "urls": urls,
        }

    def feed_text_segmentation(self, task_id: int) -> dict[str, Any]:
        """调度url种子，输出给文本分块模块消费。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "reason": "调度任务不存在"}
        seed = task.get("URL种子", "")
        depth = _as_int(task.get("抓取深度"), 1) or 1

        url_result = self.extract_urls_from_seed(seed, min(depth, 3))
        pages = url_result["urls"]
        page_blocks = []
        for i, url in enumerate(pages):
            block_id = hashlib.md5(f"{url}|block".encode()).hexdigest()[:8]
            page_blocks.append({
                "url": url,
                "block_id": block_id,
                "block_types": ["paragraph", "heading", "list"],
                "estimated_chars": 2500 + (i * 500),
            })
        return {
            "ok": True,
            "task_id": task_id,
            "seed_url": seed,
            "total_pages": len(pages),
            "page_blocks": page_blocks,
        }

    def _next_seq(self) -> int:
        """轮次内的事件序号：暂停边界、回执先后都靠它判定。"""
        rows = store.rows(META_TABLE)
        if not rows:
            rows.append({"id": 1, "seq": 0})
        rows[0]["seq"] = _as_int(rows[0].get("seq"), 0) + 1
        return rows[0]["seq"]

    def _rounds(self, task_id: int) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(ROUNDS_TABLE)
            if _as_int(row.get("任务ID")) == task_id
        ]

    def _current_round(self, task_id: int) -> dict[str, Any] | None:
        rounds = self._rounds(task_id)
        return rounds[-1] if rounds else None

    def _dispatches(self, task_id: int) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(DISPATCHES_TABLE)
            if _as_int(row.get("任务ID")) == task_id
        ]

    def _find_dispatch(self, task_id: int, page_uri: str) -> dict[str, Any] | None:
        for dispatch in reversed(self._dispatches(task_id)):
            if dispatch.get("页面URI") == page_uri:
                return dispatch
        return None

    def _next_id(self, table: str) -> int:
        rows = store.rows(table)
        return max((_as_int(row.get("id")) for row in rows), default=0) + 1

    def _pages_for_task(self, task: dict[str, Any]) -> list[str]:
        seed = str(task.get("URL种子", "")).strip()
        depth = _as_int(task.get("抓取深度"), 1) or 1
        urls = self.extract_urls_from_seed(seed, min(depth, 3))["urls"]
        max_pages = _as_int(task.get("最大页面数"), 0)
        if max_pages > 0:
            urls = urls[:max_pages]
        return urls

    # ------------------------------------------------------------------ 调度清单回写
    def refresh_task_summary(self, task: dict[str, Any]) -> dict[str, Any]:
        """把派发/回执/结算结果回写到调度清单，并切换页面处理状态。"""
        task_id = _as_int(task.get("id"))
        dispatches = self._dispatches(task_id)
        current = self._current_round(task_id)
        pending_pages = [d for d in dispatches if d.get("状态") in (DISPATCH_RECEIVED, DISPATCH_HELD)]
        settled_pages = [d for d in dispatches if d.get("状态") == DISPATCH_SETTLED]
        returned_pages = [
            d for d in dispatches if d.get("状态") in (DISPATCH_RECEIVED, DISPATCH_SETTLED, DISPATCH_HELD)
        ]

        if task.get("status") == "已完成":
            page_state = "已完结"
        elif task.get("status") == "部分失败":
            page_state = "部分失败"
        elif task.get("status") == "已暂停" and pending_pages:
            page_state = "暂停在途"
        elif task.get("status") == "已暂停":
            page_state = "暂停停派"
        elif dispatches and len(settled_pages) == len(dispatches):
            page_state = "已结算"
        elif pending_pages:
            page_state = "结算在途"
        elif dispatches:
            page_state = "采集中"
        else:
            page_state = "未派发"

        task["当前轮次"] = str(current.get("轮次", "")) if current else ""
        task["派发页面数"] = len(dispatches)
        task["已回传页面数"] = len(returned_pages)
        task["已结算页面数"] = len(settled_pages)
        task["待结算页面数"] = len(pending_pages)
        task["页面处理状态"] = page_state
        return task

    # ------------------------------------------------------------------ 派发
    def _dispatch_pages(
        self, task: dict[str, Any], round_row: dict[str, Any], pages: list[str]
    ) -> list[dict[str, Any]]:
        """按轮次派发尚未派过的页面；暂停状态下一律不再派发新页面。"""
        task_id = _as_int(task.get("id"))
        known = {d.get("页面URI") for d in self._dispatches(task_id)}
        dispatched: list[dict[str, Any]] = []
        for url in pages:
            if task.get("status") == "已暂停":
                break
            if url in known:
                continue
            seq = self._next_seq()
            dispatch = {
                "id": self._next_id(DISPATCHES_TABLE),
                "任务ID": task_id,
                "任务名称": task.get("任务名称", ""),
                "轮次": round_row["轮次"],
                "页面URI": url,
                "状态": DISPATCH_DISPATCHED,
                "派发序号": seq,
                "回执序号": None,
                "回执轮次": None,
                "回执标识": "",
                "结算单号": "",
                "结算轮次": None,
                "完成轮次": None,
                "验证结果": "",
                "分块数": 0,
                "派发时间": _now(),
            }
            store.rows(DISPATCHES_TABLE).append(dispatch)
            known.add(url)
            dispatched.append(dispatch)
            round_row["派发页面数"] = _as_int(round_row.get("派发页面数")) + 1
        return dispatched

    # ------------------------------------------------------------------ 开始调度
    def start_scheduling(self, task_id: int) -> dict[str, Any]:
        """开始一轮调度：建轮次、派发页面。暂停中不允许开始。"""
        with _LOCK:
            task = store.find(self.MODULE, task_id)
            if task is None:
                return {"ok": False, "reason": "调度任务不存在"}
            if task.get("status") == "已暂停":
                return {"ok": False, "reason": "任务已暂停，暂停状态生效后不再派发新页面，请先恢复调度"}
            if task.get("status") in ("已完成", "部分失败"):
                return {
                    "ok": False,
                    "reason": f"任务已{task.get('status')}，如需重新采集请登记新的调度任务",
                }

            current = self._current_round(task_id)
            if current is not None and current.get("状态") == "执行中":
                self.refresh_task_summary(task)
                return {
                    "ok": True,
                    "already_running": True,
                    "task_id": task_id,
                    "round": current["轮次"],
                    "dispatched_pages": [
                        d["页面URI"] for d in self._dispatches(task_id)
                        if d.get("轮次") == current["轮次"]
                    ],
                    "message": f"任务已在第{current['轮次']}轮执行中，不重复派发",
                }

            round_no = (_as_int(current.get("轮次")) + 1) if current else 1
            round_row = {
                "id": self._next_id(ROUNDS_TABLE),
                "任务ID": task_id,
                "任务名称": task.get("任务名称", ""),
                "轮次": round_no,
                "状态": "执行中",
                "开始序号": self._next_seq(),
                "暂停序号": None,
                "结束序号": None,
                "派发页面数": 0,
                "开始时间": _now(),
                "暂停时间": "",
                "结束时间": "",
                "恢复自轮次": None,
            }
            store.rows(ROUNDS_TABLE).append(round_row)

            task["status"] = "执行中"
            task["暂停轮次"] = ""
            task.pop("abnormal", None)
            task["abnormal"] = False
            task["pending"] = True
            dispatched = self._dispatch_pages(task, round_row, self._pages_for_task(task))
            self.refresh_task_summary(task)
            return {
                "ok": True,
                "task_id": task_id,
                "round": round_no,
                "dispatched_pages": [d["页面URI"] for d in dispatched],
                "total_pages": len(dispatched),
                "message": f"第{round_no}轮调度已开始，派发{len(dispatched)}个页面",
            }

    # ------------------------------------------------------------------ 暂停
    def pause_scheduling(self, task_id: int, reason: str = "") -> dict[str, Any]:
        """暂停调度：记录暂停边界序号，之后不再派发新页面。"""
        with _LOCK:
            task = store.find(self.MODULE, task_id)
            if task is None:
                return {"ok": False, "reason": "调度任务不存在"}
            if task.get("status") == "已暂停":
                current = self._current_round(task_id)
                return {
                    "ok": True,
                    "already_paused": True,
                    "task_id": task_id,
                    "round": current.get("轮次") if current else None,
                    "pending_pages": _as_int(task.get("待结算页面数")),
                    "message": "任务已处于暂停状态",
                }

            current = self._current_round(task_id)
            if current is None or current.get("状态") != "执行中":
                return {"ok": False, "reason": "任务当前不在执行中，无法暂停"}

            pause_seq = self._next_seq()
            current["状态"] = "暂停中"
            current["暂停序号"] = pause_seq
            current["暂停原因"] = reason
            current["暂停时间"] = _now()
            task["status"] = "已暂停"
            task["暂停轮次"] = str(current["轮次"])
            task["pending"] = True
            self.refresh_task_summary(task)

            pending_blocks = sum(
                1 for row in store.rows(PENDING_MODULE)
                if _as_int(row.get("任务ID")) == task_id and row.get("status") in PENDING_STATUSES
            )
            return {
                "ok": True,
                "task_id": task_id,
                "round": current["轮次"],
                "pause_boundary_seq": pause_seq,
                "pending_pages": _as_int(task.get("待结算页面数")),
                "pending_blocks": pending_blocks,
                "message": f"第{current['轮次']}轮调度已暂停，暂停边界序号#{pause_seq}，不再派发新页面",
            }

    # ------------------------------------------------------------------ 页面回执
    @staticmethod
    def _settlement_no(task_id: int, dispatch: dict[str, Any]) -> str:
        digest = hashlib.md5(
            f"{task_id}|{dispatch.get('页面URI')}|{dispatch.get('轮次')}".encode()
        ).hexdigest()[:8]
        return f"JS{task_id}-R{dispatch.get('轮次')}-{digest}"

    def _receipt_explanation(
        self,
        *,
        dispatch_round: int,
        receipt_round: int,
        pause_conflict: bool,
        paused: bool,
        pause_seq: int | None,
        receipt_seq: int,
        verification_passed: bool,
        completion_round: int | None,
    ) -> str:
        if not verification_passed:
            return (
                f"第{dispatch_round}轮派发，第{receipt_round}轮回执；"
                f"验证结果未通过，原文保留至恢复后人工核对。"
            )
        if pause_conflict:
            if completion_round is None:
                return (
                    f"第{dispatch_round}轮派发；暂停申请与分块回执同轮到达，"
                    f"回执（#{receipt_seq}）晚于暂停边界（#{pause_seq}），"
                    f"不计入第{dispatch_round}轮，待恢复轮核对后结算。"
                )
            return (
                f"第{dispatch_round}轮派发；暂停申请与分块回执同轮到达，"
                f"回执（#{receipt_seq}）晚于暂停边界（#{pause_seq}），"
                f"不计入第{dispatch_round}轮，恢复后在第{completion_round}轮结算，"
                f"故该页在第{completion_round}轮才算完成。"
            )
        if completion_round is None:
            return (
                f"第{dispatch_round}轮派发，第{receipt_round}轮回执；"
                f"任务处于暂停状态，回执先归档待结算，待恢复轮核对后结算。"
            )
        if receipt_round != dispatch_round:
            return (
                f"第{dispatch_round}轮派发，第{receipt_round}轮回执，"
                f"第{completion_round}轮结算完成（跨轮交付）。"
            )
        return f"第{dispatch_round}轮派发并在第{completion_round}轮结算完成。"

    def receive_page_receipt(
        self,
        task_id: int,
        page_uri: str,
        *,
        verified: bool = True,
        receipt_id: str | None = None,
        raw_html: str = "",
    ) -> dict[str, Any]:
        """接收页面完成回执与验证结果，把文本块同步到待结算分块数据集。

        同一派发记录只接受第一份回执；重复回执直接拒绝，不再生成结算数据。
        暂停边界之后到达的回执只归档为待结算，等恢复轮再结算。
        """
        with _LOCK:
            task = store.find(self.MODULE, task_id)
            if task is None:
                return {"ok": False, "reason": "调度任务不存在"}

            dispatch = self._find_dispatch(task_id, page_uri)
            if dispatch is None:
                return {"ok": False, "reason": f"页面未在任务 {task_id} 中派发：{page_uri}"}

            state = dispatch.get("状态")
            if state in (DISPATCH_RECEIVED, DISPATCH_SETTLED, DISPATCH_HELD) or dispatch.get("回执序号"):
                return {
                    "ok": False,
                    "accepted": False,
                    "duplicate": True,
                    "task_id": task_id,
                    "page_uri": page_uri,
                    "settlement_no": dispatch.get("结算单号"),
                    "settlement_status": state,
                    "completion_round": dispatch.get("完成轮次"),
                    "block_count": dispatch.get("分块数", 0),
                    "reason": "重复回执：该页面已有一项结算请求，本次回执不再结算，原有文本保持不变",
                }

            if task.get("status") not in ("执行中", "已暂停"):
                return {
                    "ok": False,
                    "accepted": False,
                    "reason": f"任务当前状态为{task.get('status')}，不接受页面回执",
                }

            if receipt_id:
                for other in self._dispatches(task_id):
                    if other.get("回执标识") == receipt_id:
                        return {
                            "ok": False,
                            "accepted": False,
                            "duplicate": True,
                            "task_id": task_id,
                            "page_uri": page_uri,
                            "reason": "重复回执标识：该回执已用于其他页面，本次不再结算",
                        }

            current = self._current_round(task_id)
            if current is None:
                return {"ok": False, "reason": "任务尚无采集轮次，请先开始调度"}

            receipt_seq = self._next_seq()
            dispatch_round = _as_int(dispatch.get("轮次"))
            receipt_round = _as_int(current.get("轮次"))
            pause_seq = current.get("暂停序号")
            pause_conflict = (
                task.get("status") == "已暂停"
                and pause_seq is not None
                and receipt_seq > _as_int(pause_seq)
                and receipt_round == dispatch_round
            )

            segment = TextSegmentService().segment_page(page_uri, raw_html)
            blocks = segment.get("blocks", []) if segment.get("ok") else []
            verification_passed = bool(verified and blocks)
            verification = "通过" if verification_passed else "未通过"

            settle_now = (
                task.get("status") == "执行中"
                and verification_passed
            )
            completion_round = receipt_round if settle_now else None
            if not verification_passed:
                settlement_status = "保留核对"
                dispatch_status = DISPATCH_HELD
            elif settle_now:
                settlement_status = "已结算"
                dispatch_status = DISPATCH_SETTLED
            else:
                settlement_status = "待结算"
                dispatch_status = DISPATCH_RECEIVED

            settlement_no = self._settlement_no(task_id, dispatch)
            explanation = self._receipt_explanation(
                dispatch_round=dispatch_round,
                receipt_round=receipt_round,
                pause_conflict=pause_conflict,
                paused=task.get("status") == "已暂停",
                pause_seq=_as_int(pause_seq) if pause_seq is not None else None,
                receipt_seq=receipt_seq,
                verification_passed=verification_passed,
                completion_round=completion_round,
            )

            for block in blocks:
                row = {
                    "id": self._next_id(PENDING_MODULE),
                    "status": settlement_status,
                    "pending": settlement_status in PENDING_STATUSES,
                    "abnormal": not verification_passed,
                    "任务ID": task_id,
                    "关联任务": task.get("任务名称", ""),
                    "结算单号": settlement_no,
                    "派发轮次": str(dispatch_round),
                    "回执轮次": str(receipt_round),
                    "完成轮次": str(completion_round) if completion_round is not None else "",
                    "页面URI": page_uri,
                    "分块ID": block.get("块ID", ""),
                    "块类型": block.get("块类型", ""),
                    "字符长度": block.get("字符长度", ""),
                    "验证结果": verification,
                    "完成轮次说明": explanation,
                    "回执标识": receipt_id or f"auto-{dispatch['id']}-{receipt_seq}",
                    "回执序号": receipt_seq,
                    "分块文本": block.get("text_sample", ""),
                    "结算时间": _now() if settle_now else "",
                }
                store.rows(PENDING_MODULE).append(row)

            dispatch.update({
                "状态": dispatch_status,
                "回执序号": receipt_seq,
                "回执轮次": receipt_round,
                "回执标识": receipt_id or f"auto-{dispatch['id']}-{receipt_seq}",
                "结算单号": settlement_no,
                "结算轮次": completion_round,
                "完成轮次": completion_round,
                "验证结果": verification,
                "分块数": len(blocks),
            })
            self.refresh_task_summary(task)

            return {
                "ok": True,
                "accepted": True,
                "duplicate": False,
                "task_id": task_id,
                "page_uri": page_uri,
                "settlement_no": settlement_no,
                "settlement_status": settlement_status,
                "block_count": len(blocks),
                "verified": verification_passed,
                "settled": settle_now,
                "pause_conflict": pause_conflict,
                "dispatch_round": dispatch_round,
                "receipt_round": receipt_round,
                "completion_round": completion_round,
                "pause_boundary_seq": _as_int(pause_seq) if pause_seq is not None else None,
                "receipt_seq": receipt_seq,
                "explanation": explanation,
                "message": (
                    f"页面分块已同步至待结算数据集，第{completion_round}轮完成"
                    if completion_round is not None
                    else f"页面分块已归档为{settlement_status}，等待恢复后核对结算"
                ),
            }

    # ------------------------------------------------------------------ 结算
    def _settle_dispatch(self, dispatch: dict[str, Any], round_no: int) -> int:
        """把某一派发记录名下的待结算分块归档到指定轮次。"""
        settlement_no = dispatch.get("结算单号")
        count = 0
        for row in store.rows(PENDING_MODULE):
            if row.get("结算单号") != settlement_no or row.get("status") != "待结算":
                continue
            row["status"] = "已结算"
            row["pending"] = False
            row["abnormal"] = False
            row["完成轮次"] = str(round_no)
            row["结算时间"] = _now()
            suffix = (
                f" 恢复后在第{round_no}轮结算，故该页在第{round_no}轮才算完成。"
                if dispatch.get("回执轮次") != round_no
                else f" 第{round_no}轮核对结算完成。"
            )
            row["完成轮次说明"] = str(row.get("完成轮次说明", "")).rstrip() + suffix
            count += 1
        dispatch["状态"] = DISPATCH_SETTLED
        dispatch["结算轮次"] = round_no
        dispatch["完成轮次"] = round_no
        return count

    def settle_pending(self, task_id: int, page_uri: str | None = None) -> dict[str, Any]:
        """结算当前任务下已回传且待结算的页面（恢复流程也会自动调用）。

        指定 page_uri 时只结算该页，用于待结算明细上的单条确认结算。
        """
        with _LOCK:
            task = store.find(self.MODULE, task_id)
            if task is None:
                return {"ok": False, "reason": "调度任务不存在"}
            current = self._current_round(task_id)
            if current is None:
                return {"ok": False, "reason": "任务尚无采集轮次"}
            if current.get("状态") != "执行中":
                return {
                    "ok": False,
                    "reason": (
                        f"第{current.get('轮次')}轮已暂停，待结算分块只在恢复后的轮次结算；"
                        f"请先恢复调度"
                    ),
                }
            settled_pages: list[str] = []
            blocks = 0
            for dispatch in self._dispatches(task_id):
                if dispatch.get("状态") != DISPATCH_RECEIVED:
                    continue
                if page_uri is not None and dispatch.get("页面URI") != page_uri:
                    continue
                blocks += self._settle_dispatch(dispatch, _as_int(current.get("轮次")))
                settled_pages.append(dispatch.get("页面URI", ""))
            self.refresh_task_summary(task)
            return {
                "ok": True,
                "task_id": task_id,
                "round": current.get("轮次"),
                "settled_pages": settled_pages,
                "settled_blocks": blocks,
                "pending_pages": _as_int(task.get("待结算页面数")),
            }

    # ------------------------------------------------------------------ 恢复
    def resume_scheduling(self, task_id: int) -> dict[str, Any]:
        """恢复调度：结束暂停轮次，开新轮次，先归档在途回执，再派发未派过的新页面。"""
        with _LOCK:
            task = store.find(self.MODULE, task_id)
            if task is None:
                return {"ok": False, "reason": "调度任务不存在"}
            if task.get("status") == "执行中":
                current = self._current_round(task_id)
                return {
                    "ok": True,
                    "already_running": True,
                    "task_id": task_id,
                    "round": current.get("轮次") if current else None,
                    "message": "任务已在执行中，无需恢复",
                }
            if task.get("status") != "已暂停":
                return {"ok": False, "reason": f"任务当前状态为{task.get('status')}，无法恢复"}

            current = self._current_round(task_id)
            if current is None:
                return {"ok": False, "reason": "任务尚无采集轮次"}
            old_round = _as_int(current.get("轮次"))
            current["状态"] = "已结束"
            current["结束序号"] = self._next_seq()
            current["结束时间"] = _now()

            new_round_no = old_round + 1
            round_row = {
                "id": self._next_id(ROUNDS_TABLE),
                "任务ID": task_id,
                "任务名称": task.get("任务名称", ""),
                "轮次": new_round_no,
                "状态": "执行中",
                "开始序号": self._next_seq(),
                "暂停序号": None,
                "结束序号": None,
                "派发页面数": 0,
                "开始时间": _now(),
                "暂停时间": "",
                "结束时间": "",
                "恢复自轮次": old_round,
            }
            store.rows(ROUNDS_TABLE).append(round_row)

            task["status"] = "执行中"
            task["暂停轮次"] = ""
            task["pending"] = True

            # 仍在途的已回传页面在恢复轮归档结算；未回传的页面保持原派发轮次继续等回执。
            settled_pages: list[str] = []
            settled_blocks = 0
            for dispatch in self._dispatches(task_id):
                if dispatch.get("状态") != DISPATCH_RECEIVED:
                    continue
                settled_blocks += self._settle_dispatch(dispatch, new_round_no)
                settled_pages.append(dispatch.get("页面URI", ""))

            dispatched = self._dispatch_pages(task, round_row, self._pages_for_task(task))
            self.refresh_task_summary(task)
            return {
                "ok": True,
                "task_id": task_id,
                "resumed_from_round": old_round,
                "round": new_round_no,
                "settled_pages": settled_pages,
                "settled_blocks": settled_blocks,
                "dispatched_pages": [d["页面URI"] for d in dispatched],
                "pending_pages": _as_int(task.get("待结算页面数")),
                "message": (
                    f"任务已从第{old_round}轮恢复为第{new_round_no}轮，"
                    f"归档{len(settled_pages)}个在途页面、{settled_blocks}个文本块"
                ),
            }

    # ------------------------------------------------------------------ 轮次汇总与明细
    def compute_round_summary(self, task_id: int) -> dict[str, Any]:
        """按轮次核对页面与文本块数量。

        完成口径以结算轮次为准：暂停边界后回传的页面即使在第1轮到达，
        也只有在恢复轮结算后才算该轮完成，因此两轮的「完成页面数/完成分块数」
        不会重复计数。
        """
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "reason": "调度任务不存在"}

        dispatches = self._dispatches(task_id)
        rows = [
            row for row in store.rows(PENDING_MODULE)
            if _as_int(row.get("任务ID")) == task_id
        ]
        summary_rows: list[dict[str, Any]] = []
        for round_row in self._rounds(task_id):
            round_no = _as_int(round_row.get("轮次"))
            round_dispatches = [d for d in dispatches if _as_int(d.get("轮次")) == round_no]
            round_receipts = [d for d in dispatches if _as_int(d.get("回执轮次")) == round_no]
            completed_rows = [
                row for row in rows
                if _as_int(row.get("完成轮次")) == round_no and row.get("status") == "已结算"
            ]
            pending_rows = [
                row for row in rows
                if row.get("status") in PENDING_STATUSES and _as_int(row.get("回执轮次")) == round_no
            ]
            summary_rows.append({
                "round": round_no,
                "status": round_row.get("状态"),
                "恢复自轮次": round_row.get("恢复自轮次"),
                "暂停边界序号": round_row.get("暂停序号"),
                "派发页面数": len(round_dispatches),
                "回执页面数": len(round_receipts),
                "完成页面数": len({row.get("页面URI") for row in completed_rows}),
                "完成分块数": len(completed_rows),
                "待结算页面数": len({row.get("页面URI") for row in pending_rows}),
                "待结算分块数": len(pending_rows),
                "派发页面": [d.get("页面URI") for d in round_dispatches],
                "回执页面": [d.get("页面URI") for d in round_receipts],
                "完成页面": sorted({row.get("页面URI") for row in completed_rows}),
            })

        settled_rows = [row for row in rows if row.get("status") == "已结算"]
        pending_rows = [row for row in rows if row.get("status") in PENDING_STATUSES]
        return {
            "ok": True,
            "task_id": task_id,
            "task_name": task.get("任务名称"),
            "task_status": task.get("status"),
            "current_round": _as_int(task.get("当前轮次")) if task.get("当前轮次") else None,
            "rounds": summary_rows,
            "totals": {
                "dispatched_pages": len(dispatches),
                "settled_pages": len({r.get("页面URI") for r in settled_rows}),
                "settled_blocks": len(settled_rows),
                "unsettled_pages": len({r.get("页面URI") for r in pending_rows}),
                "unsettled_blocks": len(pending_rows),
            },
        }

    def settlement_detail(self, entry_id: int) -> dict[str, Any]:
        """解释某条待结算分块属于哪一轮派发、哪一轮回执、哪一轮才算完成。"""
        entry = store.find(PENDING_MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"待结算分块 {entry_id} 不存在"}
        return self.settlement_detail_for_row(entry)

    def settlement_detail_for_row(self, entry: dict[str, Any]) -> dict[str, Any]:
        task_id = _as_int(entry.get("任务ID"))
        dispatch = next(
            (d for d in self._dispatches(task_id) if d.get("结算单号") == entry.get("结算单号")),
            None,
        )
        dispatch_round = _as_int(entry.get("派发轮次"))
        receipt_round = _as_int(entry.get("回执轮次"))
        round_row = next(
            (r for r in self._rounds(task_id) if _as_int(r.get("轮次")) == dispatch_round),
            None,
        )
        pause_seq = round_row.get("暂停序号") if round_row else None
        receipt_seq = entry.get("回执序号")
        pause_conflict = bool(
            pause_seq is not None
            and receipt_seq is not None
            and _as_int(receipt_seq) > _as_int(pause_seq)
            and receipt_round == dispatch_round
        )
        return {
            "ok": True,
            "entry_id": entry.get("id"),
            "settlement_no": entry.get("结算单号"),
            "task_id": task_id,
            "task_name": entry.get("关联任务"),
            "page_uri": entry.get("页面URI"),
            "block_id": entry.get("分块ID"),
            "settlement_status": entry.get("status"),
            "verification": entry.get("验证结果"),
            "dispatch_round": dispatch_round,
            "receipt_round": receipt_round,
            "completion_round": _as_int(entry.get("完成轮次")) if entry.get("完成轮次") else None,
            "pause_boundary_seq": pause_seq,
            "receipt_seq": receipt_seq,
            "same_round_pause_conflict": pause_conflict,
            "dispatch_seq": dispatch.get("派发序号") if dispatch else None,
            "explanation": entry.get("完成轮次说明", ""),
        }

    # ------------------------------------------------------------------ 统计
    def compute_scheduler_stats(self) -> dict[str, Any]:
        """调度统计指标。"""
        rows = store.rows(self.MODULE)
        completed = sum(1 for r in rows if r.get("status") == "已完成")
        failed = sum(1 for r in rows if r.get("status") == "部分失败")
        in_progress = sum(1 for r in rows if r.get("status") == "执行中")
        paused = sum(1 for r in rows if r.get("status") == "已暂停")
        total_depth = 0
        for r in rows:
            total_depth += _as_int(r.get("抓取深度"))
        return {
            "total_tasks": len(rows),
            "completed": completed,
            "failed": failed,
            "in_progress": in_progress,
            "paused": paused,
            "avg_depth": round(total_depth / len(rows), 1) if rows else 0.0,
            "success_ratio": round(completed / len(rows), 2) if rows else 0.0,
        }

    # ------------------------------------------------------------------ 通用动作
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """通用动作入口：暂停/恢复/开始走领域流程，其余沿用基类。"""
        if action in ("开始抓取", "暂停调度", "恢复调度"):
            if action == "开始抓取":
                result = self.start_scheduling(entry_id)
            elif action == "暂停调度":
                result = self.pause_scheduling(entry_id)
            else:
                result = self.resume_scheduling(entry_id)
            entry = store.find(self.MODULE, entry_id)
            if not result.get("ok"):
                return None, result.get("reason", "动作执行失败")
            return entry, result.get("message", f"{self.ENTITY}已{action}")
        if action in ("完成任务", "标记失败"):
            entry, message = super().run_action(entry_id, action)
            if entry is not None:
                current = self._current_round(entry_id)
                if current is not None and current.get("状态") in ("执行中", "暂停中"):
                    current["状态"] = "已结束"
                    current["结束序号"] = self._next_seq()
                    current["结束时间"] = _now()
                entry["页面处理状态"] = "任务已结束"
                self.refresh_task_summary(entry)
            return entry, message
        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["scheduler_stats"] = self.compute_scheduler_stats()
        base["inflight"] = {
            "pending_pages": sum(1 for d in store.rows(DISPATCHES_TABLE) if d.get("状态") == DISPATCH_RECEIVED),
            "held_pages": sum(1 for d in store.rows(DISPATCHES_TABLE) if d.get("状态") == DISPATCH_HELD),
            "rounds": len(store.rows(ROUNDS_TABLE)),
        }
        return base
