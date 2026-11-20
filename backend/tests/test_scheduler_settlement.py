"""种子调度暂停结算的回归测试。

覆盖：暂停边界与同轮回执冲突、重复回执幂等、暂停期间停止派发、
恢复轮归档结算、按轮次核对页面与文本块数量。

运行：backend/.venv/bin/python -m unittest discover -s backend/tests -v
（内存仓库是进程级单例，每个用例开工前重置。）
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.feed_scheduler import FeedSchedulerService  # noqa: E402
from app.services.pending_segment import PendingSegmentService  # noqa: E402
from app.store import store  # noqa: E402

TASK_ID = 4
PAGE_A = "https://example.com/news/page/1/1"
PAGE_B = "https://example.com/news/page/1/2"


class SchedulerSettlementTest(unittest.TestCase):
    def setUp(self) -> None:
        store._tables.clear()
        store._tables.update(
            {name: [dict(row) for row in rows] for name, rows in __import__(
                "app.seed", fromlist=["SEED_ROWS"]).SEED_ROWS.items()}
        )
        store._tables.pop("_scheduler_rounds", None)
        store._tables.pop("_page_dispatches", None)
        store._tables.pop("_scheduler_meta", None)
        self.scheduler = FeedSchedulerService()
        self.pending = PendingSegmentService()

    def _start(self) -> None:
        result = self.scheduler.start_scheduling(TASK_ID)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["round"], 1)

    def test_start_dispatches_pages_and_marks_scheduler(self) -> None:
        self._start()
        task = store.find("feed_scheduler", TASK_ID)
        self.assertEqual(task["status"], "执行中")
        self.assertEqual(task["当前轮次"], "1")
        self.assertEqual(task["派发页面数"], 4)
        self.assertEqual(task["页面处理状态"], "采集中")

    def test_one_page_settles_before_pause(self) -> None:
        self._start()
        result = self.scheduler.receive_page_receipt(TASK_ID, PAGE_A)
        self.assertTrue(result["ok"])
        self.assertTrue(result["settled"])
        self.assertEqual(result["completion_round"], 1)
        self.assertEqual(result["block_count"], 4)
        self.assertEqual(result["settlement_status"], "已结算")
        task = store.find("feed_scheduler", TASK_ID)
        self.assertEqual(task["已结算页面数"], 1)

    def test_pause_stops_new_dispatch(self) -> None:
        self._start()
        self.assertTrue(self.scheduler.pause_scheduling(TASK_ID)["ok"])
        task = store.find("feed_scheduler", TASK_ID)
        self.assertEqual(task["status"], "已暂停")
        self.assertEqual(task["页面处理状态"], "暂停停派")
        with self.assertRaises(AssertionError):
            # 暂停后再次开始不派发新页面
            self.assertTrue(self.scheduler.start_scheduling(TASK_ID)["ok"])

    def test_paused_receipt_is_held_and_duplicate_rejected(self) -> None:
        self._start()
        self.scheduler.pause_scheduling(TASK_ID)
        first = self.scheduler.receive_page_receipt(TASK_ID, PAGE_B, receipt_id="rcpt-b")
        self.assertTrue(first["ok"])
        self.assertFalse(first["settled"])
        self.assertTrue(first["pause_conflict"])
        self.assertEqual(first["settlement_status"], "待结算")
        self.assertIsNone(first["completion_round"])
        self.assertIn("待恢复轮", first["explanation"])

        dup = self.scheduler.receive_page_receipt(TASK_ID, PAGE_B, receipt_id="rcpt-b2")
        self.assertFalse(dup["ok"])
        self.assertTrue(dup["duplicate"])
        self.assertIn("重复回执", dup["reason"])
        # 原有文本不被重复写入
        self.assertEqual(len(store.rows("pending_segment")), 4)

        task = store.find("feed_scheduler", TASK_ID)
        self.assertEqual(task["待结算页面数"], 1)
        self.assertEqual(task["页面处理状态"], "暂停在途")

    def test_same_round_pause_receipt_conflict_explains_completion_round(self) -> None:
        self._start()
        self.scheduler.pause_scheduling(TASK_ID)
        self.scheduler.receive_page_receipt(TASK_ID, PAGE_B)
        blocks = store.rows("pending_segment")
        self.assertTrue(all(b["status"] == "待结算" for b in blocks))
        detail = self.pending.detail(blocks[0]["id"])
        self.assertTrue(detail["same_round_pause_conflict"])
        self.assertEqual(detail["dispatch_round"], 1)
        self.assertEqual(detail["receipt_round"], 1)
        self.assertIsNone(detail["completion_round"])
        self.assertIn("暂停边界", detail["explanation"])

    def test_resume_settles_inflight_and_attributes_round(self) -> None:
        self._start()
        self.scheduler.receive_page_receipt(TASK_ID, PAGE_A)
        self.scheduler.pause_scheduling(TASK_ID)
        self.scheduler.receive_page_receipt(TASK_ID, PAGE_B)
        result = self.scheduler.resume_scheduling(TASK_ID)
        self.assertTrue(result["ok"])
        self.assertEqual(result["resumed_from_round"], 1)
        self.assertEqual(result["round"], 2)
        self.assertEqual(result["settled_pages"], [PAGE_B])
        self.assertEqual(result["settled_blocks"], 4)

        task = store.find("feed_scheduler", TASK_ID)
        self.assertEqual(task["status"], "执行中")
        self.assertEqual(task["当前轮次"], "2")
        self.assertEqual(task["已结算页面数"], 2)
        self.assertEqual(task["待结算页面数"], 0)

        for block in store.rows("pending_segment"):
            self.assertEqual(block["status"], "已结算")
            self.assertEqual(int(block["完成轮次"]), 2 if block["页面URI"] == PAGE_B else 1)
        page_b_detail = self.pending.detail(
            next(b["id"] for b in store.rows("pending_segment") if b["页面URI"] == PAGE_B)
        )
        self.assertEqual(page_b_detail["completion_round"], 2)
        self.assertIn("第2轮才算完成", page_b_detail["explanation"])

    def test_round_summary_counts_pages_and_blocks_without_double_counting(self) -> None:
        self._start()
        self.scheduler.receive_page_receipt(TASK_ID, PAGE_A)
        self.scheduler.pause_scheduling(TASK_ID)
        self.scheduler.receive_page_receipt(TASK_ID, PAGE_B)
        self.scheduler.resume_scheduling(TASK_ID)

        summary = self.scheduler.compute_round_summary(TASK_ID)
        self.assertTrue(summary["ok"])
        round1, round2 = summary["rounds"]
        self.assertEqual(round1["派发页面数"], 4)
        self.assertEqual(round1["回执页面数"], 2)
        self.assertEqual(round1["完成页面数"], 1)
        self.assertEqual(round1["完成分块数"], 4)
        self.assertEqual(round2["完成页面数"], 1)
        self.assertEqual(round2["完成分块数"], 4)
        self.assertEqual(round1["完成分块数"] + round2["完成分块数"], 8)
        self.assertEqual(summary["totals"]["settled_pages"], 2)
        self.assertEqual(summary["totals"]["settled_blocks"], 8)

    def test_receipt_for_unassigned_page_rejected(self) -> None:
        self._start()
        result = self.scheduler.receive_page_receipt(TASK_ID, "https://example.com/other/page")
        self.assertFalse(result["ok"])
        self.assertIn("未在任务", result["reason"])

    def test_receipt_after_task_finished_rejected(self) -> None:
        self._start()
        self.scheduler.run_action(TASK_ID, "完成任务")
        result = self.scheduler.receive_page_receipt(TASK_ID, PAGE_A)
        self.assertFalse(result["ok"])
        self.assertIn("不接受页面回执", result["reason"])

    def test_failed_verification_keeps_text_for_review(self) -> None:
        self._start()
        result = self.scheduler.receive_page_receipt(TASK_ID, PAGE_A, verified=False)
        self.assertTrue(result["ok"])
        self.assertFalse(result["settled"])
        self.assertEqual(result["settlement_status"], "保留核对")
        blocks = [b for b in store.rows("pending_segment") if b["页面URI"] == PAGE_A]
        self.assertTrue(blocks)
        self.assertTrue(all(b["status"] == "保留核对" for b in blocks))
        self.assertTrue(all(b["分块文本"] for b in blocks))
        self.assertIn("核对", blocks[0]["完成轮次说明"])

    def test_settlement_while_paused_is_refused(self) -> None:
        self._start()
        self.scheduler.pause_scheduling(TASK_ID)
        self.scheduler.receive_page_receipt(TASK_ID, PAGE_B)
        entry = store.rows("pending_segment")[0]
        settled, message = self.pending.run_action(entry["id"], "确认结算")
        self.assertIsNone(settled)
        self.assertIn("请先恢复调度", message)
        self.assertEqual(entry["status"], "待结算")

        # 恢复后结算落到恢复轮，而不是暂停轮
        self.scheduler.resume_scheduling(TASK_ID)
        self.assertEqual(entry["status"], "已结算")
        self.assertEqual(entry["完成轮次"], "2")

    def test_concurrent_duplicate_receipts_accept_exactly_one(self) -> None:
        import threading

        self._start()
        results: list[dict] = []
        lock = threading.Lock()

        def deliver(receipt_id: str) -> None:
            outcome = self.scheduler.receive_page_receipt(TASK_ID, PAGE_A, receipt_id=receipt_id)
            with lock:
                results.append(outcome)

        threads = [threading.Thread(target=deliver, args=(f"r{i}",)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        accepted = [r for r in results if r["ok"]]
        rejected = [r for r in results if not r["ok"]]
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 7)
        self.assertTrue(all(r["duplicate"] for r in rejected))
        self.assertEqual(len(store.rows("pending_segment")), 4)

    def test_same_receipt_id_on_different_page_rejected(self) -> None:
        self._start()
        first = self.scheduler.receive_page_receipt(TASK_ID, PAGE_A, receipt_id="rcpt-1")
        self.assertTrue(first["ok"])
        second = self.scheduler.receive_page_receipt(TASK_ID, PAGE_B, receipt_id="rcpt-1")
        self.assertFalse(second["ok"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(store.rows("pending_segment")), 4)

    def test_rejected_and_held_blocks_survive_resume_untouched(self) -> None:
        self._start()
        self.scheduler.receive_page_receipt(TASK_ID, PAGE_A, verified=False)
        held = list(store.rows("pending_segment"))
        self.scheduler.pause_scheduling(TASK_ID)
        self.scheduler.resume_scheduling(TASK_ID)
        self.assertEqual(store.rows("pending_segment"), held)
        self.assertTrue(all(b["status"] == "保留核对" for b in held))


if __name__ == "__main__":
    unittest.main()
