"""结果聚合归并业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services import merge_files
from app.store import store

MODULE = "merge"
REQUIRED_FIELDS = ["任务编号", "采集计划", "分片总数"]
STATUS_ORDER = ["待归并", "归并中", "已完成", "归并失败"]
ACTION_RULES = {"启动归并": "待归并", "重试归并": "归并中", "归档任务": "归并失败"}
NEGATIVE_ACTIONS = []


class MergeService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        merge_files.ensure_seeded()
        rows = store.rows(MODULE)
        for row in rows:
            merge_files.sync_entry_progress(row)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        merge_files.ensure_seeded()
        entry = store.find(MODULE, entry_id)
        if entry is not None:
            merge_files.sync_entry_progress(entry)
        return entry

    def resolve_entry(self, identifier: str | int) -> dict[str, Any] | None:
        """聚合任务既能用内部 id，也能用任务编号（如 MERG-0002）定位。"""
        merge_files.ensure_seeded()
        text = str(identifier).strip()
        entry = store.find(MODULE, int(text)) if text.isdigit() else None
        if entry is None:
            entry = next((row for row in store.rows(MODULE) if row.get("任务编号") == text), None)
        if entry is not None:
            merge_files.sync_entry_progress(entry)
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"聚合任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于结果聚合归并可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        merge_files.sync_entry_progress(entry)
        return entry, f"聚合任务已{action}"

    def commit_snapshot(self, task_no: str, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """把当前归并结果提交成一份快照；下载与归档只认已提交快照。"""
        return merge_files.commit_snapshot(
            task_no,
            merged=values.get("已归并数"),
            total=values.get("数据总量"),
            shards=values.get("分片总数"),
        )

    def list_snapshots(self, task_no: str) -> list[dict[str, Any]]:
        return merge_files.snapshots(task_no)

    def start_download(self, task_no: str, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        return merge_files.create_download(
            task_no,
            policy=values.get("下载策略") or values.get("策略"),
            fmt=values.get("文件格式"),
            force=bool(values.get("强制新建") or values.get("force")),
        )
