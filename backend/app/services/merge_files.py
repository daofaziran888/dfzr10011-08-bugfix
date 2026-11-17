"""已提交归并快照、归并文件下载与存储归档的共享规则。

一次下载只解析并冻结一个已提交归并快照。文件内容、下载清单和存储归档都引用
同一个快照编号，聚合进度也只从这份快照派生，避免文件和进度各读一半旧数据。
"""
from __future__ import annotations

import csv
import io
import json
import threading
import zipfile
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from app.store import store

SNAPSHOT_MODULE = "_merge_snapshots"
DOWNLOAD_MODULE = "_merge_downloads"
ARCHIVE_MODULE = "_storage_archives"

_LOCK = threading.RLock()

SNAPSHOT_COMMITTED = "已提交"
DOWNLOAD_RUNNING = "下载中"
DOWNLOAD_DONE = "已完成"
DOWNLOAD_INTERRUPTED = "已中断"

POLICY_PIN_START = "固定开始时快照"
POLICY_LATEST_FINISH = "完成时最新快照"

POLICY_ALIASES: dict[str, str] = {
    "pin_start": POLICY_PIN_START,
    "pin": POLICY_PIN_START,
    "start": POLICY_PIN_START,
    "fixed": POLICY_PIN_START,
    "固定下载开始时快照": POLICY_PIN_START,
    "固定开始时快照": POLICY_PIN_START,
    "开始时快照": POLICY_PIN_START,
    "latest": POLICY_LATEST_FINISH,
    "latest_at_finish": POLICY_LATEST_FINISH,
    "finish": POLICY_LATEST_FINISH,
    "完成时最新快照": POLICY_LATEST_FINISH,
    "采用下载完成时最新快照": POLICY_LATEST_FINISH,
    "最新快照": POLICY_LATEST_FINISH,
}

FILE_COLUMNS = ["任务编号", "快照编号", "分片序号", "分片数据量", "采集计划", "任务状态"]

SEED_SNAPSHOTS: list[dict[str, Any]] = [
    {
        "任务编号": "MERG-0001",
        "快照编号": "MRSN-0001-1",
        "版本号": 1,
        "分片总数": 4,
        "已归并数": 4,
        "数据总量": 3200,
        "采集计划": "采集计划-A",
        "任务状态": "已完成",
        "提交状态": SNAPSHOT_COMMITTED,
        "提交时间": "2026-09-01T10:00:00",
    },
    {
        "任务编号": "MERG-0002",
        "快照编号": "MRSN-0002-1",
        "版本号": 1,
        "分片总数": 6,
        "已归并数": 4,
        "数据总量": 3400,
        "采集计划": "采集计划-B",
        "任务状态": "归并中",
        "提交状态": SNAPSHOT_COMMITTED,
        "提交时间": "2026-09-02T09:00:00",
    },
    {
        "任务编号": "MERG-0002",
        "快照编号": "MRSN-0002-2",
        "版本号": 2,
        "分片总数": 6,
        "已归并数": 5,
        "数据总量": 4300,
        "采集计划": "采集计划-B",
        "任务状态": "归并中",
        "提交状态": SNAPSHOT_COMMITTED,
        "提交时间": "2026-09-02T11:30:00",
    },
    {
        "任务编号": "MERG-0003",
        "快照编号": "MRSN-0003-1",
        "版本号": 1,
        "分片总数": 5,
        "已归并数": 5,
        "数据总量": 5100,
        "采集计划": "采集计划-C",
        "任务状态": "已完成",
        "提交状态": SNAPSHOT_COMMITTED,
        "提交时间": "2026-09-03T15:00:00",
    },
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_policy(raw: Any) -> str | None:
    """把动作里的策略写法统一成两种受支持的快照口径。"""
    text = str(raw or "").strip()
    if not text:
        return POLICY_PIN_START
    return POLICY_ALIASES.get(text) or POLICY_ALIASES.get(text.lower())


def normalize_format(raw: Any) -> str:
    text = str(raw or "csv").strip().lower()
    return text if text in {"csv", "zip"} else "csv"


def quote_name(name: str) -> str:
    """按 RFC 5987 编码下载文件名，中文原名回读不丢字。"""
    return quote(name, safe="")


def current_snapshot_numbers() -> dict[str, str | None]:
    """导出清单里带上每个任务当前引用的已提交快照编号。"""
    ensure_seeded()
    return {
        str(row.get("任务编号", "")): (latest_snapshot(str(row.get("任务编号", ""))) or {}).get("快照编号")
        for row in store.rows("merge")
    }


_SEEDED = False


def ensure_seeded() -> None:
    """首次访问时准备已提交快照，并把聚合任务进度同步到最新快照。"""
    global _SEEDED
    if _SEEDED:
        return
    _SEEDED = True
    snapshots = store.rows(SNAPSHOT_MODULE)
    if not snapshots:
        for index, seed in enumerate(SEED_SNAPSHOTS, start=1):
            row = dict(seed)
            row["id"] = index
            snapshots.append(row)
    for entry in store.rows("merge"):
        sync_entry_progress(entry)


def snapshots(task_no: str | None = None) -> list[dict[str, Any]]:
    ensure_seeded()
    rows = [row for row in store.rows(SNAPSHOT_MODULE) if row.get("提交状态") == SNAPSHOT_COMMITTED]
    if task_no:
        rows = [row for row in rows if row.get("任务编号") == task_no]
    return sorted(rows, key=lambda row: (str(row.get("任务编号", "")), _int(row.get("版本号"))))


def snapshot_by_no(snapshot_no: str | None) -> dict[str, Any] | None:
    if not snapshot_no:
        return None
    ensure_seeded()
    for row in store.rows(SNAPSHOT_MODULE):
        if row.get("快照编号") == snapshot_no and row.get("提交状态") == SNAPSHOT_COMMITTED:
            return row
    return None


def latest_snapshot(task_no: str) -> dict[str, Any] | None:
    rows = snapshots(task_no)
    return rows[-1] if rows else None


def sync_entry_progress(entry: dict[str, Any]) -> dict[str, Any]:
    """聚合进度只从已提交快照派生，保证列表、详情和文件读的是同一份数据。"""
    snapshot = latest_snapshot(str(entry.get("任务编号", "")))
    if snapshot is None:
        return entry
    entry["当前快照编号"] = snapshot["快照编号"]
    entry["快照版本号"] = snapshot["版本号"]
    entry["分片总数"] = snapshot["分片总数"]
    entry["已归并数"] = snapshot["已归并数"]
    entry["数据总量"] = snapshot["数据总量"]
    entry["任务状态"] = entry.get("status", entry.get("任务状态"))
    return entry


def commit_snapshot(
    task_no: str,
    *,
    merged: Any = None,
    total: Any = None,
    shards: Any = None,
) -> tuple[dict[str, Any] | None, str]:
    """提交一份新的归并快照；同一任务内版本号单调递增。"""
    with _LOCK:
        return _commit_snapshot_locked(task_no, merged=merged, total=total, shards=shards)


def _commit_snapshot_locked(
    task_no: str,
    *,
    merged: Any = None,
    total: Any = None,
    shards: Any = None,
) -> tuple[dict[str, Any] | None, str]:
    ensure_seeded()
    entry = next((row for row in store.rows("merge") if row.get("任务编号") == task_no), None)
    if entry is None:
        return None, f"聚合任务 {task_no} 不存在或已归档"
    previous = latest_snapshot(task_no)
    if previous is None:
        version = 1
        prefix = f"MRSN-{task_no.split('-')[-1]}"
    else:
        version = _int(previous.get("版本号")) + 1
        prefix = str(previous.get("快照编号", "")).rsplit("-", 1)[0]
    shard_total = _int(shards, _int(previous.get("分片总数") if previous else entry.get("分片总数")))
    merged_count = _int(merged, _int(previous.get("已归并数") if previous else entry.get("已归并数")))
    data_total = _int(total, _int(previous.get("数据总量") if previous else entry.get("数据总量")))
    if shard_total <= 0:
        return None, "分片总数必须大于 0 才能提交归并快照"
    if merged_count < 0 or merged_count > shard_total:
        return None, "已归并数不能超过分片总数"
    if data_total < 0:
        return None, "数据总量不能为负数"
    rows = store.rows(SNAPSHOT_MODULE)
    snapshot = {
        "id": _next_id(rows),
        "任务编号": task_no,
        "快照编号": f"{prefix}-{version}",
        "版本号": version,
        "分片总数": shard_total,
        "已归并数": merged_count,
        "数据总量": data_total,
        "采集计划": previous.get("采集计划") if previous else entry.get("采集计划"),
        "任务状态": entry.get("status", entry.get("任务状态")),
        "提交状态": SNAPSHOT_COMMITTED,
        "提交时间": _now(),
    }
    rows.append(snapshot)
    sync_entry_progress(entry)
    return snapshot, f"归并快照 {snapshot['快照编号']} 已提交"


# ---------------------------------------------------------------------------
# 下载清单
# ---------------------------------------------------------------------------


def downloads(task_no: str | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows(DOWNLOAD_MODULE))
    if task_no:
        rows = [row for row in rows if row.get("任务编号") == task_no]
    return sorted(rows, key=lambda row: _int(row.get("id")))


def download_by_id(download_id: Any) -> dict[str, Any] | None:
    """下载记录既能用内部 id 定位，也能用对外的下载编号定位。"""
    for row in store.rows(DOWNLOAD_MODULE):
        if str(row.get("id")) == str(download_id) or row.get("下载编号") == str(download_id):
            return row
    return None


def display_snapshot(download: dict[str, Any]) -> str | None:
    """下载清单展示口径：固定开始时快照看开始编号，采用完成时最新快照看完成编号。"""
    if download.get("策略") == POLICY_LATEST_FINISH:
        return download.get("完成快照编号") or download.get("快照编号") or download.get("开始快照编号")
    return download.get("开始快照编号") or download.get("快照编号")


def serialize_download(download: dict[str, Any]) -> dict[str, Any]:
    payload = dict(download)
    payload.pop("id", None)
    payload["展示快照编号"] = display_snapshot(download)
    payload.setdefault("快照编号", payload.get("展示快照编号"))
    return payload


def _file_name(task_no: str, snapshot_no: str, fmt: str) -> str:
    return f"{task_no}-归并结果-{snapshot_no}.{fmt}"


def _find_reusable(task_no: str, policy: str, start_snapshot_no: str) -> dict[str, Any] | None:
    """找可以直接沿用的下载记录：未完成的续传，已完成的同一快照不重复出文件。"""
    for row in reversed(downloads(task_no)):
        if row.get("策略") != policy:
            continue
        if row.get("下载状态") in {DOWNLOAD_RUNNING, DOWNLOAD_INTERRUPTED}:
            return row
        if row.get("下载状态") == DOWNLOAD_DONE and row.get("快照编号") == start_snapshot_no:
            return row
    return None


def create_download(
    task_no: str,
    *,
    policy: Any = None,
    fmt: Any = None,
    force: bool = False,
) -> tuple[dict[str, Any] | None, str]:
    """开始或续传一次下载；未完成的下载沿用原快照，不生成第二份文件。"""
    with _LOCK:
        return _create_download_locked(task_no, policy=policy, fmt=fmt, force=force)


def _create_download_locked(
    task_no: str,
    *,
    policy: Any = None,
    fmt: Any = None,
    force: bool = False,
) -> tuple[dict[str, Any] | None, str]:
    ensure_seeded()
    entry = next((row for row in store.rows("merge") if row.get("任务编号") == task_no), None)
    if entry is None:
        return None, f"聚合任务 {task_no} 不存在或已归档"
    policy_name = normalize_policy(policy)
    if policy_name is None:
        return None, f"下载策略「{policy}」不受支持，请选择固定开始时快照或完成时最新快照"
    start = latest_snapshot(task_no)
    if start is None:
        return None, f"聚合任务 {task_no} 还没有已提交归并快照，无法下载"
    file_format = normalize_format(fmt)
    if not force:
        reusable = _find_reusable(task_no, policy_name, str(start["快照编号"]))
        if reusable is not None:
            note = "沿用未完成下载" if reusable.get("下载状态") != DOWNLOAD_DONE else "同一快照已有完整下载文件"
            return reusable, f"{note}，仍引用快照 {display_snapshot(reusable)}"
    rows = store.rows(DOWNLOAD_MODULE)
    download_id = _next_id(rows)
    pinned = str(start["快照编号"]) if policy_name == POLICY_PIN_START else None
    download = {
        "id": download_id,
        "下载编号": f"MDLD-{download_id:04d}",
        "任务编号": task_no,
        "策略": policy_name,
        "开始快照编号": start["快照编号"],
        "完成快照编号": None,
        "快照编号": pinned,
        "文件名称": _file_name(task_no, str(start["快照编号"]), file_format) if pinned else None,
        "文件格式": file_format,
        "下载状态": DOWNLOAD_RUNNING,
        "条目数": None,
        "数据总量": None,
        "归档编号": None,
        "归档版本号": None,
        "快照冲突": False,
        "开始时间": _now(),
        "完成时间": None,
    }
    rows.append(download)
    message = (
        f"下载已开始，固定快照 {pinned}"
        if pinned
        else f"下载已开始，完成时采用最新已提交快照（开始时为 {start['快照编号']}）"
    )
    return download, message


def file_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """把一份已提交快照拆成逐项归并记录，每项都带同一个快照编号。"""
    task_no = str(snapshot.get("任务编号", ""))
    merged = _int(snapshot.get("已归并数"))
    total = _int(snapshot.get("数据总量"))
    per_item, remainder = divmod(total, merged) if merged else (0, 0)
    rows: list[dict[str, Any]] = []
    for index in range(1, merged + 1):
        rows.append({
            "任务编号": task_no,
            "快照编号": snapshot.get("快照编号"),
            "分片序号": index,
            "分片数据量": per_item + (1 if index <= remainder else 0),
            "采集计划": snapshot.get("采集计划"),
            "任务状态": snapshot.get("任务状态"),
        })
    return rows


def build_csv(snapshot: dict[str, Any]) -> bytes:
    buffer = io.StringIO()
    buffer.write("﻿")
    writer = csv.DictWriter(buffer, fieldnames=FILE_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(file_rows(snapshot))
    return buffer.getvalue().encode("utf-8")


def build_manifest(download: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = file_rows(snapshot)
    return {
        "下载编号": download.get("下载编号"),
        "任务编号": snapshot.get("任务编号"),
        "快照编号": snapshot.get("快照编号"),
        "版本号": snapshot.get("版本号"),
        "策略": download.get("策略"),
        "展示快照编号": display_snapshot(download),
        "文件名称": download.get("文件名称"),
        "文件格式": download.get("文件格式"),
        "条目数": len(rows),
        "数据总量": sum(_int(row.get("分片数据量")) for row in rows),
        "生成时间": download.get("完成时间") or download.get("开始时间"),
        "字段": FILE_COLUMNS,
    }


def build_zip(download: dict[str, Any], snapshot: dict[str, Any]) -> bytes:
    """把归并文件和导出清单打成一个包，清单里的文件名与下载文件名一致。"""
    csv_name = _file_name(str(snapshot.get("任务编号", "")), str(snapshot.get("快照编号", "")), "csv")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as package:
        package.writestr(csv_name, build_csv(snapshot))
        manifest = build_manifest(download, snapshot)
        manifest["文件名称"] = csv_name
        package.writestr("导出清单.json", json.dumps(manifest, ensure_ascii=False, indent=2))
    return buffer.getvalue()


def file_payload(download: dict[str, Any]) -> tuple[bytes, str, str]:
    """返回 (文件字节, 文件名, media_type)。文件只从冻结快照生成，重复读取内容一致。"""
    snapshot = snapshot_by_no(download.get("快照编号"))
    if snapshot is None:
        raise ValueError("下载记录没有可用的已提交归并快照")
    file_format = normalize_format(download.get("文件格式"))
    file_name = download.get("文件名称") or _file_name(
        str(snapshot.get("任务编号", "")), str(snapshot.get("快照编号", "")), file_format
    )
    if file_format == "zip":
        return build_zip(download, snapshot), file_name, "application/zip"
    return build_csv(snapshot), file_name, "text/csv; charset=utf-8"


# ---------------------------------------------------------------------------
# 存储归档
# ---------------------------------------------------------------------------


def archives(task_no: str | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows(ARCHIVE_MODULE))
    if task_no:
        rows = [row for row in rows if row.get("任务编号") == task_no]
    return sorted(rows, key=lambda row: _int(row.get("id")))


def _archive_for(task_no: str, snapshot_no: str) -> dict[str, Any] | None:
    return next(
        (row for row in store.rows(ARCHIVE_MODULE)
         if row.get("任务编号") == task_no and row.get("快照编号") == snapshot_no),
        None,
    )


def _archive_of_download(download: dict[str, Any]) -> dict[str, Any] | None:
    return next(
        (row for row in store.rows(ARCHIVE_MODULE)
         if row.get("下载编号") == download.get("下载编号")),
        None,
    )


def ensure_archive(
    download: dict[str, Any],
    snapshot: dict[str, Any],
    *,
    conflict: bool,
) -> dict[str, Any]:
    """登记归并文件版本，并保留与下载清单相同的快照编号。"""
    task_no = str(download.get("任务编号", ""))
    snapshot_no = str(snapshot.get("快照编号", ""))
    own = _archive_of_download(download)
    if own is not None:
        download["归档编号"] = own["归档编号"]
        download["归档版本号"] = own["版本号"]
        return own
    existing = _archive_for(task_no, snapshot_no)
    if existing is not None and not conflict:
        download["归档编号"] = existing["归档编号"]
        download["归档版本号"] = existing["版本号"]
        return existing
    rows = store.rows(ARCHIVE_MODULE)
    if conflict:
        # 生成期间出现了新的已提交快照：固定的是开始快照，归档版本继续追加。
        version = max(
            (_int(row.get("版本号")) for row in rows if row.get("任务编号") == task_no),
            default=_int(snapshot.get("版本号")),
        ) + 1
    else:
        version = _int(snapshot.get("版本号"))
    archive_id = _next_id(rows)
    archive = {
        "id": archive_id,
        "归档编号": f"MARC-{archive_id:04d}",
        "任务编号": task_no,
        "快照编号": snapshot_no,
        "版本号": version,
        "下载编号": download.get("下载编号"),
        "文件名称": download.get("文件名称"),
        "文件格式": download.get("文件格式"),
        "存储层级": "归并结果层",
        "归档方式": "追加下一版本" if conflict else "登记快照版本",
        "归档状态": "已归档",
        "条目数": _int(snapshot.get("已归并数")),
        "数据总量": _int(snapshot.get("数据总量")),
        "归档时间": _now(),
    }
    rows.append(archive)
    download["归档编号"] = archive["归档编号"]
    download["归档版本号"] = archive["版本号"]
    return archive


def complete_download(download_id: Any, *, fmt: Any = None) -> tuple[dict[str, Any] | None, str]:
    """完成下载并冻结快照：此后文件、清单、归档都指向同一快照编号。"""
    with _LOCK:
        return _complete_download_locked(download_id, fmt=fmt)


def _complete_download_locked(download_id: Any, *, fmt: Any = None) -> tuple[dict[str, Any] | None, str]:
    download = download_by_id(download_id)
    if download is None:
        return None, f"下载记录 {download_id} 不存在"
    if download.get("下载状态") == DOWNLOAD_DONE:
        download["展示快照编号"] = display_snapshot(download)
        return download, f"下载已完成，沿用原文件与快照 {display_snapshot(download)}"
    if fmt is not None:
        download["文件格式"] = normalize_format(fmt)
    snapshot = None
    if download.get("策略") == POLICY_PIN_START:
        snapshot = snapshot_by_no(download.get("快照编号") or download.get("开始快照编号"))
        if snapshot is None:
            return None, "固定开始时快照已经不可用，不能与另一份快照拼接"
        download["完成快照编号"] = snapshot["快照编号"]
    else:
        snapshot = latest_snapshot(str(download.get("任务编号", "")))
        if snapshot is None:
            return None, "聚合任务还没有已提交归并快照，无法完成下载"
        download["完成快照编号"] = snapshot["快照编号"]
        download["快照编号"] = snapshot["快照编号"]
    download["文件名称"] = _file_name(
        str(download.get("任务编号", "")),
        str(snapshot["快照编号"]),
        normalize_format(download.get("文件格式")),
    )
    download["条目数"] = _int(snapshot.get("已归并数"))
    download["数据总量"] = _int(snapshot.get("数据总量"))
    download["下载状态"] = DOWNLOAD_DONE
    download["完成时间"] = _now()
    latest = latest_snapshot(str(download.get("任务编号", "")))
    conflict = (
        download.get("策略") == POLICY_PIN_START
        and latest is not None
        and _int(latest.get("版本号")) > _int(snapshot.get("版本号"))
    )
    download["快照冲突"] = conflict
    archive = ensure_archive(download, snapshot, conflict=conflict)
    note = ""
    if conflict:
        note = f"；期间出现新快照 {latest['快照编号']}，归档追加下一版本 {archive['版本号']}"
    download["展示快照编号"] = display_snapshot(download)
    return download, f"下载已完成，引用快照 {display_snapshot(download)}{note}"


def interrupt_download(download_id: Any) -> tuple[dict[str, Any] | None, str]:
    with _LOCK:
        return _interrupt_download_locked(download_id)


def _interrupt_download_locked(download_id: Any) -> tuple[dict[str, Any] | None, str]:
    download = download_by_id(download_id)
    if download is None:
        return None, f"下载记录 {download_id} 不存在"
    if download.get("下载状态") == DOWNLOAD_DONE:
        return download, "下载已完成，无需中断"
    download["下载状态"] = DOWNLOAD_INTERRUPTED
    download["展示快照编号"] = display_snapshot(download)
    return download, f"下载已中断，重试仍沿用快照 {display_snapshot(download)}"


# ---------------------------------------------------------------------------
# 存储侧：归并文件数据、修复归并导出与状态汇总
# ---------------------------------------------------------------------------


def merge_file_entries() -> list[dict[str, Any]]:
    """存储模块可见的归并文件数据：只取已完成的下载记录。"""
    entries: list[dict[str, Any]] = []
    for download in downloads():
        if download.get("下载状态") != DOWNLOAD_DONE:
            continue
        snapshot = snapshot_by_no(download.get("快照编号"))
        if snapshot is None:
            continue
        archive = _archive_of_download(download)
        entries.append({
            "下载编号": download.get("下载编号"),
            "任务编号": download.get("任务编号"),
            "快照编号": snapshot.get("快照编号"),
            "展示快照编号": display_snapshot(download),
            "版本号": snapshot.get("版本号"),
            "文件名称": download.get("文件名称"),
            "文件格式": download.get("文件格式"),
            "条目数": download.get("条目数"),
            "数据总量": download.get("数据总量"),
            "下载状态": download.get("下载状态"),
            "策略": download.get("策略"),
            "快照冲突": download.get("快照冲突", False),
            "归档编号": archive.get("归档编号") if archive else download.get("归档编号"),
            "归档版本号": archive.get("版本号") if archive else download.get("归档版本号"),
            "存储层级": archive.get("存储层级") if archive else None,
            "清单": build_manifest(download, snapshot),
        })
    return sorted(entries, key=lambda row: str(row.get("下载编号", "")))


def verify_download(download: dict[str, Any]) -> list[str]:
    """逐项核对一条已完成下载：文件、清单、归档必须共用一个已提交快照编号。"""
    issues: list[str] = []
    if download.get("下载状态") != DOWNLOAD_DONE:
        return [f"下载 {download.get('下载编号')} 尚未完成"]
    snapshot_no = download.get("快照编号")
    snapshot = snapshot_by_no(str(snapshot_no))
    if snapshot is None:
        return [f"快照 {snapshot_no} 不是已提交快照"]
    archive = _archive_of_download(download)
    if archive is None:
        issues.append("下载清单没有对应的存储归档条目")
    elif archive.get("快照编号") != snapshot_no:
        issues.append(f"归档快照 {archive.get('快照编号')} 与下载清单 {snapshot_no} 不一致")
    file_snapshots = {row.get("快照编号") for row in file_rows(snapshot)}
    if file_snapshots != {snapshot_no}:
        issues.append(f"文件内快照 {sorted(file_snapshots)} 与下载清单 {snapshot_no} 不一致")
    manifest = build_manifest(download, snapshot)
    if manifest.get("快照编号") != snapshot_no:
        issues.append("导出清单快照编号与下载清单不一致")
    if display_snapshot(download) != snapshot_no:
        issues.append("下载清单展示的快照编号与文件快照不一致")
    if _int(download.get("条目数")) != len(file_rows(snapshot)):
        issues.append("下载清单条目数与文件条目数不一致")
    return issues


def merge_summary() -> list[dict[str, Any]]:
    """按聚合任务逐项核对：聚合进度、下载清单、存储归档是否指向同一快照。"""
    ensure_seeded()
    items: list[dict[str, Any]] = []
    for entry in store.rows("merge"):
        task_no = str(entry.get("任务编号", ""))
        sync_entry_progress(entry)
        latest = latest_snapshot(task_no)
        completed = [row for row in downloads(task_no) if row.get("下载状态") == DOWNLOAD_DONE]
        download = completed[-1] if completed else None
        archive = _archive_of_download(download) if download else None
        download_snapshot = download.get("快照编号") if download else None
        archive_snapshot = archive.get("快照编号") if archive else None
        progress_snapshot = latest.get("快照编号") if latest else entry.get("当前快照编号")
        issues: list[str] = []
        if download is not None:
            issues = verify_download(download)
        if download is None:
            result = "未下载"
        elif issues or archive is None or download_snapshot != archive_snapshot:
            result = "不一致"
        elif download_snapshot == progress_snapshot:
            result = "一致"
        else:
            result = "快照已更新"
        items.append({
            "任务编号": task_no,
            "采集计划": entry.get("采集计划") or (latest.get("采集计划") if latest else None),
            "聚合进度快照编号": progress_snapshot,
            "下载清单快照编号": download_snapshot,
            "展示快照编号": display_snapshot(download) if download else None,
            "归档快照编号": archive_snapshot,
            "归档版本号": archive.get("版本号") if archive else None,
            "归档编号": archive.get("归档编号") if archive else None,
            "下载条目数": download.get("条目数") if download else 0,
            "已归并数": _int(entry.get("已归并数")),
            "数据总量": _int(entry.get("数据总量")),
            "核对结果": result,
            "与进度一致": result == "一致",
            "一致": result in {"一致", "快照已更新"},
            "问题": issues,
        })
    return items
