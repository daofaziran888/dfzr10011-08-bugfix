"""采集存储分层接口：维护存储策略，并承接归并文件的归档与状态核对。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult

from app.services.storage import StorageService

router = APIRouter(prefix="/api/storage", tags=["采集存储分层"])

service = StorageService()

LIST_FIELDS = ["策略编号", "存储层级", "文件格式", "压缩算法", "保留天数", "迁移规则", "清理方式", "策略状态"]
STATUSES = ["草稿", "已生效", "已暂停", "已关闭"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、已生效、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号与状态过滤采集存储分层列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集存储分层清单，并附带归并文件与快照核对结果，便于一并归档。"""
    items, total = service.list_entries(page=1, size=10000)
    return {
        "module": "storage",
        "total": total,
        "items": items,
        "merge_files": service.merge_files(),
        "snapshots": {row["任务编号"]: row["快照编号"] for row in service.merge_files()},
        "核对": _check_payload(service.merge_summary()),
    }


@router.get("/merge-export")
def export_merge_files() -> dict[str, Any]:
    """修复归并导出：把已完成的归并文件按快照编号与归档版本列成清单。"""
    files = service.merge_files()
    return {
        "module": "storage",
        "total": len(files),
        "snapshots": {row["任务编号"]: row["快照编号"] for row in files},
        "items": files,
        "核对": _check_payload(service.merge_summary()),
    }


def _check_payload(summary: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total": len(summary),
        "consistent": sum(1 for item in summary if item["一致"]),
        "inconsistent": [item for item in summary if not item["一致"]],
    }


@router.get("/merge-files")
def merge_files_list() -> dict[str, Any]:
    """归并文件数据：存储模块读取的已提交快照文件视图。"""
    items = service.merge_files()
    return {"module": "storage", "total": len(items), "items": items}


@router.get("/merge-files/{download_id}")
def merge_file_detail(download_id: str) -> dict[str, Any]:
    """单个归并文件的下载清单与归档信息，快照编号必须一致。"""
    item = service.merge_file(download_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"归并文件 {download_id} 不存在或未完成下载")
    return item


@router.get("/merge-summary")
def merge_summary() -> dict[str, Any]:
    """状汇总清单：聚合进度、下载清单、存储归档逐项核对同一快照编号。"""
    items = service.merge_summary()
    return {
        "module": "storage",
        "total": len(items),
        "consistent": sum(1 for item in items if item["一致"]),
        "items": items,
    }


@router.get("/archives")
def list_archives(task: str | None = Query(default=None, description="按任务编号过滤归档条目")) -> dict[str, Any]:
    """存储分层登记的归并文件版本，保留与下载清单相同的快照编号。"""
    items = service.archives(task)
    return {"module": "storage", "total": len(items), "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条存储策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"存储策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条存储策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="存储策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条存储策略执行启用策略、暂停策略、关闭策略；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
