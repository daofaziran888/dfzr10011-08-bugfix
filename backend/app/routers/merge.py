"""结果聚合归并接口：维护聚合任务，覆盖启动归并、重试归并、归档任务等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import merge_files
from app.services.merge import MergeService

router = APIRouter(prefix="/api/merge", tags=["结果聚合归并"])

service = MergeService()

LIST_FIELDS = ["任务编号", "采集计划", "分片总数", "已归并数", "归并策略", "输出路径", "数据总量", "任务状态", "当前快照编号"]
STATUSES = ["待归并", "归并中", "已完成", "归并失败"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="待归并、归并中、已完成、归并失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤结果聚合归并列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出结果聚合归并清单：按快照口径返回聚合进度，与下载文件逐项核对。"""
    items, total = service.list_entries(page=1, size=10000)
    return {
        "module": "merge",
        "total": total,
        "snapshot": merge_files.current_snapshot_numbers(),
        "items": items,
    }


@router.get("/{entry_ref}/snapshots")
def list_snapshots(entry_ref: str) -> dict[str, Any]:
    """读取某个聚合任务的全部已提交归并快照，按版本递增。"""
    entry = service.resolve_entry(entry_ref)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    items = service.list_snapshots(str(entry.get("任务编号", "")))
    return {"module": "merge", "task": entry.get("任务编号"), "items": items, "total": len(items)}


@router.post("/{entry_ref}/snapshots", response_model=ActionResult)
def commit_snapshot(entry_ref: str, payload: EntryPayload) -> ActionResult:
    """提交一份新的归并快照；提交后聚合进度只从这份快照派生。"""
    entry = service.resolve_entry(entry_ref)
    if entry is None:
        return ActionResult(ok=False, message=f"聚合任务 {entry_id} 不存在或已归档")
    snapshot, message = service.commit_snapshot(str(entry.get("任务编号", "")), payload.values)
    if snapshot is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot)


@router.get("/{entry_ref}/downloads", response_model=PageResult[dict])
def list_task_downloads(entry_ref: str) -> PageResult[dict]:
    """某个聚合任务的下载清单：所有记录都带开始/完成快照编号，便于逐项核对。"""
    entry = service.resolve_entry(entry_ref)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    items = [
        merge_files.serialize_download(row)
        for row in merge_files.downloads(str(entry.get("任务编号", "")))
    ]
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 20)


@router.post("/{entry_ref}/downloads", response_model=ActionResult)
def start_download(entry_ref: str, payload: EntryPayload) -> ActionResult:
    """开始下载归并文件：一次下载只冻结一个已提交快照，重试沿用原快照。"""
    entry = service.resolve_entry(entry_ref)
    if entry is None:
        return ActionResult(ok=False, message=f"聚合任务 {entry_ref} 不存在或已归档")
    download, message = service.start_download(str(entry.get("任务编号", "")), payload.values)
    if download is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=merge_files.serialize_download(download))


@router.get("/downloads")
def list_downloads(
    task: str | None = Query(default=None, description="按任务编号过滤下载清单"),
) -> dict[str, Any]:
    """归并文件下载清单：每条记录只引用一个已提交快照，展示编号与归档编号同源。"""
    items = [merge_files.serialize_download(row) for row in merge_files.downloads(task)]
    return {"module": "merge", "total": len(items), "items": items}


@router.get("/downloads/{download_id}")
def get_download(download_id: str) -> dict[str, Any]:
    """读取单次下载记录与导出清单，快照编号与文件保持一致。"""
    download = merge_files.download_by_id(download_id)
    if download is None:
        raise HTTPException(status_code=404, detail=f"下载记录 {download_id} 不存在")
    snapshot = merge_files.snapshot_by_no(str(download.get("快照编号")))
    manifest = merge_files.build_manifest(download, snapshot) if snapshot else None
    payload = merge_files.serialize_download(download)
    payload["清单"] = manifest
    return payload


@router.post("/downloads/{download_id}/complete", response_model=ActionResult)
def complete_download(download_id: str, payload: EntryPayload | None = None) -> ActionResult:
    """完成下载：若是固定开始时快照策略，完成后冻结该快照并与归档保持同名同编号。"""
    values = payload.values if payload else {}
    download, message = merge_files.complete_download(download_id, fmt=values.get("文件格式"))
    if download is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=merge_files.serialize_download(download))


@router.post("/downloads/{download_id}/interrupt", response_model=ActionResult)
def interrupt_download(download_id: str) -> ActionResult:
    """中断下载；再次下载会沿用同一快照续传，不会拼出第二份文件。"""
    download, message = merge_files.interrupt_download(download_id)
    if download is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=merge_files.serialize_download(download))


@router.get("/downloads/{download_id}/file")
def download_file(download_id: str) -> Response:
    """按冻结快照生成归并文件；文件名称在完成后保持不变。"""
    download = merge_files.download_by_id(download_id)
    if download is None:
        raise HTTPException(status_code=404, detail=f"下载记录 {download_id} 不存在")
    if download.get("下载状态") != merge_files.DOWNLOAD_DONE:
        raise HTTPException(status_code=409, detail=f"下载 {download.get('下载编号')} 尚未完成，文件不可读取")
    try:
        content, file_name, media_type = merge_files.file_payload(download)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return Response(
        content=content,
        media_type=media_type,
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{merge_files.quote_name(file_name)}",
            "X-Snapshot-No": str(download.get("快照编号")),
            "X-Download-No": str(download.get("下载编号")),
        },
    )


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条聚合任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条聚合任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="聚合任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条聚合任务执行启动归并、重试归并、归档任务；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
