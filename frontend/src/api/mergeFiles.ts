/** 归并文件下载与分析：原文件名回读、打包清单回读、快照编号核对。 */
import { request } from '@/api/client'

export type DownloadRecord = {
  id: number
  下载编号: string
  任务编号: string
  策略: string
  开始快照编号: string | null
  完成快照编号: string | null
  快照编号: string | null
  展示快照编号: string | null
  文件名称: string | null
  文件格式: string
  下载状态: string
  条目数: number | null
  数据总量: number | null
  归档编号: string | null
  归档版本号: number | null
  快照冲突: boolean
  清单?: Record<string, unknown> | null
}

export type MergeFileEntry = {
  下载编号: string
  任务编号: string
  快照编号: string
  展示快照编号: string
  版本号: number
  文件名称: string
  文件格式: string
  条目数: number
  数据总量: number
  下载状态: string
  策略: string
  快照冲突: boolean
  归档编号: string
  归档版本号: number
  存储层级: string
  清单: Record<string, unknown>
}

export type MergeSummaryEntry = {
  任务编号: string
  采集计划: string
  聚合进度快照编号: string | null
  下载清单快照编号: string | null
  展示快照编号: string | null
  归档快照编号: string | null
  归档版本号: number | null
  归档编号: string | null
  下载条目数: number
  已归并数: number
  数据总量: number
  核对结果: string
  一致: boolean
  问题: string[]
}

async function readJson<T>(response: Response, message: string): Promise<T> {
  if (!response.ok) {
    throw new Error(message)
  }
  return (await response.json()) as T
}

export async function getDownload(downloadId: string): Promise<DownloadRecord> {
  return readJson<DownloadRecord>(await request(`/api/merge/downloads/${downloadId}`), '下载记录读取失败')
}

export async function startDownload(
  taskNo: string,
  values: { 下载策略: string; 文件格式: string },
): Promise<DownloadRecord> {
  const response = await request(`/api/merge/${encodeURIComponent(taskNo)}/downloads`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  const payload = await readJson<{ ok: boolean; message: string; entry: DownloadRecord | null }>(
    response,
    '归并文件下载未能开始',
  )
  if (!payload.ok || !payload.entry) {
    throw new Error(payload.message || '归并文件下载未能开始')
  }
  return payload.entry
}

async function actOnDownload(path: string): Promise<DownloadRecord> {
  const payload = await readJson<{ ok: boolean; message: string; entry: DownloadRecord | null }>(
    await request(path, { method: 'POST', body: JSON.stringify({ values: {} }) }),
    '下载记录未能更新',
  )
  if (!payload.ok || !payload.entry) {
    throw new Error(payload.message || '下载记录未能更新')
  }
  return payload.entry
}

export function completeDownload(downloadId: string): Promise<DownloadRecord> {
  return actOnDownload(`/api/merge/downloads/${downloadId}/complete`)
}

export function interruptDownload(downloadId: string): Promise<DownloadRecord> {
  return actOnDownload(`/api/merge/downloads/${downloadId}/interrupt`)
}

/** 从 Content-Disposition 里按 UTF-8 还原原始文件名。 */
export function fileNameFrom(header: string | null, fallback: string): string {
  if (!header) {
    return fallback
  }
  const encoded = /filename\*=UTF-8''([^;]+)/i.exec(header)
  if (encoded) {
    try {
      return decodeURIComponent(encoded[1].trim().replace(/^"|"$/g, ''))
    } catch {
      return fallback
    }
  }
  const plain = /filename="?([^";]+)"?/i.exec(header)
  return plain ? plain[1] : fallback
}

export async function fetchFile(downloadId: string): Promise<{ blob: Blob; fileName: string }> {
  const response = await request(`/api/merge/downloads/${downloadId}/file`)
  if (!response.ok) {
    throw new Error(`下载文件读取失败（${response.status}）`)
  }
  const blob = await response.blob()
  const fallback = `归并结果-${downloadId}`
  return { blob, fileName: fileNameFrom(response.headers.get('Content-Disposition'), fallback) }
}

export function saveBlob(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = fileName
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
  setTimeout(() => URL.revokeObjectURL(url), 2000)
}

/** 读 zip 内的导出清单；只依赖浏览器自带的 DecompressionStream。 */
export async function readManifestFromZip(blob: Blob): Promise<Record<string, unknown>> {
  const buffer = new Uint8Array(await blob.arrayBuffer())
  const view = new DataView(buffer.buffer)
  let offset = buffer.length - 22
  const limit = Math.max(0, buffer.length - 65557)
  while (offset >= limit && view.getUint32(offset, true) !== 0x06054b50) {
    offset -= 1
  }
  if (offset < limit) {
    throw new Error('压缩包缺少可读的目录，无法回读导出清单')
  }
  const count = view.getUint16(offset + 10, true)
  let cursor = view.getUint32(offset + 16, true)
  const decoder = new TextDecoder('utf-8')
  for (let index = 0; index < count; index += 1) {
    if (view.getUint32(cursor, true) !== 0x02014b50) {
      break
    }
    const method = view.getUint16(cursor + 10, true)
    const compressedSize = view.getUint32(cursor + 20, true)
    const nameLength = view.getUint16(cursor + 28, true)
    const extraLength = view.getUint16(cursor + 30, true)
    const commentLength = view.getUint16(cursor + 32, true)
    const localOffset = view.getUint32(cursor + 42, true)
    const name = decoder.decode(buffer.subarray(cursor + 46, cursor + 46 + nameLength))
    if (name.endsWith('导出清单.json')) {
      const localNameLength = view.getUint16(localOffset + 26, true)
      const localExtraLength = view.getUint16(localOffset + 28, true)
      const start = localOffset + 30 + localNameLength + localExtraLength
      const data = buffer.subarray(start, start + compressedSize)
      let raw: Uint8Array
      if (method === 0) {
        raw = data
      } else if (method === 8) {
        const stream = new Blob([data]).stream().pipeThrough(new DecompressionStream('deflate-raw'))
        raw = new Uint8Array(await new Response(stream).arrayBuffer())
      } else {
        throw new Error(`不支持的压缩方式 ${method}，无法回读导出清单`)
      }
      return JSON.parse(decoder.decode(raw)) as Record<string, unknown>
    }
    cursor += 46 + nameLength + extraLength + commentLength
  }
  throw new Error('压缩包里没有导出清单')
}
