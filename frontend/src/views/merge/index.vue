<template>
  <section class="page" data-module="merge">
    <header class="page-head">
      <div>
        <h2>结果聚合归并管理</h2>
        <p class="page-desc">维护采集结果的聚合策略、进度追踪与输出管理，为结果聚合归并提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记聚合任务</button>
        <RouterLink class="btn" to="/storage?tab=merge-files">归并文件数据</RouterLink>
        <RouterLink class="btn" to="/storage?tab=summary">状核对页</RouterLink>
        <button class="btn" type="button" @click="exportRows">导出结果聚合归并清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in statCards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <label class="filter-item">
        <span>下载策略</span>
        <select v-model="downloadPolicy">
          <option value="固定下载开始时快照">固定下载开始时快照</option>
          <option value="完成时最新快照">完成时最新快照</option>
        </select>
      </label>
      <label class="filter-item">
        <span>文件格式</span>
        <select v-model="downloadFormat">
          <option value="csv">CSV</option>
          <option value="zip">ZIP（含导出清单）</option>
        </select>
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              class="link"
              type="button"
              :disabled="downloadingTask === row['任务编号']"
              @click="downloadMergedFile(row)"
            >
              {{ downloadingTask === row['任务编号'] ? '下载中…' : '下载归并文件' }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无结果聚合归并数据，可先登记聚合任务</td>
        </tr>
      </tbody>
    </table>

    <section v-if="downloads.length" class="download-panel">
      <h3>归并文件下载清单</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>下载编号</th>
            <th>任务编号</th>
            <th>策略</th>
            <th>开始快照</th>
            <th>完成快照</th>
            <th>清单快照</th>
            <th>文件名称</th>
            <th>归档编号</th>
            <th>归档版本</th>
            <th>状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in downloads" :key="item.下载编号">
            <td>{{ item.下载编号 }}</td>
            <td>{{ item.任务编号 }}</td>
            <td>{{ item.策略 }}</td>
            <td>{{ item.开始快照编号 ?? '—' }}</td>
            <td>{{ item.完成快照编号 ?? '—' }}</td>
            <td>{{ item.展示快照编号 ?? item.快照编号 ?? '—' }}</td>
            <td>{{ item.文件名称 ?? '—' }}</td>
            <td>{{ item.归档编号 ?? '—' }}</td>
            <td>{{ item.归档版本号 ?? '—' }}</td>
            <td>
              {{ item.下载状态 }}
              <span v-if="item.快照冲突" class="error-text">（期间出现新快照，归档已追加版本）</span>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条结果聚合归并记录</span>
      <span v-if="message" class="ok-text">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'
import {
  completeDownload,
  fetchFile,
  getDownload,
  interruptDownload,
  readManifestFromZip,
  saveBlob,
  startDownload,
  type DownloadRecord,
} from '@/api/mergeFiles'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/merge'
const columns = ["任务编号", "采集计划", "分片总数", "已归并数", "归并策略", "输出路径", "数据总量", "任务状态", "当前快照编号"]
const actions = ["启动归并", "重试归并", "归档任务"]
const statuses = ["待归并", "归并中", "已完成", "归并失败"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const message = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const downloadPolicy = ref('固定下载开始时快照')
const downloadFormat = ref('csv')
const downloadingTask = ref('')
const downloads = ref<DownloadRecord[]>([])
const inFlight = new Map<string, Promise<void>>()

const statCards = computed(() => [
  { label: '聚合任务总数', value: total.value },
  { label: '归并中任务', value: rows.value.filter((row) => row['任务状态'] === '归并中').length },
  { label: '已完成任务', value: rows.value.filter((row) => row['任务状态'] === '已完成').length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '聚合任务登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  message.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('结果聚合归并动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并操作失败'
  }
}

/** 重复点击只复用同一份下载记录，不会产生第二份文件。 */
function downloadMergedFile(row: Row) {
  const taskNo = String(row['任务编号'] ?? '')
  if (!taskNo) {
    return
  }
  const current = inFlight.get(taskNo)
  if (current) {
    return current
  }
  const job = runDownload(taskNo).finally(() => {
    inFlight.delete(taskNo)
    downloadingTask.value = ''
  })
  inFlight.set(taskNo, job)
  downloadingTask.value = taskNo
  return job
}

async function runDownload(taskNo: string) {
  errorMessage.value = ''
  message.value = ''
  let record: DownloadRecord | null = null
  try {
    record = await startDownload(taskNo, { 下载策略: downloadPolicy.value, 文件格式: downloadFormat.value })
    if (record.下载状态 !== '已完成') {
      record = await completeDownload(record.下载编号)
    }
    const { blob, fileName } = await fetchFile(record.下载编号)
    if (downloadFormat.value === 'zip') {
      const manifest = await readManifestFromZip(blob)
      if (String(manifest['快照编号']) !== String(record.展示快照编号)) {
        throw new Error('导出清单快照编号与下载清单不一致，已停止保存')
      }
      message.value = `导出清单回读快照 ${manifest['快照编号']}，文件 ${manifest['文件名称']}`
    }
    saveBlob(blob, fileName)
    await refreshDownloads()
    message.value = message.value || `已保存 ${fileName}，快照 ${record.展示快照编号}`
  } catch (error) {
    if (record) {
      try {
        record = await interruptDownload(record.下载编号)
      } catch {
        // 中断登记失败时保留原始报错，不影响重试线索
      }
      await refreshDownloads()
    }
    errorMessage.value = error instanceof Error ? error.message : '归并文件下载失败'
  }
}

async function refreshDownloads() {
  try {
    const response = await request(`${ENDPOINT}/downloads`)
    if (!response.ok) {
      downloads.value = []
      return
    }
    const body = (await response.json()) as { items?: DownloadRecord[] }
    downloads.value = body.items ?? []
  } catch {
    downloads.value = []
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('聚合任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await refreshDownloads()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果聚合归并列表读取失败'
  }
}

onMounted(reload)
</script>
