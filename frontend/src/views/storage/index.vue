<template>
  <section class="page" data-module="storage">
    <header class="page-head">
      <div>
        <h2>采集存储分层管理</h2>
        <p class="page-desc">维护采集存储的分层配置、格式选择与迁移规则，并承接归并文件的归档与快照核对。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记存储策略</button>
        <button class="btn" type="button" @click="exportRows">修复归并导出</button>
      </div>
    </header>

    <nav class="tab-bar">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="btn"
        :class="{ primary: activeTab === tab.key }"
        type="button"
        @click="selectTab(tab.key)"
      >
        {{ tab.label }}
      </button>
    </nav>

    <div v-if="activeTab === 'storage'">
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
            </td>
          </tr>
          <tr v-if="!rows.length">
            <td :colspan="columns.length + 1" class="empty-state">暂无采集存储分层数据，可先登记存储策略</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else-if="activeTab === 'merge-files'">
      <p class="page-desc">归并文件数据只来自已完成的下载，文件和清单共用同一个已提交快照编号。</p>
      <table class="data-table">
        <thead>
          <tr>
            <th>下载编号</th>
            <th>任务编号</th>
            <th>快照编号</th>
            <th>清单展示快照</th>
            <th>文件名称</th>
            <th>条目数</th>
            <th>数据总量</th>
            <th>归档编号</th>
            <th>归档版本</th>
            <th>存储层级</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in mergeFileRows" :key="item.下载编号">
            <td>{{ item.下载编号 }}</td>
            <td>{{ item.任务编号 }}</td>
            <td>{{ item.快照编号 }}</td>
            <td>{{ item.展示快照编号 }}</td>
            <td>{{ item.文件名称 }}</td>
            <td>{{ item.条目数 }}</td>
            <td>{{ item.数据总量 }}</td>
            <td>{{ item.归档编号 }}</td>
            <td>{{ item.归档版本号 }}</td>
            <td>{{ item.存储层级 }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="downloadFile(item.下载编号)">按原文件名回读</button>
            </td>
          </tr>
          <tr v-if="!mergeFileRows.length">
            <td colspan="11" class="empty-state">还没有已完成的归并文件，可在结果聚合归并页下载后再归档</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else-if="activeTab === 'summary'">
      <p class="page-desc">聚合进度、下载清单与存储归档逐项核对，任一处快照编号不同都会标为不一致。</p>
      <table class="data-table">
        <thead>
          <tr>
            <th>任务编号</th>
            <th>聚合进度快照</th>
            <th>下载清单快照</th>
            <th>清单展示快照</th>
            <th>归档快照</th>
            <th>归档版本</th>
            <th>已归并数</th>
            <th>下载条目数</th>
            <th>数据总量</th>
            <th>核对结果</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in summaryRows" :key="item.任务编号">
            <td>{{ item.任务编号 }}</td>
            <td>{{ item.聚合进度快照编号 ?? '—' }}</td>
            <td>{{ item.下载清单快照编号 ?? '—' }}</td>
            <td>{{ item.展示快照编号 ?? '—' }}</td>
            <td>{{ item.归档快照编号 ?? '—' }}</td>
            <td>{{ item.归档版本号 ?? '—' }}</td>
            <td>{{ item.已归并数 }}</td>
            <td>{{ item.下载条目数 }}</td>
            <td>{{ item.数据总量 }}</td>
            <td :class="item.一致 ? '' : 'error-text'">
              {{ item.核对结果 }}
              <span v-if="item.问题.length">：{{ item.问题.join('；') }}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else>
      <p class="page-desc">存储分层登记的归并文件版本，归档条目与下载清单保留同一个快照编号。</p>
      <table class="data-table">
        <thead>
          <tr>
            <th>归档编号</th>
            <th>任务编号</th>
            <th>快照编号</th>
            <th>版本号</th>
            <th>下载编号</th>
            <th>文件名称</th>
            <th>文件格式</th>
            <th>归档方式</th>
            <th>存储层级</th>
            <th>归档状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in archiveRows" :key="item.归档编号">
            <td>{{ item.归档编号 }}</td>
            <td>{{ item.任务编号 }}</td>
            <td>{{ item.快照编号 }}</td>
            <td>{{ item.版本号 }}</td>
            <td>{{ item.下载编号 }}</td>
            <td>{{ item.文件名称 }}</td>
            <td>{{ item.文件格式 }}</td>
            <td>{{ item.归档方式 }}</td>
            <td>{{ item.存储层级 }}</td>
            <td>{{ item.归档状态 }}</td>
          </tr>
          <tr v-if="!archiveRows.length">
            <td colspan="10" class="empty-state">还没有归并文件归档条目</td>
          </tr>
        </tbody>
      </table>
    </div>

    <footer class="page-foot">
      <span v-if="activeTab === 'storage'">共 {{ total }} 条采集存储分层记录</span>
      <span v-if="message" class="ok-text">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import { fetchJson, request } from '@/api/client'
import { fetchFile, saveBlob, type MergeFileEntry, type MergeSummaryEntry } from '@/api/mergeFiles'

type Row = Record<string, string | number | null>
type ArchiveRow = {
  归档编号: string
  任务编号: string
  快照编号: string
  版本号: number
  下载编号: string
  文件名称: string
  文件格式: string
  归档方式: string
  存储层级: string
  归档状态: string
}

const ENDPOINT = '/api/storage'
const columns = ["策略编号", "存储层级", "文件格式", "压缩算法", "保留天数", "迁移规则", "清理方式", "策略状态"]
const actions = ["启用策略", "暂停策略", "关闭策略"]
const statuses = ["草稿", "已生效", "已暂停", "已关闭"]
const tabs = [
  { key: 'storage', label: '存储策略' },
  { key: 'merge-files', label: '归并文件数据' },
  { key: 'summary', label: '状核对页' },
  { key: 'archives', label: '归并文件归档' },
]

const rows = ref<Row[]>([])
const total = ref(0)
const route = useRoute()
const errorMessage = ref('')
const message = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const activeTab = ref(tabs.some((tab) => tab.key === route.query.tab) ? String(route.query.tab) : 'storage')
const mergeFileRows = ref<MergeFileEntry[]>([])
const summaryRows = ref<MergeSummaryEntry[]>([])
const archiveRows = ref<ArchiveRow[]>([])

const statCards = computed(() => [
  { label: '存储策略总数', value: total.value },
  { label: '已生效策略', value: rows.value.filter((row) => row['策略状态'] === '已生效').length },
  { label: '已暂停策略', value: rows.value.filter((row) => row['策略状态'] === '已暂停').length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '存储策略登记入口尚未接入审批流'
}

async function selectTab(key: string) {
  activeTab.value = key
  await loadTab()
}

async function loadTab() {
  errorMessage.value = ''
  try {
    if (activeTab.value === 'merge-files') {
      const payload = await fetchJson<{ items: MergeFileEntry[] }>(`${ENDPOINT}/merge-files`)
      mergeFileRows.value = payload.items ?? []
    } else if (activeTab.value === 'summary') {
      const payload = await fetchJson<{ items: MergeSummaryEntry[] }>(`${ENDPOINT}/merge-summary`)
      summaryRows.value = payload.items ?? []
    } else if (activeTab.value === 'archives') {
      const payload = await fetchJson<{ items: ArchiveRow[] }>(`${ENDPOINT}/archives`)
      archiveRows.value = payload.items ?? []
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归并文件数据读取失败'
  }
}

async function downloadFile(downloadId: string) {
  errorMessage.value = ''
  message.value = ''
  try {
    const { blob, fileName } = await fetchFile(downloadId)
    saveBlob(blob, fileName)
    message.value = `已按原文件名保存 ${fileName}`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归并文件回读失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('采集存储分层动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集存储分层操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('存储策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集存储分层列表读取失败'
  }
}

onMounted(async () => {
  await reload()
  await loadTab()
})
</script>
