<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="meta.key === 'pending_segment'"
              class="link"
              type="button"
              :data-testid="`detail-${row.id}`"
              @click="showSettlementDetail(row)"
            >
              结算明细
            </button>
            <button
              v-if="meta.key === 'feed_scheduler'"
              class="link"
              type="button"
              :data-testid="`rounds-${row.id}`"
              @click="showRoundSummary(row)"
            >
              轮次核对
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="settlementDetail" class="modal-backdrop" data-testid="settlement-detail">
      <div class="modal">
        <h3>结算明细</h3>
        <dl class="detail-list">
          <template v-for="item in detailItems" :key="item.label">
            <dt>{{ item.label }}</dt>
            <dd>{{ item.value }}</dd>
          </template>
        </dl>
        <div class="page-actions">
          <button class="btn" type="button" @click="settlementDetail = null">关闭</button>
        </div>
      </div>
    </div>

    <div v-if="roundSummary" class="modal-backdrop" data-testid="round-summary">
      <div class="modal">
        <h3>轮次核对 · {{ roundSummary.task_name }}</h3>
        <table class="data-table">
          <thead>
            <tr>
              <th>轮次</th>
              <th>状态</th>
              <th>派发页面</th>
              <th>回执页面</th>
              <th>完成页面</th>
              <th>完成分块</th>
              <th>待结算分块</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in roundSummary.rounds" :key="item.round">
              <td>第 {{ item.round }} 轮</td>
              <td>{{ item.status }}</td>
              <td>{{ item.派发页面数 }}</td>
              <td>{{ item.回执页面数 }}</td>
              <td>{{ item.完成页面数 }}</td>
              <td>{{ item.完成分块数 }}</td>
              <td>{{ item.待结算分块数 }}</td>
            </tr>
          </tbody>
        </table>
        <p class="status-legend">
          已结算页面 {{ roundSummary.totals.settled_pages }} 个、文本块
          {{ roundSummary.totals.settled_blocks }} 个；待结算
          {{ roundSummary.totals.unsettled_blocks }} 个。
        </p>
        <div class="page-actions">
          <button class="btn" type="button" @click="roundSummary = null">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  loadRoundSummary,
  loadSettlementDetail,
  moduleMeta,
  runAction as applyAction,
  type RoundSummary,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

const settlementDetail = ref<Record<string, unknown> | null>(null)
const roundSummary = ref<RoundSummary | null>(null)
const detailLabels: Record<string, string> = {
  settlement_no: '结算单号',
  task_name: '关联任务',
  page_uri: '来源页面',
  block_id: '分块ID',
  settlement_status: '结算状态',
  verification: '验证结果',
  dispatch_round: '派发轮次',
  receipt_round: '回执轮次',
  completion_round: '完成轮次',
  same_round_pause_conflict: '暂停同轮冲突',
  explanation: '完成轮次说明',
}
const detailItems = computed(() =>
  Object.entries(settlementDetail.value ?? {})
    .filter(([key]) => key in detailLabels)
    .map(([key, value]) => ({ label: detailLabels[key], value: String(value ?? '—') })),
)

async function showSettlementDetail(row: EntryRow) {
  errorMessage.value = ''
  try {
    settlementDetail.value = await loadSettlementDetail(Number(row.id))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结算明细读取失败'
  }
}

async function showRoundSummary(row: EntryRow) {
  errorMessage.value = ''
  try {
    roundSummary.value = await loadRoundSummary(Number(row.id))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '轮次核对读取失败'
  }
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
