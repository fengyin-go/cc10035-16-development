import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 待结算分块的结算明细：该页在哪一轮派发、哪一轮回执、哪一轮才算完成。 */
export async function loadSettlementDetail(id: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/pending_segment/${id}/detail`)
  if (!response.ok) throw new Error('结算明细读取失败')
  return await response.json()
}

export type RoundSummary = {
  ok: boolean
  task_id: number
  task_name?: string
  task_status?: string
  current_round?: number | null
  rounds: {
    round: number
    status: string
    派发页面数: number
    回执页面数: number
    完成页面数: number
    完成分块数: number
    待结算页面数: number
    待结算分块数: number
  }[]
  totals: Record<string, number>
}

/** 按轮次核对页面与文本块数量。 */
export async function loadRoundSummary(taskId: number): Promise<RoundSummary> {
  const response = await request(`/api/feed_scheduler/${taskId}/rounds`)
  if (!response.ok) throw new Error('轮次核对读取失败')
  return await response.json()
}
