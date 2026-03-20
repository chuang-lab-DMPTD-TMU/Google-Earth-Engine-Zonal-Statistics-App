import type { RunStatus } from '@/types'

const CLASSES: Record<RunStatus, string> = {
  running:   'badge-running',
  completed: 'badge-completed',
  failed:    'badge-failed',
  stopped:   'badge-stopped',
  unknown:   'badge bg-gray-100 text-gray-500',
}

export default function RunStatusBadge({ status }: { status: RunStatus }) {
  return <span className={CLASSES[status]}>{status}</span>
}
