import type { GeeKeyStatus } from '@/types'
import { useAppStore } from '@/store'
import CredentialsPanel from '@/components/credentials/CredentialsPanel'
import RunSelector from '@/components/runs/RunSelector'
import DatasetConfig from '@/components/datasets/DatasetConfig'
import RunDatasetView from '@/components/datasets/RunDatasetView'
import EventFeed from '@/components/runs/EventFeed'

interface SidebarProps {
  keyStatus: GeeKeyStatus | null
}

export default function Sidebar({ keyStatus }: SidebarProps) {
  const { pendingRun, activeRunId } = useAppStore()

  const keyValid    = keyStatus?.valid ?? false
  const hasRun      = keyValid && (!!activeRunId || !!pendingRun.run_id)

  return (
    <aside className="w-80 flex-shrink-0 bg-white border-r border-gray-200 flex flex-col overflow-y-auto">
      {/* Header */}
      <div className="px-4 py-4 border-b border-gray-200 bg-brand-700">
        <h1 className="text-white font-semibold text-base leading-tight">
          GEE Web App
        </h1>
        <p className="text-brand-200 text-xs mt-0.5">
          Google Earth Engine Downloader
        </p>
      </div>

      <div className="flex flex-col gap-4 px-4 py-4 flex-1">
        {/* Step 1 – Credentials */}
        <CredentialsPanel keyStatus={keyStatus} />

        {/* Step 2 – Run session (only once credentials are valid) */}
        {keyValid && <RunSelector />}

        {/* Active run indicator */}
        {hasRun && (
          <div className="flex items-center gap-2 px-3 py-2 rounded-md bg-brand-50 border border-brand-200">
            <span className="text-brand-500 text-xs font-medium shrink-0">Active run</span>
            <span className="font-mono text-xs text-brand-800 truncate">
              {activeRunId ?? pendingRun.run_id}
            </span>
          </div>
        )}

        {/* Step 3 – Dataset configuration (editable for new runs, read-only for existing) */}
        {hasRun && (
          activeRunId ? <RunDatasetView runId={activeRunId} /> : <DatasetConfig />
        )}

        {/* Global event feed – always visible once credentials are valid */}
        {keyValid && <EventFeed />}
      </div>
    </aside>
  )
}
