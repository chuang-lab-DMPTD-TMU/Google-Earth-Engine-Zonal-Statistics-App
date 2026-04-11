import { useCallback, useEffect, useState } from 'react'
import { useDropzone } from 'react-dropzone'
import { useMutation } from '@tanstack/react-query'
import { uploadAOI } from '@/api'
import type { AOIInfo } from '@/types'
import { useAppStore } from '@/store'
import MapPreview from './MapPreview'
import SnakemakeLog from './SnakemakeLog'
import HelpTooltip from '@/components/ui/HelpTooltip'

// Columns whose names suggest they're good unique identifiers.
const ID_PATTERN = /^(id|fid|uid|gid|oid|code|key|admin|name|region|area|zone|objectid|globalid|feature_id|featureid|geoid)$|_id$|^id_/i

function suggestIdColumn(columns: string[]): string | null {
  return columns.find(c => ID_PATTERN.test(c)) ?? null
}

interface Props {
  runId: string | null
  existingAoiName?: string
}

export default function AOIUpload({ runId, existingAoiName }: Props) {
  const [aoi, setAoi] = useState<AOIInfo | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [uploadPct, setUploadPct] = useState<number | null>(null)

  const { idColumn, setIdColumn } = useAppStore()

  // Auto-select a sensible default when a new AOI is loaded.
  useEffect(() => {
    if (!aoi) return
    const suggestion = suggestIdColumn(aoi.columns)
    setIdColumn(suggestion)
  }, [aoi, setIdColumn])

  const mutation = useMutation({
    mutationFn: ({ file }: { file: File }) => {
      if (!runId) throw new Error('Select or create a run first')
      setUploadPct(0)
      return uploadAOI(runId, file, setUploadPct)
    },
    onSuccess: (info) => {
      setAoi(info)
      setError(null)
      setUploadPct(null)
    },
    onError: (err: Error) => {
      setError(err.message)
      setUploadPct(null)
    },
  })

  const onDrop = useCallback(
    (accepted: File[]) => {
      if (accepted[0]) mutation.mutate({ file: accepted[0] })
    },
    [mutation],
  )

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/zip': ['.zip'],
      'application/geo+json': ['.geojson'],
      'application/octet-stream': ['.parquet'],
    },
    maxFiles: 1,
    disabled: !runId || mutation.isPending,
  })

  const suggestion = aoi ? suggestIdColumn(aoi.columns) : null
  const hasDuplicates = idColumn && aoi ? aoi.column_has_duplicates[idColumn] : false

  return (
    <div className="flex flex-col gap-3 h-full">
      <div>
        <p className="section-title flex items-center gap-1.5">Area of Interest <HelpTooltip text="Upload the geographic region to download satellite data for. Accepts shapefiles (.zip), GeoJSON, or GeoParquet files." direction="right" /></p>

        {!runId && (
          <p className="text-xs text-gray-400">
            Select or create a run session first.
          </p>
        )}

        {runId && existingAoiName && (
          <div className="flex items-center gap-2 px-3 py-2 rounded-md bg-gray-50 border border-gray-200">
            <span className="text-gray-400 text-xs">📁</span>
            <span className="text-xs text-gray-600 font-mono truncate">{existingAoiName}</span>
            <span className="text-xs text-gray-400 ml-auto shrink-0">locked</span>
          </div>
        )}

        {runId && !existingAoiName && (
          <div
            {...getRootProps()}
            className={[
              'border-2 border-dashed rounded-lg p-5 text-center cursor-pointer transition-colors',
              isDragActive
                ? 'border-brand-400 bg-brand-50'
                : 'border-gray-300 hover:border-brand-400 hover:bg-gray-50',
              mutation.isPending ? 'opacity-60 cursor-wait' : '',
            ].join(' ')}
          >
            <input {...getInputProps()} />
            <p className="text-sm text-gray-500">
              {mutation.isPending
                ? uploadPct !== null && uploadPct < 100
                  ? `Uploading… ${uploadPct}%`
                  : 'Processing…'
                : isDragActive
                ? 'Drop file here'
                : 'Drop shapefile (.zip), GeoJSON, or GeoParquet'}
            </p>
            {mutation.isPending && (
              <div className="mt-3 w-full bg-gray-200 rounded-full h-1.5 overflow-hidden">
                {uploadPct !== null && uploadPct < 100 ? (
                  <div
                    className="h-full bg-brand-400 rounded-full transition-all duration-200"
                    style={{ width: `${uploadPct}%` }}
                  />
                ) : (
                  <div className="h-full bg-brand-400 rounded-full animate-[indeterminate_1.4s_ease-in-out_infinite] w-1/2" />
                )}
              </div>
            )}
            {!mutation.isPending && <p className="text-xs text-gray-400 mt-1">or click to browse</p>}
          </div>
        )}

        {error && <p className="mt-2 text-xs text-red-600">{error}</p>}

        {aoi && !existingAoiName && (
          <div className="mt-2 text-xs text-gray-600 space-y-0.5">
            <p>
              <span className="font-medium">{aoi.feature_count}</span> features ·{' '}
              <span className="font-medium">{aoi.crs}</span>
            </p>
            <p className="text-gray-400">
              Bounds: [{aoi.bounds.map((v) => v.toFixed(4)).join(', ')}]
            </p>
          </div>
        )}

        {/* ID column picker — shown after a successful upload, before the run is locked */}
        {aoi && !existingAoiName && aoi.columns.length > 0 && (
          <div className="mt-3 space-y-1.5">
            <label className="flex items-center gap-1.5 text-xs font-medium text-gray-700">
              Feature ID column
              <HelpTooltip
                text="Each feature needs a unique identifier. This column becomes region_id in the output and is used to join statistics back to your geometries."
                direction="right"
              />
            </label>

            <div className="relative">
              <select
                value={idColumn ?? ''}
                onChange={(e) => setIdColumn(e.target.value || null)}
                className="w-full text-xs rounded-md border border-gray-300 bg-white px-2.5 py-1.5 pr-7 text-gray-800 shadow-sm focus:border-brand-400 focus:outline-none focus:ring-1 focus:ring-brand-400 appearance-none"
              >
                <option value="">— select a column —</option>
                {aoi.columns.map((col) => {
                  const isSuggested = col === suggestion
                  const samples     = aoi.column_samples[col] ?? []
                  const dups        = aoi.column_has_duplicates[col]
                  const label = [
                    col,
                    isSuggested ? '(suggested)' : '',
                    dups        ? '⚠ duplicates' : '',
                  ].filter(Boolean).join('  ')
                  return (
                    <option key={col} value={col} title={samples.length ? `e.g. ${samples.join(', ')}` : undefined}>
                      {label}
                    </option>
                  )
                })}
              </select>
              {/* chevron icon */}
              <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-gray-400">
                <svg className="h-3 w-3" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M2 4l4 4 4-4" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </span>
            </div>

            {/* Sample values for the selected column */}
            {idColumn && (aoi.column_samples[idColumn] ?? []).length > 0 && (
              <p className="text-xs text-gray-400">
                Sample values:{' '}
                <span className="font-mono text-gray-500">
                  {aoi.column_samples[idColumn].join(', ')}
                </span>
              </p>
            )}

            {/* Duplicate warning */}
            {hasDuplicates && (
              <p className="flex items-start gap-1 text-xs text-amber-600">
                <svg className="mt-px h-3.5 w-3.5 shrink-0" viewBox="0 0 16 16" fill="currentColor">
                  <path d="M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1zm0 3a.75.75 0 0 1 .75.75v3.5a.75.75 0 0 1-1.5 0v-3.5A.75.75 0 0 1 8 4zm0 7.5a.875.875 0 1 1 0-1.75.875.875 0 0 1 0 1.75z" />
                </svg>
                This column has duplicate values. Features will be disambiguated automatically by appending a suffix (e.g. <span className="font-mono">Kenya_1</span>).
              </p>
            )}

            {/* No column selected warning */}
            {!idColumn && (
              <p className="text-xs text-amber-600">
                No ID column selected — row index will be used as the feature identifier.
              </p>
            )}
          </div>
        )}
      </div>

      {/* Map or Snakemake log depending on whether geometry is locked */}
      <div className="flex-1 min-h-0">
        {existingAoiName && runId
          ? <SnakemakeLog runId={runId} />
          : (
            <div className="rounded-lg overflow-hidden border border-gray-200 h-full min-h-64">
              <MapPreview geojson={aoi?.geojson_preview ?? null} bounds={aoi?.bounds ?? null} />
            </div>
          )
        }
      </div>
    </div>
  )
}
