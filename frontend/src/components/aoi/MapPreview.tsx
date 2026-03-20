import { MapContainer, TileLayer, GeoJSON, useMap } from 'react-leaflet'
import { useEffect } from 'react'
import * as L from 'leaflet'

interface FitBoundsProps {
  geojson: GeoJSON.FeatureCollection
}

function FitBounds({ geojson }: FitBoundsProps) {
  const map = useMap()
  useEffect(() => {
    const layer = L.geoJSON(geojson)
    const bounds = layer.getBounds()
    if (bounds.isValid()) map.fitBounds(bounds, { padding: [20, 20] })
  }, [geojson, map])
  return null
}

interface Props {
  geojson: GeoJSON.FeatureCollection | null
}

export default function MapPreview({ geojson }: Props) {
  return (
    <MapContainer
      center={[20, 0]}
      zoom={2}
      style={{ height: '100%', width: '100%' }}
      scrollWheelZoom
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {geojson && (
        <>
          <GeoJSON
            key={JSON.stringify(geojson).slice(0, 80)}
            data={geojson}
            style={{ color: '#16a34a', weight: 2, fillOpacity: 0.15 }}
          />
          <FitBounds geojson={geojson} />
        </>
      )}
    </MapContainer>
  )
}
