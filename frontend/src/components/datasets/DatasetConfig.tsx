import { useQuery } from '@tanstack/react-query'
import { getProducts } from '@/api'
import ProductCard from './ProductCard'

export default function DatasetConfig() {
  const { data: products = [], isLoading } = useQuery({
    queryKey: ['products'],
    queryFn: getProducts,
  })

  if (isLoading) {
    return <p className="text-xs text-gray-400">Loading datasets…</p>
  }

  return (
    <div>
      <p className="section-title">Datasets</p>
      <div className="flex flex-col gap-2">
        {products.map((p) => (
          <ProductCard key={p.id} product={p} />
        ))}
      </div>
    </div>
  )
}
