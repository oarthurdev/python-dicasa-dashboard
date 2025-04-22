
import { useMemo } from 'react'
import { RankingCard } from './RankingCard'

export default function RankingList({ data }) {
  const sortedRanking = useMemo(() => {
    return [...data.ranking].sort((a, b) => b.pontos - a.pontos)
  }, [data.ranking])

  return (
    <div className="space-y-8">
      <h2 className="text-2xl font-semibold text-gray-900">
        Ranking de Corretores
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {sortedRanking.map((broker, index) => (
          <RankingCard 
            key={broker.id}
            rank={index + 1}
            broker={broker}
          />
        ))}
      </div>
    </div>
  )
}
