
'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@supabase/supabase-js'
import DashboardLayout from '@/components/DashboardLayout'
import RankingList from '@/components/RankingList'
import BrokerDashboard from '@/components/BrokerDashboard'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
)

export default function Home() {
  const [currentPage, setCurrentPage] = useState('ranking')
  const [data, setData] = useState({
    brokers: [],
    leads: [],
    activities: [],
    ranking: []
  })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      const [brokersData, leadsData, activitiesData, rankingData] = await Promise.all([
        supabase.from('brokers').select('*'),
        supabase.from('leads').select('*'),
        supabase.from('activities').select('*'),
        supabase.from('broker_points').select('*')
      ])

      setData({
        brokers: brokersData.data || [],
        leads: leadsData.data || [],
        activities: activitiesData.data || [],
        ranking: rankingData.data || []
      })
      setLoading(false)
    } catch (error) {
      console.error('Error loading data:', error)
      setLoading(false)
    }
  }

  if (loading) {
    return <div>Carregando...</div>
  }

  return (
    <DashboardLayout>
      {currentPage === 'ranking' ? (
        <RankingList data={data} />
      ) : currentPage.startsWith('broker/') ? (
        <BrokerDashboard 
          brokerId={parseInt(currentPage.split('/')[1])} 
          data={data} 
        />
      ) : null}
    </DashboardLayout>
  )
}
