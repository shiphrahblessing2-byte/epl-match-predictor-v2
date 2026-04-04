'use client'

import { useRouter } from 'next/navigation'
import { useState } from 'react'

export default function RefreshButton() {
  const router = useRouter()
  const [loading, setLoading] = useState(false)

  const handleRefresh = () => {
    setLoading(true)
    router.refresh()
    setTimeout(() => setLoading(false), 1500)
  }

  return (
    <button
      onClick={handleRefresh}
      disabled={loading}
      className="flex items-center gap-2 text-sm px-4 py-2 rounded-lg border border-gray-200 hover:bg-gray-50 transition disabled:opacity-50"
    >
      <span className={loading ? 'animate-spin' : ''}>🔄</span>
      {loading ? 'Refreshing...' : 'Refresh'}
    </button>
  )
}
