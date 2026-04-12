'use client'
import { useRouter } from 'next/navigation'
import { useState } from 'react'

export default function RefreshButton() {
  const router = useRouter()
  const [spin, setSpin] = useState(false)

  const refresh = () => {
    setSpin(true)
    router.refresh()
    setTimeout(() => setSpin(false), 1200)
  }

  return (
    <button
      onClick={refresh}
      aria-label="Refresh fixtures"
      className="p-2 rounded-lg text-gray-400 hover:text-emerald-600 hover:bg-emerald-50 dark:hover:bg-emerald-900/20 transition"
    >
      <svg
        width="16" height="16" viewBox="0 0 24 24" fill="none"
        stroke="currentColor" strokeWidth="2"
        className={spin ? 'animate-spin' : ''}
      >
        <path d="M23 4v6h-6"/>
        <path d="M1 20v-6h6"/>
        <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>
      </svg>
    </button>
  )
}