import type { Metadata } from 'next'
import './globals.css'
import Link from 'next/link'

export const metadata: Metadata = {
  title: 'EPL Match Predictor',
  description: 'AI-powered Premier League predictions',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="bg-gray-50 dark:bg-gray-900 min-h-screen transition-colors">
        <nav className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-6 py-3 flex gap-6 text-sm font-medium sticky top-0 z-10">
          <Link href="/" className="text-gray-700 dark:text-gray-200 hover:text-emerald-600 dark:hover:text-emerald-400 transition flex items-center gap-1.5">
            🏠 Predictions
          </Link>
          <Link href="/accuracy" className="text-gray-700 dark:text-gray-200 hover:text-emerald-600 dark:hover:text-emerald-400 transition flex items-center gap-1.5">
            📊 Accuracy
          </Link>
        </nav>
        {children}
      </body>
    </html>
  )
}
