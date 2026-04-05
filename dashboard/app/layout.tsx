import type { Metadata } from 'next'
import './globals.css'
import Link from 'next/link'
import ThemeToggle from '@/app/components/ThemeToggle'

export const metadata: Metadata = {
  title: 'Match Predictor — EPL, La Liga, UCL, UEL',
  description: 'AI-powered football predictions across Europe's top competitions',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="bg-gray-50 dark:bg-gray-900 min-h-screen transition-colors">
        <nav className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-6 py-3 flex items-center justify-between sticky top-0 z-10">
          <div className="flex gap-6 text-sm font-medium">
            <Link href="/"
              className="text-gray-700 dark:text-gray-200 hover:text-emerald-600 dark:hover:text-emerald-400 transition flex items-center gap-1.5">
              ⚽ Predictions
            </Link>
            <Link href="/accuracy"
              className="text-gray-700 dark:text-gray-200 hover:text-emerald-600 dark:hover:text-emerald-400 transition flex items-center gap-1.5">
              📊 Accuracy
            </Link>
          </div>
          <ThemeToggle />
        </nav>
        {children}
      </body>
    </html>
  )
}