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
      <body className="bg-gray-50 min-h-screen">
        <nav className="bg-white border-b px-6 py-3 flex gap-6 text-sm font-medium">
          <Link href="/" className="hover:text-green-600 transition">🏠 Predictions</Link>
          <Link href="/accuracy" className="hover:text-green-600 transition">📊 Accuracy</Link>
        </nav>
        {children}
      </body>
    </html>
  )
}
