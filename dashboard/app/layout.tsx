import type { Metadata } from "next"
import { Inter } from "next/font/google"
import "./globals.css"
import Link from "next/link"

const inter = Inter({ subsets: ["latin"] })

export const metadata: Metadata = {
  title: "EPL Match Predictor",
  description: "AI-powered EPL match outcome predictions",
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={`${inter.className} bg-gray-50 min-h-screen`}>
        <nav className="bg-white border-b px-6 py-4 flex gap-6 items-center">
          <span className="font-bold text-lg">⚽ EPL Predictor</span>
          <Link href="/"
            className="text-sm text-gray-600 hover:text-green-600">
            Predictions
          </Link>
          <Link href="/accuracy"
            className="text-sm text-gray-600 hover:text-green-600">
            Accuracy
          </Link>
          <a
            href="https://shiphrahb-epl-match-predictor.hf.space/docs"
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm text-gray-600 hover:text-green-600 ml-auto">
            API Docs ↗
          </a>
        </nav>
        {children}
      </body>
    </html>
  )
}