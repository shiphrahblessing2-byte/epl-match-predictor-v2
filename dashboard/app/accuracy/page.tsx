export const dynamic = 'force-dynamic'

const API = process.env.NEXT_PUBLIC_API_URL

async function getAccuracy() {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL
  if (!apiUrl) return null   // ← guard against undefined at build time

  try {
    const res = await fetch(`${apiUrl}/accuracy`, {
      next: { revalidate: 300 }
    })
    if (!res.ok) return null
    return res.json()
  } catch {
    return null
  }
}

export default async function AccuracyPage() {
  const data = await getAccuracy()

  return (
    <main className="max-w-4xl mx-auto px-4 py-10">
      <h1 className="text-3xl font-bold mb-2">📊 Model Accuracy</h1>
      <p className="text-gray-500 mb-8">
        Rolling accuracy over the last 10 graded predictions
      </p>

      {!data || data.rolling_accuracy === null ? (
        <div className="text-center py-20 text-gray-400">
          No graded predictions yet — check back after matchweek
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-6">
          <div className="border rounded-xl p-6 shadow-sm bg-white text-center">
            <div className="text-5xl font-black text-green-600 mb-2">
              {(data.rolling_accuracy * 100).toFixed(1)}%
            </div>
            <div className="text-gray-500">Rolling Accuracy</div>
            <div className="text-xs text-gray-400 mt-1">
              vs 33.3% random baseline
            </div>
          </div>
          <div className="border rounded-xl p-6 shadow-sm bg-white text-center">
            <div className="text-5xl font-black text-blue-600 mb-2">
              {data.correct}/{data.total}
            </div>
            <div className="text-gray-500">Correct Predictions</div>
            <div className="text-xs text-gray-400 mt-1">
              Last {data.sample_size} matches
            </div>
          </div>
        </div>
      )}
    </main>
  )
}