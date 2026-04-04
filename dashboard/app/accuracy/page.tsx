export const dynamic = 'force-dynamic'

const API = process.env.NEXT_PUBLIC_API_URL

async function getAccuracy() {
  if (!API) return null
  try {
    const res = await fetch(`${API}/accuracy`, { cache: 'no-store' })
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
      <p className="text-gray-500 mb-8">Prediction performance over resolved fixtures</p>

      {!data ? (
        <div className="text-center py-20 text-gray-400">No accuracy data available yet</div>
      ) : (
        <div className="space-y-6">
          {/* Summary Cards */}
          <div className="grid grid-cols-3 gap-4">
            <div className="border rounded-xl p-5 text-center bg-white shadow-sm">
              <div className="text-xs text-gray-400 mb-1">Overall Accuracy</div>
              <div className="text-3xl font-bold text-green-600">
                {(data.accuracy * 100).toFixed(1)}%
              </div>
            </div>
            <div className="border rounded-xl p-5 text-center bg-white shadow-sm">
              <div className="text-xs text-gray-400 mb-1">Total Predictions</div>
              <div className="text-3xl font-bold text-gray-700">{data.total}</div>
            </div>
            <div className="border rounded-xl p-5 text-center bg-white shadow-sm">
              <div className="text-xs text-gray-400 mb-1">Correct</div>
              <div className="text-3xl font-bold text-blue-600">{data.correct}</div>
            </div>
          </div>

          {/* Per-Class Breakdown */}
          {data.breakdown && (
            <div className="border rounded-xl p-5 bg-white shadow-sm">
              <h2 className="font-semibold mb-4 text-gray-700">Breakdown by Outcome</h2>
              <div className="space-y-3">
                {Object.entries(data.breakdown).map(([label, stats]: any) => (
                  <div key={label} className="flex items-center gap-4">
                    <span className="w-28 text-sm text-gray-500 capitalize">{label.replace('_', ' ')}</span>
                    <div className="flex-1 bg-gray-100 rounded-full h-3">
                      <div
                        className="bg-green-500 h-3 rounded-full"
                        style={{ width: `${(stats.accuracy * 100).toFixed(0)}%` }}
                      />
                    </div>
                    <span className="text-sm font-semibold w-12 text-right">
                      {(stats.accuracy * 100).toFixed(0)}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </main>
  )
}
