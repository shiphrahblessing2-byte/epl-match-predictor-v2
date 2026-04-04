export const dynamic = 'force-dynamic'

import { getTeamName } from '@/lib/teams'
import RefreshButton from '@/components/RefreshButton'

const API = process.env.NEXT_PUBLIC_API_URL

async function getUpcoming() {
  if (!API) return { predictions: [] }
  try {
    const res = await fetch(`${API}/upcoming`, { cache: 'no-store' })
    if (!res.ok) return { predictions: [] }
    return res.json()
  } catch {
    return { predictions: [] }
  }
}

export default async function HomePage() {
  const data = await getUpcoming()
  const predictions = data.predictions ?? []

  return (
    <main className="max-w-4xl mx-auto px-4 py-10">
      <div className="flex justify-between items-center mb-2">
        <h1 className="text-3xl font-bold">⚽ EPL Match Predictor</h1>
        <RefreshButton />
      </div>
      <p className="text-gray-500 mb-8">AI predictions for upcoming Premier League fixtures</p>

      {predictions.length === 0 ? (
        <div className="text-center py-20 text-gray-400">No upcoming fixtures found</div>
      ) : (
        <div className="space-y-4">
          {predictions.map((p: any, i: number) => (
            <div key={i} className="border rounded-xl p-5 shadow-sm bg-white">
              <div className="flex justify-between items-center mb-3">
                <div>
                  <span className="text-sm text-gray-400 font-mono">
                    Fixture #{p.fixture_id} · {new Date(p.predicted_at).toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short' })}
                  </span>
                  <div className="text-lg font-semibold mt-1">
                    {getTeamName(p.home_team_id)}
                    <span className="text-gray-400 mx-2 font-normal text-sm">vs</span>
                    {getTeamName(p.away_team_id)}
                  </div>
                </div>
                <span className={`text-sm font-semibold px-3 py-1 rounded-full ${
                  p.predicted === 'home_win' ? 'bg-green-100 text-green-700'
                  : p.predicted === 'draw'   ? 'bg-yellow-100 text-yellow-700'
                  :                            'bg-blue-100 text-blue-700'
                }`}>
                  {p.predicted_label}
                </span>
              </div>

              <div className="grid grid-cols-3 gap-2 text-center mb-3">
                <div className="bg-gray-50 rounded-lg p-3">
                  <div className="text-xs text-gray-400 mb-1">Home Win</div>
                  <div className="text-2xl font-bold text-green-600">{(p.probabilities.home_win * 100).toFixed(0)}%</div>
                </div>
                <div className="bg-gray-50 rounded-lg p-3">
                  <div className="text-xs text-gray-400 mb-1">Draw</div>
                  <div className="text-2xl font-bold text-yellow-600">{(p.probabilities.draw * 100).toFixed(0)}%</div>
                </div>
                <div className="bg-gray-50 rounded-lg p-3">
                  <div className="text-xs text-gray-400 mb-1">Away Win</div>
                  <div className="text-2xl font-bold text-blue-600">{(p.probabilities.away_win * 100).toFixed(0)}%</div>
                </div>
              </div>

              <div className="text-right text-xs text-gray-400">
                Confidence: {(p.confidence * 100).toFixed(1)}%
              </div>
            </div>
          ))}
        </div>
      )}
    </main>
  )
}
