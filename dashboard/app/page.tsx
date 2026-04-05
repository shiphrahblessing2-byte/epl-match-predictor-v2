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
    <main className="max-w-3xl mx-auto px-4 py-10">

      {/* Header */}
      <div className="flex justify-between items-center mb-1">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white tracking-tight">
          ⚽ EPL Match Predictor
        </h1>
        <RefreshButton />
      </div>
      <p className="text-sm text-gray-500 dark:text-gray-400 mb-8">
        AI predictions for upcoming Premier League fixtures
      </p>

      {predictions.length === 0 ? (
        <div className="text-center py-24 text-gray-400">
          <div className="text-4xl mb-4">🏟️</div>
          <p className="text-lg font-medium">No upcoming fixtures found</p>
          <p className="text-sm mt-1">Check back closer to the next matchweek</p>
        </div>
      ) : (
        <div className="space-y-4">
          {predictions.map((p: any, i: number) => {
            const homeTeam = getTeamName(p.home_team_id)
            const awayTeam = getTeamName(p.away_team_id)
            const matchDate = new Date(p.predicted_at).toLocaleDateString('en-GB', {
              weekday: 'short', day: 'numeric', month: 'short'
            })

            const badgeStyle =
              p.predicted === 'home_win'
                ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400'
                : p.predicted === 'draw'
                ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-400'
                : 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-400'

            return (
              <div
                key={i}
                className="rounded-2xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 shadow-sm overflow-hidden"
              >
                {/* Card Header */}
                <div className="flex items-center justify-between px-5 pt-4 pb-2">
                  <span className="text-xs font-medium text-gray-400 dark:text-gray-500 uppercase tracking-wide">
                    {matchDate} · Fixture #{p.fixture_id}
                  </span>
                  <span className={`text-xs font-semibold px-3 py-1 rounded-full ${badgeStyle}`}>
                    {p.predicted_label}
                  </span>
                </div>

                {/* Teams Row */}
                <div className="grid grid-cols-3 items-center px-5 py-3">
                  {/* Home Team */}
                  <div className="text-left">
                    <div className="text-[10px] uppercase tracking-widest text-gray-400 dark:text-gray-500 mb-1 font-medium">
                      Home
                    </div>
                    <div className="text-base font-bold text-gray-900 dark:text-white leading-tight">
                      {homeTeam}
                    </div>
                  </div>

                  {/* VS */}
                  <div className="text-center">
                    <span className="text-xl font-black text-gray-300 dark:text-gray-600">VS</span>
                  </div>

                  {/* Away Team */}
                  <div className="text-right">
                    <div className="text-[10px] uppercase tracking-widest text-gray-400 dark:text-gray-500 mb-1 font-medium">
                      Away
                    </div>
                    <div className="text-base font-bold text-gray-900 dark:text-white leading-tight">
                      {awayTeam}
                    </div>
                  </div>
                </div>

                {/* Divider */}
                <div className="mx-5 border-t border-gray-100 dark:border-gray-700" />

                {/* Probability Bars */}
                <div className="grid grid-cols-3 gap-px bg-gray-100 dark:bg-gray-700 mx-5 my-3 rounded-xl overflow-hidden">
                  <div className="bg-white dark:bg-gray-800 p-3 text-center">
                    <div className="text-[10px] uppercase tracking-wide text-gray-400 dark:text-gray-500 mb-1">
                      Home Win
                    </div>
                    <div className="text-xl font-bold text-emerald-600 dark:text-emerald-400">
                      {(p.probabilities.home_win * 100).toFixed(0)}%
                    </div>
                    {/* Mini progress bar */}
                    <div className="mt-2 h-1 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-emerald-500 rounded-full"
                        style={{ width: `${(p.probabilities.home_win * 100).toFixed(0)}%` }}
                      />
                    </div>
                  </div>

                  <div className="bg-white dark:bg-gray-800 p-3 text-center">
                    <div className="text-[10px] uppercase tracking-wide text-gray-400 dark:text-gray-500 mb-1">
                      Draw
                    </div>
                    <div className="text-xl font-bold text-amber-500 dark:text-amber-400">
                      {(p.probabilities.draw * 100).toFixed(0)}%
                    </div>
                    <div className="mt-2 h-1 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-amber-400 rounded-full"
                        style={{ width: `${(p.probabilities.draw * 100).toFixed(0)}%` }}
                      />
                    </div>
                  </div>

                  <div className="bg-white dark:bg-gray-800 p-3 text-center">
                    <div className="text-[10px] uppercase tracking-wide text-gray-400 dark:text-gray-500 mb-1">
                      Away Win
                    </div>
                    <div className="text-xl font-bold text-blue-600 dark:text-blue-400">
                      {(p.probabilities.away_win * 100).toFixed(0)}%
                    </div>
                    <div className="mt-2 h-1 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-blue-500 rounded-full"
                        style={{ width: `${(p.probabilities.away_win * 100).toFixed(0)}%` }}
                      />
                    </div>
                  </div>
                </div>

                {/* Footer */}
                <div className="px-5 pb-4 text-right">
                  <span className="text-xs text-gray-400 dark:text-gray-500">
                    Model confidence: <strong className="text-gray-600 dark:text-gray-300">
                      {(p.confidence * 100).toFixed(1)}%
                    </strong>
                  </span>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </main>
  )
}
