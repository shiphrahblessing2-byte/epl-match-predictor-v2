export const dynamic = 'force-dynamic'

import { getTeamName } from '@/lib/teams'
import RefreshButton from '@/components/RefreshButton'

const API = process.env.NEXT_PUBLIC_API_URL

const LEAGUES = [
  { key: 'ALL',  name: 'All',               flag: '🌍' },
  { key: 'EPL',  name: 'Premier League',    flag: '🏴󠁧󠁢󠁥󠁮󠁧󠁿' },
  { key: 'UCL',  name: 'Champions League',  flag: '⭐' },
  { key: 'UEL',  name: 'Europa League',     flag: '🟠' },
  { key: 'LIGA', name: 'La Liga',           flag: '🇪🇸' },
]

async function getFixtures() {
  if (!API) return { fixtures: [] }
  try {
    const res = await fetch(
      `${API}/fixtures?weeks_back=1&weeks_ahead=3`,
      { cache: 'no-store' }
    )
    if (!res.ok) return { fixtures: [] }
    return res.json()
  } catch {
    return { fixtures: [] }
  }
}

function groupByDate(fixtures: any[]) {
  return fixtures.reduce((acc: Record<string, any[]>, f) => {
    const date = f.match_date?.slice(0, 10) ?? 'Unknown'
    if (!acc[date]) acc[date] = []
    acc[date].push(f)
    return acc
  }, {})
}

function formatDate(dateStr: string) {
  const d = new Date(dateStr)
  const today = new Date()
  const yesterday = new Date(today); yesterday.setDate(today.getDate() - 1)
  const tomorrow  = new Date(today); tomorrow.setDate(today.getDate() + 1)

  if (d.toDateString() === today.toDateString())     return '📅 Today'
  if (d.toDateString() === yesterday.toDateString()) return '⏪ Yesterday'
  if (d.toDateString() === tomorrow.toDateString())  return '⏩ Tomorrow'
  return d.toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short' })
}

function isPast(dateStr: string) {
  return new Date(dateStr) < new Date()
}

export default async function HomePage({ searchParams }: { searchParams: { league?: string } }) {
  const data    = await getFixtures()
  const allFixtures = data.fixtures ?? []
  const league  = searchParams?.league?.toUpperCase() ?? 'ALL'

  const filtered = league === 'ALL'
    ? allFixtures
    : allFixtures.filter((f: any) => f.league_key === league)

  const grouped = groupByDate(filtered)
  const sortedDates = Object.keys(grouped).sort()

  return (
    <main className="max-w-3xl mx-auto px-4 py-8">

      {/* Header */}
      <div className="flex justify-between items-center mb-1">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
          ⚽ EPL Match Predictor
        </h1>
        <RefreshButton />
      </div>
      <p className="text-sm text-gray-500 dark:text-gray-400 mb-5">
        Predictions and results across Europe's top competitions
      </p>

      {/* League Tabs */}
      <div className="flex gap-2 flex-wrap mb-6">
        {LEAGUES.map(l => (
          <a
            key={l.key}
            href={l.key === 'ALL' ? '/' : `/?league=${l.key}`}
            className={`text-xs font-semibold px-3 py-1.5 rounded-full border transition ${
              league === l.key
                ? 'bg-emerald-600 text-white border-emerald-600'
                : 'bg-white dark:bg-gray-800 text-gray-600 dark:text-gray-300 border-gray-200 dark:border-gray-700 hover:border-emerald-400'
            }`}
          >
            {l.flag} {l.name}
          </a>
        ))}
      </div>

      {filtered.length === 0 ? (
        <div className="text-center py-20 text-gray-400">
          <div className="text-4xl mb-3">🏟️</div>
          <p className="font-medium">No fixtures found</p>
          <p className="text-sm mt-1">Try a different league or check back later</p>
        </div>
      ) : (
        <div className="space-y-8">
          {sortedDates.map(date => (
            <div key={date}>
              {/* Date Header */}
              <div className="flex items-center gap-3 mb-3">
                <span className={`text-sm font-bold ${
                  isPast(date)
                    ? 'text-gray-400 dark:text-gray-500'
                    : 'text-emerald-600 dark:text-emerald-400'
                }`}>
                  {formatDate(date)}
                </span>
                <div className="flex-1 h-px bg-gray-200 dark:bg-gray-700" />
                <span className="text-xs text-gray-400">{grouped[date].length} matches</span>
              </div>

              <div className="space-y-3">
                {grouped[date].map((f: any, i: number) => {
                  const past = f.status_short === 'FT'
                  const pred = f.prediction

                  const homeName = f.home_team || getTeamName(f.home_team_id)
                  const awayName = f.away_team || getTeamName(f.away_team_id)

                  const badgeStyle = !pred ? 'bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400'
                    : pred.predicted === 'home_win' ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400'
                    : pred.predicted === 'draw'     ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-400'
                    :                                 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-400'

                  return (
                    <div key={i} className={`rounded-2xl border overflow-hidden shadow-sm ${
                      past
                        ? 'border-gray-200 dark:border-gray-700 bg-white/60 dark:bg-gray-800/60 opacity-80'
                        : 'border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800'
                    }`}>

                      {/* Top Bar */}
                      <div className="flex items-center justify-between px-4 pt-3 pb-1">
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-gray-400 dark:text-gray-500 uppercase tracking-wide">
                            {f.league_name ?? 'EPL'}
                          </span>
                          {f.league_round && (
                            <span className="text-xs text-gray-300 dark:text-gray-600">· GW{f.league_round}</span>
                          )}
                        </div>
                        {past ? (
                          <span className="text-xs font-bold px-2 py-0.5 rounded-full bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400">
                            FT
                          </span>
                        ) : pred ? (
                          <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${badgeStyle}`}>
                            {pred.predicted_label}
                          </span>
                        ) : (
                          <span className="text-xs text-gray-300 dark:text-gray-600">No prediction</span>
                        )}
                      </div>

                      {/* Teams + Score/Probabilities */}
                      <div className="px-4 py-2">
                        <div className="grid grid-cols-3 items-center">
                          <div>
                            <div className="text-[10px] uppercase tracking-widest text-gray-400 mb-0.5">Home</div>
                            <div className="text-sm font-bold text-gray-900 dark:text-white">{homeName}</div>
                          </div>
                          <div className="text-center">
                            {past ? (
                              <span className="text-2xl font-black text-gray-900 dark:text-white">
                                {f.home_goals} – {f.away_goals}
                              </span>
                            ) : (
                              <span className="text-lg font-black text-gray-200 dark:text-gray-700">VS</span>
                            )}
                          </div>
                          <div className="text-right">
                            <div className="text-[10px] uppercase tracking-widest text-gray-400 mb-0.5">Away</div>
                            <div className="text-sm font-bold text-gray-900 dark:text-white">{awayName}</div>
                          </div>
                        </div>
                      </div>

                      {/* Prediction Bars (upcoming EPL only) */}
                      {!past && pred && (
                        <div className="grid grid-cols-3 gap-px bg-gray-100 dark:bg-gray-700 mx-4 mb-3 rounded-xl overflow-hidden">
                          {[
                            { label: 'Home Win', val: pred.probabilities.home_win, color: 'text-emerald-600 dark:text-emerald-400', bar: 'bg-emerald-500' },
                            { label: 'Draw',     val: pred.probabilities.draw,      color: 'text-amber-500 dark:text-amber-400',    bar: 'bg-amber-400' },
                            { label: 'Away Win', val: pred.probabilities.away_win,  color: 'text-blue-600 dark:text-blue-400',      bar: 'bg-blue-500' },
                          ].map(item => (
                            <div key={item.label} className="bg-white dark:bg-gray-800 p-2 text-center">
                              <div className="text-[10px] uppercase tracking-wide text-gray-400 mb-1">{item.label}</div>
                              <div className={`text-base font-bold ${item.color}`}>
                                {(item.val * 100).toFixed(0)}%
                              </div>
                              <div className="mt-1 h-1 bg-gray-100 dark:bg-gray-700 rounded-full overflow-hidden">
                                <div className={`h-full ${item.bar} rounded-full`}
                                  style={{ width: `${(item.val * 100).toFixed(0)}%` }} />
                              </div>
                            </div>
                          ))}
                        </div>
                      )}

                      {/* Match time for upcoming */}
                      {!past && (
                        <div className="px-4 pb-3 text-right text-xs text-gray-400">
                          {new Date(f.match_date).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })} UTC
                          {pred && <> · Confidence: <strong className="text-gray-600 dark:text-gray-300">{(pred.confidence * 100).toFixed(1)}%</strong></>}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            </div>
          ))}
        </div>
      )}
    </main>
  )
}
