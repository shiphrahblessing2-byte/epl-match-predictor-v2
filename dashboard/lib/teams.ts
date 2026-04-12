const TEAM_NAMES: Record<number, string> = {
  // EPL
  33: 'Manchester United', 34: 'Newcastle', 40: 'Liverpool',
  42: 'Arsenal', 47: 'Tottenham', 49: 'Chelsea', 50: 'Manchester City',
  51: 'Brighton', 52: 'Crystal Palace', 55: 'Brentford',
  57: 'Ipswich', 62: 'Sheffield Utd', 63: 'Leeds', 65: 'Nottm Forest',
  66: 'Aston Villa', 67: 'Newcastle', 71: 'West Ham',
  // La Liga
  529: 'Barcelona', 530: 'Atletico Madrid', 531: 'Athletic Club',
  532: 'Valencia', 533: 'Villarreal', 536: 'Sevilla', 541: 'Real Madrid',
  // UCL / UEL
  157: 'Bayern Munich', 165: 'Borussia Dortmund', 489: 'AC Milan',
  492: 'Napoli', 496: 'Juventus', 505: 'Inter Milan',
}

export function getTeamName(id: number | string): string {
  const n = Number(id)
  return TEAM_NAMES[n] ?? `Team ${id}`
}