// lib/teams.ts
export const TEAMS: Record<number, { name: string; short: string }> = {
  33:  { name: 'Manchester United', short: 'MAN UTD' },
  34:  { name: 'Newcastle United',  short: 'NEWCASTLE' },
  40:  { name: 'Liverpool',         short: 'LIVERPOOL' },
  41:  { name: 'Southampton',       short: 'SOUTHAMPTON' },
  42:  { name: 'Arsenal',           short: 'ARSENAL' },
  45:  { name: 'Everton',           short: 'EVERTON' },
  46:  { name: 'Leicester City',    short: 'LEICESTER' },
  47:  { name: 'Tottenham',         short: 'SPURS' },
  48:  { name: 'West Ham',          short: 'WEST HAM' },
  49:  { name: 'Chelsea',           short: 'CHELSEA' },
  50:  { name: 'Manchester City',   short: 'MAN CITY' },
  51:  { name: 'Brighton',          short: 'BRIGHTON' },
  52:  { name: 'Crystal Palace',    short: 'CRYSTAL P' },
  55:  { name: 'Brentford',         short: 'BRENTFORD' },
  57:  { name: 'Ipswich Town',      short: 'IPSWICH' },
  62:  { name: 'Sheffield United',  short: 'SHEFF UTD' },
  63:  { name: 'Leeds United',      short: 'LEEDS' },
  65:  { name: 'Nottm Forest',      short: 'FOREST' },
  66:  { name: 'Aston Villa',       short: 'ASTON VILLA' },
  67:  { name: 'Fulham',            short: 'FULHAM' },
  71:  { name: 'Wolverhampton',     short: 'WOLVES' },
  73:  { name: 'Bournemouth',       short: 'BOURNEMOUTH' },
}

export function getTeamName(id: number): string {
  return TEAMS[id]?.name ?? `Team ${id}`
}

export function getTeamShort(id: number): string {
  return TEAMS[id]?.short ?? `#${id}`
}