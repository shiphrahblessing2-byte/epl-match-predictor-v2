# live_data_pipeline.py — 2022-2024 + UCL/UEL (Free Plan Max)
import requests
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import logging
import os 

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
headers = {"x-apisports-key": os.getenv("API_FOOTBALL_KEY")}

# ✅ Full free coverage
SEASONS = [2022, 2023, 2024]  # Free plan max
LEAGUES = {
    "EPL": 39,
    "LIGA": 140,
    "UCL": 2,
    "UEL": 3
}

KEY_TEAMS = {
    "EPL": {
        "teams": [
            "Manchester United", "Newcastle", "Bournemouth", "Fulham",
            "Wolves", "Liverpool", "Southampton", "Arsenal", "Everton",
            "Leicester", "Tottenham", "West Ham", "Chelsea", "Manchester City",
            "Brighton", "Crystal Palace", "Brentford", "Ipswich",
            "Nottingham Forest", "Aston Villa"
        ],
        "league_id": 39
    },
    "LIGA": {
        "teams": [
            "Barcelona", "Atletico Madrid", "Athletic Club", "Valencia",
            "Villarreal", "Las Palmas", "Sevilla", "Leganes", "Celta Vigo",
            "Espanyol", "Real Madrid", "Alaves", "Real Betis", "Getafe",
            "Girona", "Real Sociedad", "Valladolid", "Osasuna",
            "Rayo Vallecano", "Mallorca"
        ],
        "league_id": 140
    },
    "UCL": {
        "teams": [
            "Liverpool", "Arsenal", "Manchester City", "Aston Villa", "Lille",
            "Paris Saint Germain", "Monaco", "Stade Brestois 29", "Bayern München",
            "Borussia Dortmund", "Bayer Leverkusen", "VfB Stuttgart", "RB Leipzig",
            "PSV Eindhoven", "Feyenoord", "Benfica", "Sporting CP", "Celtic",
            "Rangers", "Vikingur Reykjavik", "Bodo/Glimt", "Jagiellonia",
            "The New Saints", "Malmo FF", "Dinamo Minsk", "FC Midtjylland",
            "Twente", "AC Milan", "Juventus", "Atalanta", "Bologna", "Inter",
            "Barcelona", "Atletico Madrid", "Real Madrid", "Girona",
            "Shakhtar Donetsk", "Qarabag", "FCSB", "Slavia Praha", "BSC Young Boys",
            "Ludogorets", "Club Brugge KV", "Red Bull Salzburg", "Dynamo Kyiv",
            "FK Partizan", "FK Crvena Zvezda", "Maccabi Tel Aviv", "FC Lugano",
            "Fenerbahçe", "PAOK", "Dinamo Zagreb", "Sparta Praha", "Sturm Graz",
            "Galatasaray", "HJK Helsinki", "Ferencvarosi TC", "Shamrock Rovers",
            "Slovan Bratislava", "Lincoln Red Imps FC", "FC Differdange 03",
            "Flora Tallinn", "Ordabasy", "KI Klaksvik", "UE Santa Coloma",
            "Dinamo Batumi", "Pyunik Yerevan", "Union St. Gilloise", "Apoel Nicosia",
            "Petrocub", "Egnatia Rrogozhinë", "Borac Banja Luka", "Dečić",
            "Panevėžys", "Rīgas FS", "Struga", "Celje", "Hamrun Spartans",
            "Virtus", "Larne", "Ballkani"
        ],
        "league_id": 2
    },
    "UEL": {
        "teams": [
            "Manchester United", "Tottenham", "Lyon", "Nice", "1899 Hoffenheim",
            "Eintracht Frankfurt", "Ajax", "AZ Alkmaar", "FC Porto", "SC Braga",
            "Kilmarnock", "Heart Of Midlothian", "Rangers", "Bodo/Glimt", "Molde",
            "Jagiellonia", "Wisla Krakow", "The New Saints", "IF Elfsborg",
            "Malmo FF", "Dinamo Minsk", "FC Midtjylland", "Twente", "Lazio",
            "AS Roma", "Athletic Club", "Real Sociedad", "Beşiktaş", "Maribor",
            "Olympiakos Piraeus", "Anderlecht", "Qarabag", "FCSB", "Slavia Praha",
            "HNK Rijeka", "Ludogorets", "Plzen", "Sheriff Tiraspol", "Dynamo Kyiv",
            "FK Partizan", "Maccabi Tel Aviv", "FC Lugano", "Fenerbahçe",
            "Panathinaikos", "PAOK", "Botev Plovdiv", "Galatasaray", "Zira",
            "Ferencvarosi TC", "Shamrock Rovers", "Lincoln Red Imps FC",
            "KI Klaksvik", "Vojvodina", "UE Santa Coloma", "Cercle Brugge",
            "Rapid Vienna", "Trabzonspor", "Lask Linz", "Union St. Gilloise",
            "Silkeborg", "Servette FC", "Apoel Nicosia", "FK Tobol Kostanay",
            "Petrocub", "Paks", "TSC Backa Topola", "Borac Banja Luka", "Pafos",
            "Ružomberok", "Panevėžys", "Rīgas FS", "Celje", "Maccabi Petah Tikva",
            "Kryvbas KR", "Llapi", "Corvinul Hunedoara"
        ],
        "league_id": 3
    }
}

TEAM_DICTS ={
  "EPL": {
    "Manchester United": 33,
    "Newcastle": 34,
    "Bournemouth": 35,
    "Fulham": 36,
    "Wolves": 39,
    "Liverpool": 40,          # ✅ ADDED
    "Southampton": 41,
    "Arsenal": 42,            # ✅ ADDED  
    "Everton": 45,
    "Leicester": 46,
    "Tottenham": 47,
    "West Ham": 48,
    "Chelsea": 49,
    "Manchester City": 50,    # ✅ ADDED
    "Brighton": 51,
    "Crystal Palace": 52,
    "Brentford": 55,
    "Ipswich": 57,
    "Nottingham Forest": 65,
    "Aston Villa": 66
  },
  "LIGA": {
    "Barcelona": 529,
    "Atletico Madrid": 530,
    "Athletic Club": 531,
    "Valencia": 532,
    "Villarreal": 533,
    "Las Palmas": 534,
    "Sevilla": 536,
    "Leganes": 537,
    "Celta Vigo": 538,
    "Espanyol": 540,
    "Real Madrid": 541,
    "Alaves": 542,
    "Real Betis": 543,
    "Getafe": 546,
    "Girona": 547,
    "Real Sociedad": 548,
    "Valladolid": 720,
    "Osasuna": 727,
    "Rayo Vallecano": 728,
    "Mallorca": 798
  },
  "UCL": {
    "Liverpool": 40,
    "Arsenal": 42,
    "Manchester City": 50,
    "Aston Villa": 66,
    "Lille": 79,
    "Paris Saint Germain": 85,
    "Monaco": 91,
    "Stade Brestois 29": 106,
    "Bayern M\u00fcnchen": 157,
    "Borussia Dortmund": 165,
    "Bayer Leverkusen": 168,
    "VfB Stuttgart": 172,
    "RB Leipzig": 173,
    "PSV Eindhoven": 197,
    "Feyenoord": 209,
    "Benfica": 211,
    "Sporting CP": 228,
    "Celtic": 247,
    "Rangers": 257,
    "Vikingur Reykjavik": 278,
    "Bodo/Glimt": 327,
    "Jagiellonia": 336,
    "The New Saints": 354,
    "Malmo FF": 375,
    "Dinamo Minsk": 394,
    "FC Midtjylland": 397,
    "Twente": 415,
    "AC Milan": 489,
    "Juventus": 496,
    "Atalanta": 499,
    "Bologna": 500,
    "Inter": 505,
    "Barcelona": 529,
    "Atletico Madrid": 530,
    "Real Madrid": 541,
    "Girona": 547,
    "Shakhtar Donetsk": 550,
    "Qarabag": 556,
    "FCSB": 559,
    "Slavia Praha": 560,
    "BSC Young Boys": 565,
    "Ludogorets": 566,
    "Club Brugge KV": 569,
    "Red Bull Salzburg": 571,
    "Dynamo Kyiv": 572,
    "FK Partizan": 573,
    "FK Crvena Zvezda": 598,
    "Maccabi Tel Aviv": 604,
    "FC Lugano": 606,
    "Fenerbah\u00e7e": 611,
    "PAOK": 619,
    "Dinamo Zagreb": 620,
    "Sparta Praha": 628,
    "Sturm Graz": 637,
    "Galatasaray": 645,
    "HJK Helsinki": 649,
    "Ferencvarosi TC": 651,
    "Shamrock Rovers": 652,
    "Slovan Bratislava": 656,
    "Lincoln Red Imps FC": 667,
    "FC Differdange 03": 684,
    "Flora Tallinn": 687,
    "Ordabasy": 692,
    "KI Klaksvik": 701,
    "UE Santa Coloma": 703,
    "Dinamo Batumi": 705,
    "Pyunik Yerevan": 709,
    "Union St. Gilloise": 1393,
    "Apoel Nicosia": 2247,
    "Petrocub": 2271,
    "Egnatia Rrogozhin\u00eb": 3327,
    "Borac Banja Luka": 3364,
    "De\u010di\u0107": 3745,
    "Panev\u0117\u017eys": 3874,
    "R\u012bgas FS": 4160,
    "Struga": 4346,
    "Celje": 4360,
    "Hamrun Spartans": 4626,
    "Virtus": 5308,
    "Larne": 5354,
    "Ballkani": 12733
  },
  "UEL": {
    "Manchester United": 33,
    "Tottenham": 47,
    "Lyon": 80,
    "Nice": 84,
    "1899 Hoffenheim": 167,
    "Eintracht Frankfurt": 169,
    "Ajax": 194,
    "AZ Alkmaar": 201,
    "FC Porto": 212,
    "SC Braga": 217,
    "Kilmarnock": 250,
    "Heart Of Midlothian": 254,
    "Rangers": 257,
    "Bodo/Glimt": 327,
    "Molde": 329,
    "Jagiellonia": 336,
    "Wisla Krakow": 338,
    "The New Saints": 354,
    "IF Elfsborg": 372,
    "Malmo FF": 375,
    "Dinamo Minsk": 394,
    "FC Midtjylland": 397,
    "Twente": 415,
    "Lazio": 487,
    "AS Roma": 497,
    "Athletic Club": 531,
    "Real Sociedad": 548,
    "Be\u015fikta\u015f": 549,
    "Maribor": 552,
    "Olympiakos Piraeus": 553,
    "Anderlecht": 554,
    "Qarabag": 556,
    "FCSB": 559,
    "Slavia Praha": 560,
    "HNK Rijeka": 561,
    "Ludogorets": 566,
    "Plzen": 567,
    "Sheriff Tiraspol": 568,
    "Dynamo Kyiv": 572,
    "FK Partizan": 573,
    "Maccabi Tel Aviv": 604,
    "FC Lugano": 606,
    "Fenerbah\u00e7e": 611,
    "Panathinaikos": 617,
    "PAOK": 619,
    "Botev Plovdiv": 634,
    "Galatasaray": 645,
    "Zira": 648,
    "Ferencvarosi TC": 651,
    "Shamrock Rovers": 652,
    "Lincoln Red Imps FC": 667,
    "KI Klaksvik": 701,
    "Vojvodina": 702,
    "UE Santa Coloma": 703,
    "Cercle Brugge": 741,
    "Rapid Vienna": 781,
    "Trabzonspor": 998,
    "Lask Linz": 1026,
    "Union St. Gilloise": 1393,
    "Silkeborg": 2073,
    "Servette FC": 2184,
    "Apoel Nicosia": 2247,
    "FK Tobol Kostanay": 2259,
    "Petrocub": 2271,
    "Paks": 2390,
    "TSC Backa Topola": 2646,
    "Borac Banja Luka": 3364,
    "Pafos": 3403,
    "Ru\u017eomberok": 3549,
    "Panev\u0117\u017eys": 3874,
    "R\u012bgas FS": 4160,
    "Celje": 4360,
    "Maccabi Petah Tikva": 4495,
    "Kryvbas KR": 6489,
    "Llapi": 14395,
    "Corvinul Hunedoara": 20034
  }
}

def fetch_live_fixtures():
    """Real-time livescore (current season)."""
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"live": "all"}
    
    resp = requests.get(url, headers=headers, params=params)
    data = resp.json()
    
    fixtures = []
    for f in data.get("response", []):
        fixtures.append({
            "api_id": f['fixture']['id'],
            "league_key": f['league']['name'][:3],
            "match_date": f['fixture']['date'],
            "home_team": f['teams']['home']['name'],
            "away_team": f['teams']['away']['name'],
            "home_score": f['goals']['home'],
            "away_score": f['goals']['away'],
            "status": f['fixture']['status']['short'],
            "minute": f['fixture']['status'].get('elapsed', 0)
        })
    
    if fixtures:
        client.table("live_fixtures").upsert(fixtures).execute()
        log.info(f"✅ Live: {len(fixtures)} matches")
    return len(fixtures)

def fetch_historical_fixtures(season):
    """Historical fixtures 2022-2024 for training — with pagination + error handling."""
    all_fixtures = []
    
    for league_name, league_id in LEAGUES.items():
        page = 1
        while True:
            url = "https://v3.football.api-sports.io/fixtures"
            params = {"league": league_id, "season": season, "page": page}
            
            resp = requests.get(url, headers=headers, params=params)
            data = resp.json()
            
            # ✅ Catch API-level errors
            if data.get("errors"):
                log.error(f"❌ API error for {league_name} season {season} page {page}: {data['errors']}")
                break
            
            results = data.get("response", [])
            
            # ✅ No more pages
            if not results:
                log.info(f"  {league_name} {season}: {len([f for f in all_fixtures if f['league_key'] == league_name])} fixtures fetched")
                break
            
            for f in results:
                all_fixtures.append({
                    "api_id": f['fixture']['id'],
                    "league_key": league_name,
                    "season": season,
                    "match_date": f['fixture']['date'],
                    "home_team": f['teams']['home']['name'],
                    "away_team": f['teams']['away']['name'],
                    "home_score": f['goals']['home'],
                    "away_score": f['goals']['away'],
                    "status": f['fixture']['status']['short']
                })
            
            # ✅ Check if more pages exist
            total_pages = data.get("paging", {}).get("total", 1)
            log.info(f"  {league_name} {season} page {page}/{total_pages}: {len(results)} fixtures")
            
            if page >= total_pages:
                break
            
            page += 1
            time.sleep(1)  # rate limit between pages
        
        time.sleep(1)  # rate limit between leagues
    
    if all_fixtures:
        # ✅ Deduplicate before upsert
        df = pd.DataFrame(all_fixtures).drop_duplicates(subset=["api_id"], keep="last")
        unique_fixtures = df.to_dict("records")
        
        client.table("historical_fixtures").upsert(
            unique_fixtures,
            on_conflict="api_id"  # ✅ prevents 23505 on re-run
        ).execute()
        log.info(f"✅ Season {season}: {len(unique_fixtures)} fixtures saved")
    else:
        log.warning(f"⚠️  Season {season}: 0 fixtures returned — check API key/quota")
    
    return len(all_fixtures)

def fetch_injuries(season):
    injuries = []
    for league_name, league_data in KEY_TEAMS.items():
        for team_name in league_data["teams"][:3]:
            team_id = TEAM_DICTS.get(league_name, {}).get(team_name, 42)
            
            url = "https://v3.football.api-sports.io/injuries"
            params = {"team": team_id, "season": season}
            
            resp = requests.get(url, headers=headers, params=params)
            data = resp.json()
            
            for player in data.get("response", [])[:5]:
                injuries.append({
                    "player_name": player['player']['name'],
                    "team_name": team_name,
                    "league_key": league_name,
                    "season": season,
                    "injury_type": player.get('type', {}).get('name', 'Unknown'),
                    "api_team_id": team_id
                })
            time.sleep(6)
    
    if injuries:
        # ✅ Deduplicate by the unique constraint columns before upsert
        seen = set()
        unique_injuries = []
        for inj in injuries:
            key = (inj["player_name"], inj["team_name"], inj["season"])
            if key not in seen:
                seen.add(key)
                unique_injuries.append(inj)
        
        client.table("live_injuries").upsert(
            unique_injuries,
            on_conflict="player_name,team_name,season"
        ).execute()
        log.info(f"✅ Injuries {season}: {len(unique_injuries)} players (deduplicated from {len(injuries)})")
    
    return len(injuries)

# 🏃‍♂️ MAIN PIPELINE
def run_pipeline():
    log.info("🚀 Multi-season pipeline (2022-2024 + UCL/UEL)")
    
    # 1. Real-time livescore
    live_count = fetch_live_fixtures()
    
    # 2. Historical fixtures (model training)
    hist_total = 0
    for season in SEASONS:
        hist_total += fetch_historical_fixtures(season)
    
    # 3. Injuries all seasons
    inj_total = 0
    for season in SEASONS:
        inj_total += fetch_injuries(season)
    
    log.info(f"🎉 Pipeline complete!")
    log.info(f"  Live matches: {live_count}")
    log.info(f"  Historical fixtures: {hist_total}")
    log.info(f"  Injuries: {inj_total}")

def verify_pipeline():
    log.info("\n📊 Verification — Supabase row counts:")
    
    tables = {
        "live_fixtures": None,
        "live_injuries": None,
        "historical_fixtures": None,
    }
    
    for table in tables:
        try:
            result = client.table(table).select("*", count="exact").limit(1).execute()
            log.info(f"  {table}: {result.count} rows")
        except Exception as e:
            log.error(f"  {table}: ERROR — {e}")
    
    # Per-season breakdown for historical
    try:
        for season in SEASONS:
            result = (
                client.table("historical_fixtures")
                .select("*", count="exact")
                .eq("season", season)
                .limit(1)
                .execute()
            )
            log.info(f"  historical_fixtures season {season}: {result.count} rows")
    except Exception as e:
        log.error(f"  historical_fixtures season breakdown: ERROR — {e}")

# Add to run_pipeline():
def run_pipeline():
    log.info("🚀 Multi-season pipeline (2022-2024 + UCL/UEL)")
    live_count = fetch_live_fixtures()
    hist_total = sum(fetch_historical_fixtures(s) for s in SEASONS)
    inj_total = sum(fetch_injuries(s) for s in SEASONS)
    
    log.info(f"🎉 Pipeline complete! Live:{live_count} | Historical:{hist_total} | Injuries:{inj_total}")
    verify_pipeline()  # ✅ confirm actual DB state

if __name__ == "__main__":
    run_pipeline()