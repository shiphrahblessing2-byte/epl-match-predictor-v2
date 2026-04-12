"""
football-data.co.uk → Supabase match_stats_basic (NEW TABLE)
✅ Separate from ESPN match_stats. Handles 404s + all errors.
"""
import pandas as pd
from supabase import create_client
import os
import requests
from dotenv import load_dotenv
import logging
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# ✅ WORKING URLs ONLY
URLS = {
    'EPL': {2021: 'https://www.football-data.co.uk/mmz4281/2122/E0.csv',
            2022: 'https://www.football-data.co.uk/mmz4281/2223/E0.csv',
            2023: 'https://www.football-data.co.uk/mmz4281/2324/E0.csv'},
    'LIGA': {2021: 'https://www.football-data.co.uk/mmz4281/2122/SP1.csv',
             2022: 'https://www.football-data.co.uk/mmz4281/2223/SP1.csv',
             2023: 'https://www.football-data.co.uk/mmz4281/2324/SP1.csv'},

}

def safe_download(url):
    """Download with 404 protection."""
    try:
        resp = requests.head(url, timeout=10)
        if resp.status_code != 200:
            log.warning(f"⏭️ 404 skipped: {url}")
            return None
        df = pd.read_csv(url)
        return df
    except:
        log.warning(f"⏭️ Failed: {url}")
        return None

def safe_column(df, col, default=0):
    return pd.to_numeric(df[col].astype(str), errors='coerce').fillna(default) if col in df.columns else pd.Series([default] * len(df), index=df.index)

def safe_date(series):
    dates = pd.to_datetime(series, dayfirst=True, errors='coerce')
    return dates.dt.strftime('%Y-%m-%d').where(dates.notna(), None)

def generate_fixture_id(row, league_key):
    try:
        date_str = pd.to_datetime(row['Date'], dayfirst=True).strftime('%Y%m%d')
    except:
        date_str = '00000000'
    home = str(row.get('HomeTeam', 'unk'))[:3].lower()
    away = str(row.get('AwayTeam', 'unk'))[:3].lower()
    return f"fd_{league_key}_{date_str}_{home}_{away}"[:50]  # fd_ prefix avoids ESPN collision

def import_all():
    total_rows = 0
    for league_key, seasons in URLS.items():
        for supabase_year, csv_url in seasons.items():
            log.info(f"📥 {league_key} {supabase_year}: {csv_url}")
            
            df = safe_download(csv_url)
            if df is None or len(df) == 0:
                log.warning(f"⏭️ Skipped {league_key} {supabase_year}")
                continue
            
            log.info(f"   📊 {len(df)} raw matches")
            
            # Transform data
            df['fixture_id'] = df.apply(lambda row: generate_fixture_id(row, league_key), axis=1)
            df['match_date'] = safe_date(df['Date'])
            df['home_team'] = df['HomeTeam'].fillna('Unknown').astype(str)
            df['away_team'] = df['AwayTeam'].fillna('Unknown').astype(str)
            df['home_goals'] = safe_column(df, 'FTHG')
            df['away_goals'] = safe_column(df, 'FTAG')
            df['home_shots'] = safe_column(df, 'HS')
            df['away_shots'] = safe_column(df, 'AS')
            df['home_shots_on_target'] = safe_column(df, 'HST')
            df['away_shots_on_target'] = safe_column(df, 'AST')
            df['home_possession'] = safe_column(df, 'HS%') / 100
            df['away_possession'] = safe_column(df, 'AS%') / 100
            df['home_corners'] = safe_column(df, 'HC')
            df['away_corners'] = safe_column(df, 'AC')
            df['season'] = supabase_year
            df['league_key'] = league_key
            
            cols = ['fixture_id', 'match_date', 'home_team', 'away_team',
                   'home_goals', 'away_goals', 'home_shots', 'away_shots',
                   'home_shots_on_target', 'away_shots_on_target',
                   'home_possession', 'away_possession',
                   'home_corners', 'away_corners', 'season', 'league_key']
            
            df_final = df[cols].fillna(0)
            rows = df_final.to_dict('records')
            
            # Upsert to NEW TABLE
            for i in range(0, len(rows), 100):
                client.table("match_stats_basic").upsert(
                    rows[i:i+100], on_conflict="fixture_id"
                ).execute()
            
            total_rows += len(rows)
            log.info(f"✅ {league_key} {supabase_year}: {len(rows)} → match_stats_basic")
    
    log.info(f"🎉 TOTAL: {total_rows:,} matches in match_stats_basic!")
    print("\n✅ Ready for feature engineering!")
    print("🔍 Query: SELECT * FROM match_stats_basic LIMIT 5;")

if __name__ == "__main__":
    import_all()