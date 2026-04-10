"""
Central config — loads all environment variables from .env
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env from project root ────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")   # ← THE MISSING LINE

# ── Paths ──────────────────────────────────────────────────
MODELS_DIR       = BASE_DIR / "models"
DATA_RAW_DIR     = BASE_DIR / "data" / "raw"
DATA_INTERIM_DIR = BASE_DIR / "data" / "interim"
DATA_PROC_DIR    = BASE_DIR / "data" / "processed"
REPORTS_DIR      = BASE_DIR / "reports"
CACHE_DIR        = BASE_DIR / "data" / "cache"

for _dir in [DATA_RAW_DIR, DATA_INTERIM_DIR, DATA_PROC_DIR,
             MODELS_DIR, REPORTS_DIR, CACHE_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

# ── API-Football ───────────────────────────────────────────
API_FOOTBALL_KEY  = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE = "https://v3.football.api-sports.io"
API_FOOTBALL_HOST = "v3.football.api-sports.io"

# ── Supabase ───────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# ── EPL identifiers ────────────────────────────────────────
EPL_LEAGUE_ID = int(os.getenv("EPL_LEAGUE_ID", 39))
EPL_SEASON    = int(os.getenv("EPL_SEASON", 2024))

# ── Validation ─────────────────────────────────────────────
def validate_config():
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            f"Copy .env.example to .env and fill in the values."
        )