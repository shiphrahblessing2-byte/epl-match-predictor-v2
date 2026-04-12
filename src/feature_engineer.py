"""
Layer 3 — Feature Engineering
Reads from Supabase matches table, computes features,
writes to Supabase matches_features table.

Usage:
    python3 src/feature_engineer.py            # all seasons
    python3 src/feature_engineer.py --dry-run  # compute only, no DB write
    python3 src/feature_engineer.py --season 2024
"""
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from supabase import create_client

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    SUPABASE_URL,
    SUPABASE_KEY,
    EPL_SEASON,
    DATA_INTERIM_DIR,
    validate_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("feature_engineer.log"),
    ],
)
log = logging.getLogger(__name__)

ROLLING_WINDOW = 5   # last 5 matches for rolling stats

# ── Add this right after imports, before any def functions ────────────────

def fetch_table(client, table: str, select: str = "*") -> pd.DataFrame:
    """Paginated fetch — always gets ALL rows regardless of table size."""
    rows, offset, limit = [], 0, 1000
    while True:
        resp = (
            client.table(table)
            .select(select)
            .range(offset, offset + limit - 1)
            .execute()
        )
        batch = resp.data
        if not batch:
            break
        rows.extend(batch)
        log.info(f"  {table}: fetched {len(rows)} rows...")
        if len(batch) < limit:
            break
        offset += limit
    df = pd.DataFrame(rows)
    log.info(f"  ✅ {table}: total {len(df)} rows loaded")
    return df


# ══════════════════════════════════════════════════════════
# LOAD DATA FROM SUPABASE
# ══════════════════════════════════════════════════════════

def load_matches(client) -> pd.DataFrame:
    log.info("📥 Loading matches from Supabase...")
    all_rows = []
    page     = 0
    size     = 1000

    while True:
        resp = (
            client.table("matches")
            .select("*")
            .order("match_date", desc=False)
            .range(page * size, (page + 1) * size - 1)
            .execute()
        )
        batch = resp.data
        all_rows.extend(batch)
        log.info(f"  Page {page + 1}: {len(batch)} rows fetched")
        if len(batch) < size:
            break
        page += 1

    df = pd.DataFrame(all_rows)
    df["match_date"] = pd.to_datetime(df["match_date"], utc=True)
    log.info(f"✅ Loaded {len(df)} matches across seasons: {sorted(df['season'].unique())}")
    return df


# ══════════════════════════════════════════════════════════
# FEATURE FUNCTIONS
# ══════════════════════════════════════════════════════════

def rolling_averages(team_df: pd.DataFrame, window: int = ROLLING_WINDOW) -> pd.DataFrame:
    """
    Compute rolling stats for a single team's matches.
    closed='left' ensures we NEVER include the current match — only past matches.
    This is the most critical rule to prevent data leakage.
    """
    team_df = team_df.sort_values("match_date").copy()

    team_df["goals_scored_rolling"]   = (
        team_df["goals_scored"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    team_df["goals_conceded_rolling"] = (
        team_df["goals_conceded"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    team_df["wins_rolling"]           = (
        team_df["win"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    team_df["clean_sheets_rolling"]   = (
        team_df["clean_sheet"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    # ← ADD — venue-specific win rate (home wins only for home rows, away wins for away rows)
    team_df["venue_wins_rolling"] = (
        team_df["venue_win"]
        .rolling(window, min_periods=1, closed="left")
        .mean()
    )
    return team_df


def win_streak(team_df: pd.DataFrame) -> pd.DataFrame:
    """
    Count consecutive wins BEFORE each match (not including current).
    Resets to 0 on draw or loss.
    """
    team_df = team_df.sort_values("match_date").copy()
    streaks = []
    current = 0
    for win in team_df["win"]:
        streaks.append(current)      # record streak BEFORE this match
        current = current + 1 if win == 1 else 0
    team_df["win_streak"] = streaks
    return team_df


def momentum(team_df: pd.DataFrame, window: int = ROLLING_WINDOW) -> pd.DataFrame:
    """
    Weighted recent form — more recent matches count more.
    Weights: [1, 2, 3, 4, 5] so last match = weight 5.
    closed='left' — no data leakage.
    """
    team_df   = team_df.sort_values("match_date").copy()
    weights   = list(range(1, window + 1))   # [1, 2, 3, 4, 5]
    form_vals = []

    wins = team_df["win"].tolist()
    for i in range(len(wins)):
        past = wins[max(0, i - window):i]    # up to 5 results BEFORE current
        if not past:
            form_vals.append(0.0)
        else:
            w = weights[-len(past):]         # align weights to available history
            form_vals.append(
                sum(p * wt for p, wt in zip(past, w)) / sum(w)
            )

    team_df["form_momentum"] = form_vals
    return team_df

def build_injury_features(df: pd.DataFrame, client) -> pd.DataFrame:
    """
    Joins live_injuries to compute injured player count per team per match.
    Uses season + team_name match. Returns injury_count columns.
    """
    log.info("🏥 Joining injury data...")
    inj_resp = client.table("live_injuries").select(
        "team_name, league_key, season, injury_type"
    ).execute()
    inj_df = pd.DataFrame(inj_resp.data)

    if inj_df.empty:
        log.warning("  ⚠️ No injury data found")
        df["home_injuries"] = 0
        df["away_injuries"] = 0
        return df

    # Count injuries per team per season
    inj_counts = (
        inj_df.groupby(["team_name", "league_key", "season"])
        .size()
        .reset_index(name="injury_count")
    )

    # Join to home team
    df = df.merge(
        inj_counts.rename(columns={
            "team_name": "home_team", "injury_count": "home_injuries"
        }),
        on=["home_team", "league_key", "season"], how="left"
    )

    # Join to away team
    df = df.merge(
        inj_counts.rename(columns={
            "team_name": "away_team", "injury_count": "away_injuries"
        }),
        on=["away_team", "league_key", "season"], how="left"
    )

    df["home_injuries"] = df["home_injuries"].fillna(0).astype(int)
    df["away_injuries"] = df["away_injuries"].fillna(0).astype(int)
    df["injury_diff"] = df["home_injuries"] - df["away_injuries"]

    log.info(f"  ✅ Injury features joined: {inj_counts['injury_count'].sum():.0f} total injuries")
    return df

def build_stats_features(df: pd.DataFrame, client) -> pd.DataFrame:
    log.info("📊 Joining combined match stats...")

    # ✅ FIXED — paginate to get ALL rows, not just first 1000
    rows, offset, limit = [], 0, 1000
    while True:
        resp = client.table("match_stats").select("*") \
            .range(offset, offset + limit - 1).execute()
        batch = resp.data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    espn_df = pd.DataFrame(rows)
    log.info(f"  Loaded {len(espn_df)} rows from match_stats")

    if espn_df.empty:
        log.warning("  ⚠️ match_stats is empty")
        return df

    # Force same type on join key
    df["fixture_id"]      = df["fixture_id"].astype(int)
    espn_df["fixture_id"] = espn_df["fixture_id"].astype(int)

    # Drop duplicate cols before merge to avoid _x/_y suffixes
    overlap_cols = [c for c in espn_df.columns if c in df.columns and c != "fixture_id"]
    if overlap_cols:
        df = df.drop(columns=overlap_cols, errors="ignore")

    df = df.merge(espn_df, on="fixture_id", how="left")

    # Verify coverage after merge
    for col in ["home_shots", "home_possession", "home_corners", "home_shots_on_target"]:
        if col in df.columns:
            pct = df[col].notna().mean() * 100
            log.info(f"  {col} coverage after join: {pct:.1f}%")

    log.info(f"  ✅ match_stats joined: {len(espn_df)} rows fetched, {len(df)} features rows")
    return df

def build_fbref_features(df: pd.DataFrame, client) -> pd.DataFrame:
    """Joins fbref_matchlogs for xG, progressive passes, cards."""
    log.info("📊 Joining FBref matchlogs...")

    fbref_resp = client.table("fbref_matchlogs").select("*").execute()
    fbref_df = pd.DataFrame(fbref_resp.data)

    if fbref_df.empty:
        log.warning("  ⚠️ fbref_matchlogs is empty — skipping")
        return df

    # Check what columns fbref gives you
    log.info(f"  FBref columns: {list(fbref_df.columns)}")

    # Join on fixture_id if available, else date+team
    if "fixture_id" in fbref_df.columns:
        df = df.merge(fbref_df, on="fixture_id", how="left", suffixes=("", "_fbref"))
        log.info(f"  ✅ FBref joined on fixture_id: {len(fbref_df)} rows")
    else:
        log.warning("  ⚠️ No fixture_id in fbref_matchlogs — manual join needed")

    return df


# ✅ FIX — add league_key param
def h2h_win_rate(df: pd.DataFrame, home_id: int, away_id: int,
                 before_date: pd.Timestamp, league_key: str = None) -> float:
    mask = (
        (
            ((df["home_team_id"] == home_id) & (df["away_team_id"] == away_id)) |
            ((df["home_team_id"] == away_id) & (df["away_team_id"] == home_id))
        )
        & (df["match_date"] < before_date)
    )
    if league_key:
        mask = mask & (df["league_key"] == league_key)   # ✅ same-league H2H only
    h2h = df[mask]
    if h2h.empty:
        return 0.5
    wins = 0
    for _, row in h2h.iterrows():
        if row["home_team_id"] == home_id and row["result"] == 2:
            wins += 1
        elif row["away_team_id"] == home_id and row["result"] == 0:
            wins += 1
    return round(wins / len(h2h), 4)


def encode_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    venue_code : 1=home, 0=away
    opp_code   : integer ID of the opponent
    hour       : kick-off hour (UTC)
    day_code   : 0=Mon … 6=Sun
    """
    df["venue_code"] = 1   # always building from home team's perspective
    df["opp_code"]   = df["away_team_id"].astype(int)
    df["hour"]       = df["match_date"].dt.hour.astype("Int8")
    df["day_code"]   = df["match_date"].dt.dayofweek.astype("Int8")
    return df

def compute_elo(df: pd.DataFrame, k: int = 20) -> pd.DataFrame:
    """League-specific Elo — no cross-league contamination."""
    elo  = {}   # key: (team_id, league_key)
    rows = []
    completed = df[df["result"].notna()].sort_values("match_date")

    for _, row in completed.iterrows():
        h   = (row["home_team_id"], row["league_key"])
        a   = (row["away_team_id"], row["league_key"])
        elo.setdefault(h, 1500.0)
        elo.setdefault(a, 1500.0)

        rows.append({
            "fixture_id": row["fixture_id"],
            "home_elo":   round(elo[h], 4),
            "away_elo":   round(elo[a], 4),
            "elo_diff":   round(elo[h] - elo[a], 4),
        })

        exp_h    = 1 / (1 + 10 ** ((elo[a] - elo[h]) / 400))
        actual_h = 1.0 if row["result"] == 2 else (0.5 if row["result"] == 1 else 0.0)
        elo[h]  += k * (actual_h - exp_h)
        elo[a]  += k * ((1 - actual_h) - (1 - exp_h))

    return pd.DataFrame(rows)

    

# ══════════════════════════════════════════════════════════
# BUILD TEAM-LEVEL VIEW
# ══════════════════════════════════════════════════════════

def build_team_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand each match into TWO rows — one for each team
    so rolling stats can be computed per-team chronologically.
    """
    home_rows = df.rename(columns={
        "home_team_id": "team_id",
        "away_team_id": "opp_id",
        "home_goals":   "goals_scored",
        "away_goals":   "goals_conceded",
    }).copy()
    home_rows["venue"]       = "home"
    home_rows["win"]         = (home_rows["result"] == 2).astype(int)
    home_rows["venue_win"]   = (home_rows["result"] == 2).astype(int)  # ← ADD
    home_rows["clean_sheet"] = (home_rows["goals_conceded"] == 0).astype(int)

    away_rows = df.rename(columns={
        "away_team_id": "team_id",
        "home_team_id": "opp_id",
        "away_goals":   "goals_scored",
        "home_goals":   "goals_conceded",
    }).copy()
    away_rows["venue"]       = "away"
    away_rows["win"]         = (away_rows["result"] == 0).astype(int)
    away_rows["venue_win"]   = (away_rows["result"] == 0).astype(int)  # ← ADD
    away_rows["clean_sheet"] = (away_rows["goals_conceded"] == 0).astype(int)

    combined = pd.concat([home_rows, away_rows], ignore_index=True)
    return combined.sort_values("match_date")


# ══════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════

def build_features(df: pd.DataFrame, client) -> pd.DataFrame:
    log.info("⚙️  Building team-level rows...")
    team_rows = build_team_rows(df)

    all_team_leagues = team_rows[["league_key", "team_id"]].drop_duplicates()
    processed = []
    for _, grp in all_team_leagues.iterrows():
        t = team_rows[
            (team_rows["team_id"] == grp["team_id"]) &
            (team_rows["league_key"] == grp["league_key"])
        ].copy()
        t = rolling_averages(t)
        t = win_streak(t)
        t = momentum(t)
        processed.append(t)

    enriched = pd.concat(processed, ignore_index=True).sort_values("match_date")

    home_feats = enriched[enriched["venue"] == "home"][[
        "fixture_id", "team_id", "opp_id", "match_date",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum", "venue_wins_rolling",
    ]].copy()
    home_feats.columns = [
        "fixture_id", "home_team_id", "away_team_id", "match_date",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum", "home_form_at_home",
    ]

    away_feats = enriched[enriched["venue"] == "away"][[
        "fixture_id", "team_id",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum", "venue_wins_rolling",
    ]].copy()
    away_feats.columns = [
        "fixture_id", "away_team_id",
        "opp_goals_scored_rolling", "opp_goals_conceded_rolling",
        "opp_wins_rolling", "opp_clean_sheets_rolling",
        "opp_win_streak", "opp_form_momentum", "away_form_away",
    ]

    features = home_feats.merge(away_feats, on=["fixture_id", "away_team_id"])

    features = encode_categories(features)

    elo_df = compute_elo(df)
    features = features.merge(elo_df, on="fixture_id", how="left")
    features[["home_elo", "away_elo", "elo_diff"]] = (
        features[["home_elo", "away_elo", "elo_diff"]].fillna(0)
    )

    # ✅ STEP 1 — Merge league_key + season + team names FIRST, before any groupby
    meta_cols = df[["fixture_id", "season", "league_key", "home_team", "away_team"]].drop_duplicates("fixture_id")
    features = features.merge(meta_cols, on="fixture_id", how="left")

    # ✅ STEP 2 — Safety check: rename suffix columns if duplicate merge occurred
    for col in ["league_key_x", "league_key_y"]:
        if col in features.columns:
            features.rename(columns={"league_key_x": "league_key"}, inplace=True)
            features.drop(columns=["league_key_y"], errors="ignore", inplace=True)
            break

    # ✅ STEP 3 — Verify league_key exists before proceeding
    if "league_key" not in features.columns:
        raise ValueError("❌ league_key missing from features after merge — check df columns")
    log.info(f"  league_key values: {features['league_key'].unique()}")

    # Match stats (ESPN + fallback)
    features = build_stats_features(features, client)

    # Injuries
    features = build_injury_features(features, client)

    # FBref
    features = build_fbref_features(features, client)

    # ✅ STEP 4 — xG join AFTER league_key is confirmed present
    log.info("📊 Joining xG data...")
    xg_df = fetch_table(
        client, "match_xg",
        "fixture_id, home_xg, away_xg, forecast_home_win, forecast_draw, forecast_away_win"
    )
    log.info(f"  Loaded {len(xg_df)} xG rows")

    if not xg_df.empty:
        features = features.merge(xg_df, on="fixture_id", how="left")
        features = features.sort_values("match_date").copy()

        # ✅ Now league_key is guaranteed to exist
        features["home_xg_rolling"] = (
            features.groupby(["league_key", "home_team_id"])["home_xg"]
            .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
        )
        features["opp_xg_rolling"] = (
            features.groupby(["league_key", "away_team_id"])["away_xg"]
            .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
        )
        log.info(f"  ✅ xG joined: {len(xg_df)} rows")

    # H2H — needs league_key too
    features["h2h_win_rate"] = features.apply(
        lambda row: h2h_win_rate(
            df, row["home_team_id"], row["away_team_id"],
            row["match_date"], row["league_key"]
        ), axis=1
    )

    # Final column selection
    output_cols = [
        "fixture_id", "home_team_id", "away_team_id", "season", "league_key",
        "venue_code", "opp_code", "hour", "day_code",
        "goals_scored_rolling", "goals_conceded_rolling",
        "wins_rolling", "clean_sheets_rolling",
        "win_streak", "form_momentum",
        "opp_goals_scored_rolling", "opp_goals_conceded_rolling",
        "opp_wins_rolling", "opp_clean_sheets_rolling",
        "opp_win_streak", "opp_form_momentum",
        "h2h_win_rate",
        "home_elo", "away_elo", "elo_diff",
        "home_form_at_home", "away_form_away",
        "home_xg", "away_xg",
        "forecast_home_win", "forecast_draw", "forecast_away_win",
        "home_xg_rolling", "opp_xg_rolling",
        "home_shots", "away_shots",
        "home_shots_on_target", "away_shots_on_target",
        "home_corners", "away_corners",
        "home_possession", "away_possession",
        "home_injuries", "away_injuries", "injury_diff",
    ]

    # ✅ Graceful degradation — skip cols that didn't populate
    output_cols = [c for c in output_cols if c in features.columns]
    features = features[output_cols]

    numeric_cols = features.select_dtypes(include="number").columns
    features[numeric_cols] = features[numeric_cols].round(4)
    features = features.fillna(0)

    log.info(f"✅ Features built: {len(features)} rows × {len(output_cols)} columns")
    return features



def write_features(df: pd.DataFrame, client) -> int:
    """Upsert feature rows to Supabase in batches of 100."""
    rows    = df.to_dict(orient="records")
    written = 0
    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        client.table("matches_features").upsert(
            batch, on_conflict="fixture_id"
        ).execute()
        written += len(batch)
        log.info(f"  Batch {i//100 + 1}: {len(batch)} rows → matches_features")
    log.info(f"✅ Written {written} rows → Supabase.matches_features")
    return written


def main(season: int | None = None, dry_run: bool = False):
    print("\n🚀 Layer 3 — Feature Engineering Pipeline")
    print("=" * 55)
    print(f"   Season : {'ALL' if season is None else season}")
    print(f"   Window : Last {ROLLING_WINDOW} matches (rolling)")
    print(f"   Mode   : {'DRY RUN (no DB write)' if dry_run else 'LIVE'}")
    print("=" * 55)

    validate_config()
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Load all seasons (need full history for H2H + rolling across seasons)
    df = load_matches(client)

    # Build features on ALL data (rolling needs full history)
    features = build_features(df, client)  # ← Pass client

    # Filter output to requested season if specified
    if season:
        features_out = features[features["season"] == season]
        log.info(f"📊 Filtered to season {season}: {len(features_out)} rows")
    else:
        features_out = features

    # Save interim CSV
    csv_path = DATA_INTERIM_DIR / "matches_features.csv"
    features_out.to_csv(csv_path, index=False)
    log.info(f"💾 CSV saved → {csv_path}")

    print(f"\n📊 Sample output:")
    print(features_out.head(3).to_string(index=False))

    if dry_run:
        print("\n✅ DRY RUN complete — Supabase write skipped")
        return features_out

    print("\n🗄️  Writing to Supabase...")
    written = write_features(features_out, client)

    print(f"\n✅ Pipeline complete:")
    print(f"   Fixtures processed : {len(features_out)}")
    print(f"   Written            : {written} rows → Supabase.matches_features")
    print(f"   CSV                : {csv_path}")
    return features_out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EPL Feature Engineering")
    parser.add_argument("--season",  type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(season=args.season, dry_run=args.dry_run)
