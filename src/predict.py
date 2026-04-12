"""
Layer 5 — Core Prediction Logic
Loaded by api.py — never run directly.
"""
import pickle
import logging
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

log = logging.getLogger(__name__)

MODELS_DIR   = Path(__file__).resolve().parent.parent / "models"
LABEL_MAP    = {0: "Away Win", 1: "Draw", 2: "Home Win"}
RESULT_SHORT = {0: "away_win", 1: "draw", 2: "home_win"}


# ── Load model artifacts once at import time ───────────────
def load_model():
    model = CatBoostClassifier()
    model.load_model(str(MODELS_DIR / "catboost_multileague_v2.cbm"))
    with open(MODELS_DIR / "feature_cols.pkl", "rb") as f:
        feature_cols = pickle.load(f)
    log.info(f"✅ Model loaded — {len(feature_cols)} features: {feature_cols}")
    return model, feature_cols


MODEL, FEATURE_COLS = load_model()


# ══════════════════════════════════════════════════════════
# FEATURE BUILDER
# ══════════════════════════════════════════════════════════

def get_h2h_win_rate(home_id: int, away_id: int, client) -> float:
    """Compute H2H win rate from historical matches table."""
    resp = (
        client.table("matches")
        .select("home_team_id,away_team_id,result")
        .or_(
            f"and(home_team_id.eq.{home_id},away_team_id.eq.{away_id}),"
            f"and(home_team_id.eq.{away_id},away_team_id.eq.{home_id})"
        )
        .execute()
    )
    h2h = resp.data
    if not h2h:
        return 0.5
    wins = sum(
        1 for m in h2h
        if (m["home_team_id"] == home_id and m["result"] == 2)
        or (m["away_team_id"] == home_id and m["result"] == 0)
    )
    return round(wins / len(h2h), 4)

def get_team_name(team_id: int, client) -> str:
    try:
        resp = (
            client.table("teams")
            .select("team_name")           # ← was "name"
            .eq("team_id", team_id)
            .limit(1)
            .execute()
        )
        return resp.data[0]["team_name"] if resp.data else f"Team {team_id}"  # ← was "name"
    except:
        return f"Team {team_id}"


def _neutral_defaults(home_id: int, opp_id: int, league_key: str) -> dict:
    """Fallback when no DB data is available — uses league averages."""
    return {
        "home_team_id":                 home_id,        # ✅ top feature
        "opp_code":                     opp_id,         # ✅ top feature
        "league_key":                   league_key,
        "home_goals_scored_rolling":    1.50,
        "home_goals_conceded_rolling":  1.50,
        "home_wins_rolling":            0.38,
        "home_clean_sheets_rolling":    0.23,
        "home_win_streak":              0,
        "home_form_momentum":           0.38,
        "opp_goals_scored_rolling":     1.50,
        "opp_goals_conceded_rolling":   1.50,
        "opp_wins_rolling":             0.38,
        "opp_clean_sheets_rolling":     0.23,
        "opp_win_streak":               0,
        "opp_form_momentum":            0.38,
        "h2h_win_rate":                 0.50,
        "attack_vs_defence":            1.00,           # ✅ engineered feature
        "opp_attack_vs_defence":        1.00,           # ✅ engineered feature
    }


def get_team_features(home_id: int, opp_id: int,
                      client, match_date: datetime | None = None,
                      league_key: str = "EPL") -> dict:

    match_date = match_date or datetime.now(timezone.utc)

    # ── Fetch latest home team row ─────────────────────
    home_resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", home_id)
        .eq("league_key", league_key)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )
    row = home_resp.data[0] if home_resp.data else {}

    # ── Fetch latest away team row ─────────────────────
    opp_resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", opp_id)
        .eq("league_key", league_key)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )
    opp_row = opp_resp.data[0] if opp_resp.data else {}

    h2h = get_h2h_win_rate(home_id, opp_id, client)

    # ── Rolling stats ──────────────────────────────────
    # NOTE: matches_features uses renamed cols (home_goals_scored_rolling etc.)
    home_scored   = row.get("home_goals_scored_rolling") or row.get("goals_scored_rolling", 1.50)
    home_conceded = row.get("home_goals_conceded_rolling") or row.get("goals_conceded_rolling", 1.50)
    opp_scored    = opp_row.get("home_goals_scored_rolling") or opp_row.get("goals_scored_rolling", 1.50)
    opp_conceded  = opp_row.get("home_goals_conceded_rolling") or opp_row.get("goals_conceded_rolling", 1.50)

    # ── Derived / engineered features ─────────────────
    attack_vs_defence     = round(home_scored / (opp_conceded + 0.1), 4)
    opp_attack_vs_defence = round(opp_scored  / (home_conceded + 0.1), 4)

    home_elo = row.get("home_elo", 1500.0)
    away_elo = opp_row.get("home_elo", 1500.0)
    elo_diff = home_elo - away_elo

    home_form_at_home = row.get("home_form_at_home", 0.0)
    away_form_away    = opp_row.get("away_form_away", 0.0)
    venue_elo_boost   = elo_diff + (home_form_at_home * 50)

    home_form_momentum = row.get("home_form_momentum") or row.get("form_momentum", 0.38)
    opp_form_momentum  = opp_row.get("home_form_momentum") or opp_row.get("form_momentum", 0.38)

    home_xg_rolling = row.get("home_xg_rolling", 0.0)
    opp_xg_rolling  = opp_row.get("home_xg_rolling", 0.0)
    xg_diff         = home_xg_rolling - opp_xg_rolling
    xg_rolling_diff = xg_diff

    home_shots   = row.get("home_shots", 0.0)
    away_shots   = opp_row.get("away_shots", 0.0)
    home_sot     = row.get("home_shots_on_target", 0.0)
    away_sot     = opp_row.get("away_shots_on_target", 0.0)
    home_corners = row.get("home_corners", 0.0)
    away_corners = opp_row.get("away_corners", 0.0)
    home_poss    = row.get("home_possession", 50.0)
    away_poss    = opp_row.get("away_possession", 50.0)

    home_sot_ratio = home_sot / (home_shots + 0.1)
    away_sot_ratio = away_sot / (away_shots + 0.1)

    home_injuries = row.get("home_injuries", 0)
    away_injuries = opp_row.get("away_injuries", 0)

    home_form_adj = home_form_momentum * (1 - home_injuries * 0.05)
    away_form_adj = opp_form_momentum  * (1 - away_injuries * 0.05)

    return {
        # Identity
        "home_team_id":                 home_id,
        "opp_code":                     opp_id,
        "league_key":                   league_key,
        "venue_code":                   1,
        "hour":                         match_date.hour,
        "day_code":                     match_date.weekday(),

        # Home rolling
        "home_goals_scored_rolling":    home_scored,
        "home_goals_conceded_rolling":  home_conceded,
        "home_wins_rolling":            row.get("home_wins_rolling") or row.get("wins_rolling", 0.38),
        "home_clean_sheets_rolling":    row.get("home_clean_sheets_rolling") or row.get("clean_sheets_rolling", 0.23),
        "home_win_streak":              row.get("home_win_streak") or row.get("win_streak", 0),
        "home_form_momentum":           home_form_momentum,
        "home_form_at_home":            home_form_at_home,

        # Away rolling
        "opp_goals_scored_rolling":     opp_scored,
        "opp_goals_conceded_rolling":   opp_conceded,
        "opp_wins_rolling":             opp_row.get("opp_wins_rolling") or opp_row.get("wins_rolling", 0.38),
        "opp_clean_sheets_rolling":     opp_row.get("opp_clean_sheets_rolling") or opp_row.get("clean_sheets_rolling", 0.23),
        "opp_win_streak":               opp_row.get("opp_win_streak") or opp_row.get("win_streak", 0),
        "opp_form_momentum":            opp_form_momentum,
        "away_form_away":               away_form_away,

        # H2H + matchup
        "h2h_win_rate":                 h2h,
        "attack_vs_defence":            attack_vs_defence,
        "opp_attack_vs_defence":        opp_attack_vs_defence,
        "form_diff":                    round(home_form_momentum - opp_form_momentum, 4),
        "goal_diff_rolling":            round(home_scored - home_conceded, 4),
        "opp_goal_diff_rolling":        round(opp_scored - opp_conceded, 4),

        # Elo
        "home_elo":                     home_elo,
        "away_elo":                     away_elo,
        "elo_diff":                     elo_diff,
        "venue_elo_boost":              venue_elo_boost,

        # xG
        "home_xg_rolling":              home_xg_rolling,
        "opp_xg_rolling":               opp_xg_rolling,
        "forecast_home_win":            row.get("forecast_home_win", 0.0),
        "forecast_draw":                row.get("forecast_draw", 0.0),
        "forecast_away_win":            row.get("forecast_away_win", 0.0),
        "xg_diff":                      xg_diff,
        "xg_rolling_diff":              xg_rolling_diff,

        # Match stats
        "home_shots":                   home_shots,
        "away_shots":                   away_shots,
        "home_shots_on_target":         home_sot,
        "away_shots_on_target":         away_sot,
        "home_corners":                 home_corners,
        "away_corners":                 away_corners,
        "home_possession":              home_poss,
        "away_possession":              away_poss,
        "sot_diff":                     round(home_sot_ratio - away_sot_ratio, 4),
        "possession_diff":              round(home_poss - away_poss, 4),
        "corners_diff":                 round(home_corners - away_corners, 4),
        "shots_diff":                   round(home_shots - away_shots, 4),
        "shots_on_target_diff":         round(home_sot - away_sot, 4),

        # Injuries
        "home_injuries":                home_injuries,
        "away_injuries":                away_injuries,
        "injury_diff":                  home_injuries - away_injuries,
        "home_form_adj":                round(home_form_adj, 4),
        "away_form_adj":                round(away_form_adj, 4),
        "form_adj_diff":                round(home_form_adj - away_form_adj, 4),
    }


# ══════════════════════════════════════════════════════════
# PREDICTION ENGINE
# ══════════════════════════════════════════════════════════

def predict_match(home_id: int, away_id: int,
                  client, match_date: datetime | None = None,
                  league_key: str = "EPL") -> dict:
    if match_date is None:
        match_date = datetime.now(timezone.utc)

    try:
        features = get_team_features(
            home_id, away_id, client, match_date, league_key
        )
    except Exception as e:
        log.warning(f"DB lookup failed ({e}) — using neutral defaults")
        features = _neutral_defaults(home_id, away_id, league_key)

    X          = pd.DataFrame([features])[FEATURE_COLS]
    probs      = MODEL.predict_proba(X)[0]
    pred_class = int(np.argmax(probs))

    return {
        "home_team_id":    home_id,
        "away_team_id":    away_id,
        "home_team_name":  get_team_name(home_id, client),   # ✅ ADD
        "away_team_name":  get_team_name(away_id, client),   # ✅ ADD
        "league":          league_key,
        "predicted":       RESULT_SHORT[pred_class],
        "predicted_label": LABEL_MAP[pred_class],
        "probabilities": {
            "home_win": round(float(probs[2]), 4),
            "draw":     round(float(probs[1]), 4),
            "away_win": round(float(probs[0]), 4),
        },
        "confidence":    round(float(max(probs)), 4),
        "features_used": features,
        "predicted_at":  match_date.isoformat(),
    }


def predict_batch(matches: list[dict], client) -> list[dict]:
    """Predict a batch of matches (max 50)."""
    return [
        predict_match(
            m["home_team_id"],
            m["away_team_id"],
            client,
            datetime.fromisoformat(m["match_date"])
            if m.get("match_date") else None,
            league_key=m.get("league_key", "EPL"),  # ✅ fixed — was always EPL
        )
        for m in matches[:50]
    ]

# ── ADD at the very bottom of predict.py ──────────────────────────────────
if __name__ == "__main__":
    import argparse, os
    from supabase import create_client
    from dotenv import load_dotenv

    load_dotenv()
    client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    parser = argparse.ArgumentParser()
    parser.add_argument("--home", type=int, required=True, help="Home team_id")
    parser.add_argument("--away", type=int, required=True, help="Away team_id")
    parser.add_argument("--league", type=str, default="EPL")
    args = parser.parse_args()

    result = predict_match(args.home, args.away, client, league_key=args.league)
    import json
    print(json.dumps(result, indent=2, default=str))