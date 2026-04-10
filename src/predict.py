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
    """Fetch latest rolling stats for home + away team from Supabase."""

    # Home team latest features
    resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", home_id)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )
    row = resp.data[0] if resp.data else {}

    # Away team latest features
    opp_resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", opp_id)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )
    opp_row = opp_resp.data[0] if opp_resp.data else {}

    h2h = get_h2h_win_rate(home_id, opp_id, client)

    # Rolling stats with defaults
    home_scored    = row.get("goals_scored_rolling", 1.50)
    home_conceded  = row.get("goals_conceded_rolling", 1.50)
    opp_scored     = opp_row.get("goals_scored_rolling", 1.50)
    opp_conceded   = opp_row.get("goals_conceded_rolling", 1.50)

    # ✅ Engineered features — must match build_training_features.py logic
    attack_vs_defence     = round(home_scored / (opp_conceded + 0.1), 4)
    opp_attack_vs_defence = round(opp_scored  / (home_conceded + 0.1), 4)

    return {
        "home_team_id":                 home_id,           # ✅ 22.5% importance
        "opp_code":                     opp_id,            # ✅ 19.1% importance
        "league_key":                   league_key,        # ✅ 8.4% importance
        "home_goals_scored_rolling":    home_scored,
        "home_goals_conceded_rolling":  home_conceded,
        "home_wins_rolling":            row.get("wins_rolling", 0.38),
        "home_clean_sheets_rolling":    row.get("clean_sheets_rolling", 0.23),
        "home_win_streak":              row.get("win_streak", 0),
        "home_form_momentum":           row.get("form_momentum", 0.38),
        "opp_goals_scored_rolling":     opp_scored,
        "opp_goals_conceded_rolling":   opp_conceded,
        "opp_wins_rolling":             opp_row.get("wins_rolling", 0.38),
        "opp_clean_sheets_rolling":     opp_row.get("clean_sheets_rolling", 0.23),
        "opp_win_streak":               opp_row.get("win_streak", 0),
        "opp_form_momentum":            opp_row.get("form_momentum", 0.38),
        "h2h_win_rate":                 h2h,
        "attack_vs_defence":            attack_vs_defence,     # ✅ 5.8% importance
        "opp_attack_vs_defence":        opp_attack_vs_defence, # ✅ 3.9% importance
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