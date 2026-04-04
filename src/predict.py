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
from supabase import create_client

log = logging.getLogger(__name__)

MODELS_DIR   = Path(__file__).resolve().parent.parent / "models"
LABEL_MAP    = {0: "Away Win", 1: "Draw", 2: "Home Win"}
RESULT_SHORT = {0: "away_win", 1: "draw", 2: "home_win"}

# ── Load model artifacts once at import time ───────────────
def load_model():
    model = CatBoostClassifier()
    model.load_model(str(MODELS_DIR / "model.cbm"))
    with open(MODELS_DIR / "feature_cols.pkl", "rb") as f:
        feature_cols = pickle.load(f)
    log.info(f"✅ Model loaded — {len(feature_cols)} features")
    return model, feature_cols

MODEL, FEATURE_COLS = load_model()


# ══════════════════════════════════════════════════════════
# FEATURE BUILDER
# ══════════════════════════════════════════════════════════

def get_team_features(team_id: int, opp_id: int,
                      client, match_date: datetime | None = None) -> dict:
    """
    Fetch the most recent feature row for team_id from matches_features.
    Uses latest season 2024 record as the current form baseline.
    """
    if match_date is None:
        match_date = datetime.now(timezone.utc)

    # Get latest feature row for this team as home team
    resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", team_id)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )

    if not resp.data:
        # Fallback — use league average defaults
        log.warning(f"No feature data for team {team_id} — using league avg")
        return _league_avg_features(team_id, opp_id, match_date)

    row = resp.data[0]

    # Get opponent's latest stats
    opp_resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", opp_id)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )
    opp_row = opp_resp.data[0] if opp_resp.data else {}

    # Get H2H win rate from historical matches
    h2h = get_h2h_win_rate(team_id, opp_id, client)

    return {
        "venue_code":                 1,
        "opp_code":                   opp_id,
        "hour":                       match_date.hour,
        "day_code":                   match_date.weekday(),
        "goals_scored_rolling":       row.get("goals_scored_rolling", 1.5),
        "goals_conceded_rolling":     row.get("goals_conceded_rolling", 1.5),
        "wins_rolling":               row.get("wins_rolling", 0.38),
        "clean_sheets_rolling":       row.get("clean_sheets_rolling", 0.23),
        "win_streak":                 row.get("win_streak", 0),
        "form_momentum":              row.get("form_momentum", 0.38),
        "opp_goals_scored_rolling":   opp_row.get("goals_scored_rolling", 1.5),
        "opp_goals_conceded_rolling": opp_row.get("goals_conceded_rolling", 1.5),
        "h2h_win_rate":               h2h,
    }


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


def _league_avg_features(team_id: int, opp_id: int,
                         match_date: datetime) -> dict:
    return {
        "venue_code": 1, "opp_code": opp_id,
        "hour": match_date.hour, "day_code": match_date.weekday(),
        "goals_scored_rolling": 1.50, "goals_conceded_rolling": 1.50,
        "wins_rolling": 0.38, "clean_sheets_rolling": 0.23,
        "win_streak": 0, "form_momentum": 0.38,
        "opp_goals_scored_rolling": 1.50, "opp_goals_conceded_rolling": 1.50,
        "h2h_win_rate": 0.50,
    }


# ══════════════════════════════════════════════════════════
# PREDICTION ENGINE
# ══════════════════════════════════════════════════════════

def predict_match(home_id: int, away_id: int,
                  client, match_date: datetime | None = None) -> dict:
    """
    Core prediction function — returns probabilities + predicted outcome.
    """
    if match_date is None:
        match_date = datetime.now(timezone.utc)

    features = get_team_features(home_id, away_id, client, match_date)
    X = pd.DataFrame([features])[FEATURE_COLS].astype(float)

    probs      = MODEL.predict_proba(X)[0]
    pred_class = int(np.argmax(probs))

    return {
        "home_team_id":  home_id,
        "away_team_id":  away_id,
        "predicted":     RESULT_SHORT[pred_class],
        "predicted_label": LABEL_MAP[pred_class],
        "probabilities": {
            "home_win":  round(float(probs[2]), 4),
            "draw":      round(float(probs[1]), 4),
            "away_win":  round(float(probs[0]), 4),
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
        )
        for m in matches[:50]
    ]
