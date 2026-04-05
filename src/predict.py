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
    model.load_model(str(MODELS_DIR / "catboost_multileague_v2.cbm"))
    with open(MODELS_DIR / "feature_cols.pkl", "rb") as f:
        feature_cols = pickle.load(f)
    log.info(f"✅ Model loaded — {len(feature_cols)} features")
    return model, feature_cols

MODEL, FEATURE_COLS = load_model()


# ══════════════════════════════════════════════════════════
# FEATURE BUILDER
# ══════════════════════════════════════════════════════════

def get_team_features(home_id: int, opp_id: int,
                      client, match_date: datetime | None = None,
                      league_key: str = "EPL") -> dict:

    resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", home_id)
        # ← REMOVE: .eq("league_key", league_key)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )
    row = resp.data[0] if resp.data else {}

    opp_resp = (
        client.table("matches_features")
        .select("*")
        .eq("home_team_id", opp_id)
        # ← REMOVE: .eq("league_key", league_key)
        .order("fixture_id", desc=True)
        .limit(1)
        .execute()
    )
    opp_row = opp_resp.data[0] if opp_resp.data else {}

    h2h = get_h2h_win_rate(home_id, opp_id, client)

    return {
        "league_key":                   league_key,   # ← still passes to model
        "home_goals_scored_rolling":    row.get("goals_scored_rolling", 1.50),
        "home_goals_conceded_rolling":  row.get("goals_conceded_rolling", 1.50),
        "home_wins_rolling":            row.get("wins_rolling", 0.38),
        "home_clean_sheets_rolling":    row.get("clean_sheets_rolling", 0.23),
        "home_win_streak":              row.get("win_streak", 0),
        "home_form_momentum":           row.get("form_momentum", 0.38),
        "opp_goals_scored_rolling":     opp_row.get("goals_scored_rolling", 1.50),
        "opp_goals_conceded_rolling":   opp_row.get("goals_conceded_rolling", 1.50),
        "opp_wins_rolling":             opp_row.get("wins_rolling", 0.38),
        "opp_clean_sheets_rolling":     opp_row.get("clean_sheets_rolling", 0.23),
        "opp_win_streak":               opp_row.get("win_streak", 0),
        "opp_form_momentum":            opp_row.get("form_momentum", 0.38),
        "h2h_win_rate":                 h2h,
    }

def _league_avg_features(league_key: str = "EPL") -> dict:
    """Neutral fallback when no DB data is available."""
    return {
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
                  client, match_date: datetime | None = None,
                  league_key: str = "EPL") -> dict:     # ← add league_key
    if match_date is None:
        match_date = datetime.now(timezone.utc)

    features = get_team_features(home_id, away_id, client,
                                  match_date, league_key)  # ← pass it through
    X = pd.DataFrame([features])[FEATURE_COLS]             # ← no .astype(float) — league_key is string

    probs      = MODEL.predict_proba(X)[0]
    pred_class = int(np.argmax(probs))

    return {
        "home_team_id":    home_id,
        "away_team_id":    away_id,
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
        )
        for m in matches[:50]
    ]
