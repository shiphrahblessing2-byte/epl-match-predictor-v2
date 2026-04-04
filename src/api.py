"""
Layer 5 — FastAPI Serving Layer
Runs on HuggingFace Spaces (port 7860) or locally (port 8000).

Run locally:
    uvicorn src.api:app --reload --port 8000
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SUPABASE_URL, SUPABASE_KEY, MODELS_DIR
from predict import (
    MODEL, FEATURE_COLS, LABEL_MAP,
    predict_match, predict_batch,
)
from supabase import create_client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Load metadata ──────────────────────────────────────────
with open(MODELS_DIR / "metadata.json") as f:
    METADATA = json.load(f)

VERSION = "1.0.0"

# ── App ────────────────────────────────────────────────────
app = FastAPI(
    title="EPL Match Predictor API",
    description="Predicts EPL match outcomes using CatBoost trained on 2022-2024 data.",
    version=VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Supabase client (shared) ───────────────────────────────
db = create_client(SUPABASE_URL, SUPABASE_KEY)


# ══════════════════════════════════════════════════════════
# REQUEST / RESPONSE SCHEMAS
# ══════════════════════════════════════════════════════════

class MatchRequest(BaseModel):
    home_team_id: int = Field(..., example=33, description="API-Football team ID")
    away_team_id: int = Field(..., example=49, description="API-Football team ID")
    match_date:   Optional[str] = Field(None, example="2026-04-05T15:00:00+00:00")


class BatchRequest(BaseModel):
    matches: list[MatchRequest] = Field(..., max_length=50)


# ══════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════

@app.get("/", tags=["Info"])
def root():
    return {
        "name":    "EPL Match Predictor API",
        "version": VERSION,
        "status":  "online",
        "docs":    "/docs",
        "endpoints": [
            "/health", "/model/info",
            "/predict", "/predict/batch",
            "/upcoming", "/accuracy",
        ],
    }


@app.get("/health", tags=["Info"])
def health():
    return {
        "status":         "healthy",
        "model_loaded":   MODEL is not None,
        "feature_count":  len(FEATURE_COLS),
        "trained_at":     METADATA.get("trained_at"),
        "test_accuracy":  METADATA.get("metrics", {}).get("accuracy"),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


@app.get("/model/info", tags=["Info"])
def model_info():
    return {
        "version":       VERSION,
        "feature_cols":  FEATURE_COLS,
        "label_map":     LABEL_MAP,
        "train_seasons": METADATA.get("train_seasons"),
        "test_season":   METADATA.get("test_season"),
        "metrics":       METADATA.get("metrics"),
        "catboost_params": METADATA.get("catboost_params"),
    }


@app.post("/predict", tags=["Prediction"])
def predict(req: MatchRequest):
    """
    Predict outcome for a single match.
    Returns probabilities for Home Win, Draw, Away Win.
    """
    if req.home_team_id == req.away_team_id:
        raise HTTPException(400, "home_team_id and away_team_id must be different")

    try:
        match_date = (
            datetime.fromisoformat(req.match_date)
            if req.match_date else None
        )
        result = predict_match(
            req.home_team_id,
            req.away_team_id,
            db,
            match_date,
        )
        return result
    except Exception as e:
        log.error(f"Prediction error: {e}")
        raise HTTPException(500, f"Prediction failed: {str(e)}")


@app.post("/predict/batch", tags=["Prediction"])
def predict_batch_endpoint(req: BatchRequest):
    """
    Predict outcomes for up to 50 matches.
    """
    try:
        matches = [m.model_dump() for m in req.matches]
        results = predict_batch(matches, db)
        return {
            "count":       len(results),
            "predictions": results,
        }
    except Exception as e:
        log.error(f"Batch prediction error: {e}")
        raise HTTPException(500, f"Batch prediction failed: {str(e)}")


@app.get("/upcoming", tags=["Prediction"])
def upcoming_predictions():
    """
    Return predictions for the next matchweek.
    Reads upcoming fixtures from API-Football and runs predictions.
    """
    try:
        # Fetch upcoming fixtures from matches table (NS = not started)
        resp = (
            db.table("matches")
            .select("fixture_id,home_team_id,away_team_id,match_date")
            .neq("status_short", "FT")
            .order("match_date", desc=False)
            .limit(10)
            .execute()
        )
        fixtures = resp.data
        if not fixtures:
            return {"count": 0, "predictions": [],
                    "note": "No upcoming fixtures found"}

        predictions = []
        for fix in fixtures:
            pred = predict_match(
                fix["home_team_id"],
                fix["away_team_id"],
                db,
                datetime.fromisoformat(fix["match_date"]),
            )
            pred["fixture_id"] = fix["fixture_id"]
            predictions.append(pred)

        return {"count": len(predictions), "predictions": predictions}
    except Exception as e:
        log.error(f"Upcoming predictions error: {e}")
        raise HTTPException(500, str(e))


@app.get("/accuracy", tags=["Evaluation"])
def rolling_accuracy():
    """
    Return rolling accuracy over the last 10 predictions
    where actual result is known.
    """
    try:
        resp = (
            db.table("predictions")
            .select("predicted,actual,correct")
            .order("predicted_at", desc=True)
            .limit(10)
            .execute()
        )
        rows = resp.data
        if not rows:
            return {"rolling_accuracy": None, "sample_size": 0,
                    "note": "No predictions with known outcomes yet"}

        correct = sum(1 for r in rows if r.get("correct"))
        return {
            "rolling_accuracy": round(correct / len(rows), 4),
            "correct":          correct,
            "total":            len(rows),
            "sample_size":      len(rows),
        }
    except Exception as e:
        raise HTTPException(500, str(e))
