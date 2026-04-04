# EPL Match Predictor — Production Architecture Plan

## Architecture Overview

The system is structured across 7 horizontal layers. Data flows top-to-bottom
during normal operation and loops back bottom-to-top during the weekly retraining cycle.

---

## Layer 1 — External Sources

| Source            | Type     | Limit              | Role                |
|-------------------|----------|--------------------|---------------------|
| API-Football      | REST API | 100 req/day (free) | Primary data source |
| GitHub `/data/`   | CSV files| Unlimited          | Fallback only       |

NOTE: Cache all API responses locally before writing to Supabase to avoid
wasting rate-limit quota on retries.

---

## Layer 2 — Data Ingestion

Flow: api_client.py → validate_data.py → Supabase matches table

### Data Quality Checks (DQ-001 → DQ-010)

| Check ID | Check                                          | Action on Fail    |
|----------|------------------------------------------------|-------------------|
| DQ-001   | No null fixture_id                             | Drop row          |
| DQ-002   | match_date is valid datetime                   | Drop row          |
| DQ-003   | home_goals and away_goals are integers >= 0    | Drop row          |
| DQ-004   | result in {0, 1, 2}                            | Drop row          |
| DQ-005   | status_short == "FT" (finished matches only)   | Drop row          |
| DQ-006   | No duplicate fixture_id                        | Deduplicate       |
| DQ-007   | home_team_id and away_team_id are known teams  | Flag + log        |
| DQ-008   | match_date not in future                       | Drop row          |
| DQ-009   | Season field matches expected season           | Flag + log        |
| DQ-010   | Row count per matchweek >= 8 (partial guard)   | Warn + continue   |

### Supabase matches Table Schema

CREATE TABLE matches (
  fixture_id       BIGINT PRIMARY KEY,
  match_date       TIMESTAMPTZ NOT NULL,
  season           INT NOT NULL,
  league_round     TEXT,
  home_team_id     INT NOT NULL,
  away_team_id     INT NOT NULL,
  home_goals       INT,
  away_goals       INT,
  result           SMALLINT CHECK (result IN (0, 1, 2)),
  status_short     TEXT DEFAULT 'FT',
  inserted_at      TIMESTAMPTZ DEFAULT NOW()
);

---

## Layer 3 — Feature Engineering

Flow: rolling_averages() → encode_categories() → h2h() + win_streak()
      + momentum() → Supabase matches_features table

### Feature Engineering Rules

| Function              | Key Rule                                | Output                                                              |
|-----------------------|-----------------------------------------|---------------------------------------------------------------------|
| rolling_averages()    | closed='left' — never includes current  | goals_scored_rolling, goals_conceded_rolling, wins_rolling          |
| encode_categories()   | Fit encoder on train only               | opp_code, venue_code                                                |
| h2h()                 | Historical win rate vs specific opponent| h2h_win_rate                                                        |
| win_streak()          | Consecutive wins before this match      | win_streak                                                          |
| momentum()            | Weighted recent form (recent = higher)  | form_momentum                                                       |

### Supabase matches_features Table Schema

CREATE TABLE matches_features (
  fixture_id                  BIGINT PRIMARY KEY REFERENCES matches(fixture_id),
  venue_code                  SMALLINT,
  opp_code                    INT,
  hour                        SMALLINT,
  day_code                    SMALLINT,
  goals_scored_rolling        FLOAT,
  goals_conceded_rolling      FLOAT,
  wins_rolling                FLOAT,
  clean_sheets_rolling        FLOAT,
  win_streak                  SMALLINT,
  opp_goals_scored_rolling    FLOAT,
  opp_goals_conceded_rolling  FLOAT,
  h2h_win_rate                FLOAT,
  away_team_id                INT,
  inserted_at                 TIMESTAMPTZ DEFAULT NOW()
);

---

## Layer 4 — Model Training

Flow: Google Colab T4 → CatBoostClassifier → Gate 4 Check → GitHub Releases

### Training Schedule

| Item      | Value                                          |
|-----------|------------------------------------------------|
| Platform  | Google Colab (T4 GPU)                          |
| Trigger   | GitHub Actions — Monday 06:00 UTC (09:00 EAT)  |
| Window    | Apr 6–9 initial run, then weekly               |
| Runtime   | ~3–5 min per run                               |

### CatBoost Config (production)

CatBoostClassifier(
    iterations=750,
    depth=4,                      # shallower = better generalisation (Phase 7 result)
    learning_rate=0.05,
    l2_leaf_reg=3,
    loss_function="MultiClass",
    eval_metric="TotalF1",
    auto_class_weights="Balanced", # CRITICAL — without this Draw F1=0.00
    early_stopping_rounds=50,
    random_seed=42,
)

### Gate 4 — Model Quality Check

| Metric              | Threshold              | Current production | Rationale                     |
|---------------------|------------------------|--------------------|-------------------------------|
| F1 (weighted)       | >= production F1       | 0.433              | Must beat current model       |
| Precision (weighted)| >= 0.40                | 0.43               | Avoid precision collapse      |
| Brier score         | <= 0.28                | ~0.26              | Probability calibration check |
| Draw recall         | >= 0.10                | 0.13               | Draw prediction must not die  |

CORRECTION: Original plan had F1 >= 58% — not achievable with 13 features.
Gate must be rolling: new model F1 >= current production F1.

### GitHub Releases Artifacts

model.cbm          ← CatBoost binary model
feature_cols.pkl   ← Feature list
metadata.json      ← F1, accuracy, date, iteration count

---

## Layer 5 — Model Serving

Flow: FastAPI → Docker → HuggingFace Spaces → Next.js Dashboard (Vercel)

### FastAPI Endpoints

| Endpoint         | Method | Description                        |
|------------------|--------|------------------------------------|
| /                | GET    | Welcome + version                  |
| /health          | GET    | Model loaded + feature count       |
| /model/info      | GET    | Feature list + label mapping       |
| /predict         | POST   | Single match prediction            |
| /predict/batch   | POST   | Batch predictions (max 50)         |
| /upcoming        | GET    | Next matchweek predictions         |
| /accuracy        | GET    | Rolling 10-match accuracy          |

### Dockerfile

FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY models/ ./models/
COPY src/api.py ./src/
COPY src/predict.py ./src/

EXPOSE 7860
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "7860"]

NOTE: HuggingFace Spaces requires port 7860, not 8000.

### HuggingFace Spaces Config (top of Space README.md)

---
title: EPL Match Predictor API
emoji: ⚽
colorFrom: green
colorTo: blue
sdk: docker
pinned: false
---

### Next.js Dashboard Pages (Vercel)

| Page       | Data Source                     | Cache      |
|------------|---------------------------------|------------|
| /          | HuggingFace API /upcoming       | ISR 3600s  |
| /accuracy  | Supabase accuracy_log table     | ISR 300s   |
| /history   | Supabase predictions table      | ISR 600s   |

---

## Layer 6 — Monitoring & Automation

Flow: GitHub Actions → monitor.py → Safety Gate → Keep-Alive Cron

### GitHub Actions Workflows

.github/workflows/
├── weekly_retrain.yml     ← Monday 06:00 UTC
├── keepalive.yml          ← Sunday 23:00 UTC (HF ping)
└── validate_data.yml      ← On push to data/

### weekly_retrain.yml

name: Weekly Retrain
on:
  schedule:
    - cron: "0 6 * * 1"    # Monday 06:00 UTC = 09:00 EAT
  workflow_dispatch:         # allow manual trigger

jobs:
  retrain:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Fetch new matchweek data
        env:
          API_FOOTBALL_KEY: ${{ secrets.API_FOOTBALL_KEY }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
        run: python src/api_client.py

      - name: Run feature engineering
        run: python src/features.py

      - name: Retrain model
        run: python src/train_catboost.py

      - name: Gate check (must pass to deploy)
        run: python src/monitor.py --gate

      - name: Deploy to HuggingFace
        if: success()
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
        run: python src/deploy.py

### keepalive.yml

name: Keep HuggingFace Space Alive
on:
  schedule:
    - cron: "0 23 * * 0"   # Sunday 23:00 UTC

jobs:
  ping:
    runs-on: ubuntu-latest
    steps:
      - name: Ping HuggingFace Space
        run: curl https://YOUR-USERNAME-epl-predictor.hf.space/health

### validate_data.yml

name: Validate Data on Push
on:
  push:
    paths:
      - 'data/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements.txt
      - run: python src/validate_data.py

### monitor.py — Drift Detection Logic

# --gate mode:
#   Load new model metrics from reports/evaluation_summary.json
#   Load production model metrics from models/metadata.json
#   Fail (exit 1) if new F1 < production F1
#   Fail (exit 1) if new Brier > 0.28
#   Fail (exit 1) if new Draw recall < 0.10
#   Pass (exit 0) — pipeline continues to deploy

# --drift mode (weekly check):
#   Load last 10 predictions from Supabase predictions table
#   Compare rolling precision to production baseline
#   Alert if precision drops > 5% from baseline

---

## Layer 7 — Persistent Storage

| Store        | Contents                                           | Free Tier        | Purpose                    |
|--------------|----------------------------------------------------|------------------|----------------------------|
| Supabase     | matches, matches_features, predictions, accuracy_log | 500MB DB       | Primary database           |
| GitHub Repo  | Source code, CI/CD secrets, model releases         | 1GB (LFS)        | Version control + registry |
| Vercel CDN   | Next.js frontend                                   | 100GB/month      | Frontend static + ISR      |
| CSV Backup   | /data/*.csv                                        | Unlimited (git)  | Data recovery fallback     |

### Supabase predictions Table Schema

CREATE TABLE predictions (
  id              BIGSERIAL PRIMARY KEY,
  fixture_id      BIGINT REFERENCES matches(fixture_id),
  predicted_at    TIMESTAMPTZ DEFAULT NOW(),
  prediction      CHAR(1) CHECK (prediction IN ('W','D','L')),
  prob_W          FLOAT,
  prob_D          FLOAT,
  prob_L          FLOAT,
  confidence      FLOAT,
  actual_result   CHAR(1),       -- filled after match completes
  was_correct     BOOLEAN,       -- filled after match completes
  model_version   TEXT
);

### Supabase accuracy_log Table Schema

CREATE TABLE accuracy_log (
  id              BIGSERIAL PRIMARY KEY,
  logged_at       TIMESTAMPTZ DEFAULT NOW(),
  model_version   TEXT,
  matchweek       INT,
  rolling_10_acc  FLOAT,
  rolling_10_f1   FLOAT,
  n_correct       INT,
  n_total         INT
);

---

## Required Secrets (GitHub → Settings → Secrets → Actions)

Secret Name          Value
-----------          -----
API_FOOTBALL_KEY     Your API-Football key (rapid API dashboard)
SUPABASE_URL         https://xxxx.supabase.co
SUPABASE_KEY         Supabase service role key (NOT the anon key)
HF_TOKEN             HuggingFace write token (hf_settings → access tokens)
VERCEL_TOKEN         Vercel deploy token (optional — only if automating deploys)

---

## Implementation Order (3 weeks)

| Week   | Task                                              | Deliverable                    |
|--------|---------------------------------------------------|--------------------------------|
| Week 6 | Supabase project setup + create all 4 tables      | DB schema live                 |
| Week 6 | Connect api_client.py → Supabase writes           | Data flowing into DB           |
| Week 6 | Build validate_data.py (DQ-001 → DQ-010)          | Clean ingestion pipeline       |
| Week 6 | Write Dockerfile + test locally                   | Container running on port 7860 |
| Week 7 | Create HuggingFace Space + push Docker image      | Public API endpoint live       |
| Week 7 | Set up GitHub Actions weekly_retrain.yml          | Automated weekly retraining    |
| Week 7 | Next.js dashboard scaffold on Vercel              | Frontend skeleton deployed     |
| Week 8 | Build monitor.py --gate and --drift modes         | Gate 4 automated               |
| Week 8 | Add keepalive.yml + validate_data.yml             | HuggingFace always on          |
| Week 8 | Wire accuracy_log → /accuracy endpoint → dashboard| Live accuracy visible          |
| Week 8 | Full end-to-end test (ingest → train → predict)   | System verified online         |

---

## Cost Estimate (Monthly)

| Service            | Plan              | Cost       |
|--------------------|-------------------|------------|
| API-Football       | Free (100 req/day)| $0         |
| Supabase           | Free (500MB)      | $0         |
| HuggingFace Spaces | Free CPU          | $0         |
| Vercel             | Free (Hobby)      | $0         |
| GitHub Actions     | Free (2000 min/mo)| $0         |
| Total              |                   | $0/month   |

---

## Key Corrections from Original Plan

1. depth=4 not depth=6
   Phase 7 tuning result — depth=4 beats depth=6 on this dataset size.

2. Gate F1 >= production F1, not fixed 58%
   Current model F1=0.433. A fixed 58% gate would permanently block deployment.
   Use rolling gate: new model must beat whatever is currently in production.

3. HuggingFace port is 7860, not 8000
   Spaces requires port 7860. Port 8000 will cause a silent deployment failure.

4. auto_class_weights='Balanced' is mandatory
   Without it Draw F1=0.00. This must be in every production training run.

5. Keep-alive cron is not optional
   HuggingFace free Spaces sleep after 48h of inactivity.
   The Sunday 23:00 UTC ping keeps the API warm for Monday predictions.
