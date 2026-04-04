---
title: EPL Match Predictor API
emoji: ⚽
colorFrom: green
colorTo: blue
sdk: docker
pinned: false
---

# EPL Match Predictor API

Predicts English Premier League match outcomes using CatBoost trained on 2022–2024 seasons.

**Live API:** https://shiphrahb-epl-match-predictor.hf.space
**Swagger Docs:** https://shiphrahb-epl-match-predictor.hf.space/docs

---

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API info + version |
| GET | `/health` | Model status + accuracy |
| GET | `/model/info` | Features + label map |
| POST | `/predict` | Single match prediction |
| POST | `/predict/batch` | Batch (max 50 matches) |
| GET | `/upcoming` | Next matchweek predictions |
| GET | `/accuracy` | Rolling 10-match accuracy |

---

## Usage Examples

### Single Prediction
```bash
curl -X POST https://shiphrahb-epl-match-predictor.hf.space/predict \
  -H "Content-Type: application/json" \
  -d '{"home_team_id": 33, "away_team_id": 49}'
```

**Response:**
```json
{
  "home_team_id": 33,
  "away_team_id": 49,
  "predicted": "away_win",
  "predicted_label": "Away Win",
  "probabilities": {
    "home_win": 0.2018,
    "draw": 0.3819,
    "away_win": 0.4162
  },
  "confidence": 0.4162
}
```

### Batch Prediction
```bash
curl -X POST https://shiphrahb-epl-match-predictor.hf.space/predict/batch \
  -H "Content-Type: application/json" \
  -d '{
    "matches": [
      {"home_team_id": 33, "away_team_id": 49},
      {"home_team_id": 40, "away_team_id": 42},
      {"home_team_id": 50, "away_team_id": 47}
    ]
  }'
```

---

## Team ID Reference

| ID | Team |
|----|------|
| 33 | Manchester United |
| 34 | Newcastle United |
| 40 | Liverpool |
| 42 | Arsenal |
| 47 | Tottenham Hotspur |
| 49 | Chelsea |
| 50 | Manchester City |
| 66 | Aston Villa |

---

## Model

- **Algorithm:** CatBoostClassifier
- **Train seasons:** 2022, 2023
- **Test season:** 2024
- **Accuracy:** 43.4% (vs 33.3% random baseline)
- **F1 weighted:** 0.422
- **Features:** 13 engineered features (rolling form, H2H, momentum)
