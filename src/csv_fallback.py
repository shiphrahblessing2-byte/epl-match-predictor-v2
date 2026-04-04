import csv
import os
from typing import List, Dict

DATA_DIR = os.path.join(os.path.dirname(__file__), "../data")

def read_csv(filename: str) -> List[Dict]:
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        return []
    with open(filepath, newline="") as f:
        return list(csv.DictReader(f))

def get_fixtures_fallback() -> List[Dict]:
    return read_csv("fixtures.csv")

def get_predictions_fallback() -> List[Dict]:
    return read_csv("predictions.csv")
