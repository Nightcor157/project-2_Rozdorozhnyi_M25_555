# src/primitive_db/utils.py

import json
from typing import Any, Dict


def load_metadata(filepath: str) -> Dict[str, Any]:

    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: Dict[str, Any]) -> None:
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

