# src/primitive_db/utils.py


import json
import os
from typing import Any, Dict, List

from .constants import DATA_DIR


def load_metadata(filepath: str) -> Dict[str, Any]:
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def save_metadata(filepath: str, data: Dict[str, Any]) -> None:
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def _get_table_path(table_name: str) -> str:
    return os.path.join(DATA_DIR, f"{table_name}.json")


def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    path = _get_table_path(table_name)
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    path = _get_table_path(table_name)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

