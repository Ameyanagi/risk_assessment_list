from __future__ import annotations

import json
import sqlite3
from functools import lru_cache
from importlib import resources


def default_db_path() -> str:
    resource = resources.files("risk_assessment_list").joinpath("data/ra.sqlite3")
    return str(resource)


@lru_cache(maxsize=4)
def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or default_db_path()
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def load_json(value: str | None) -> dict:
    if not value:
        return {}
    return json.loads(value)
