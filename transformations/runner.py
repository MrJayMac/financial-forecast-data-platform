from __future__ import annotations

import glob
import os
from typing import Iterable

from sqlalchemy import text

from platform_common.db import get_engine


def read_sql_files(directory: str) -> list[str]:
    files = sorted(glob.glob(os.path.join(directory, "*.sql")))
    statements: list[str] = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            sql = fh.read().strip()
            if sql:
                statements.append(sql)
    return statements


def run_sql(statements: Iterable[str]) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        for sql in statements:
            conn.execute(text(sql))


def run_all(sql_dir: str | None = None) -> None:
    directory = sql_dir or os.path.join(os.path.dirname(__file__), "sql")
    statements = read_sql_files(directory)
    run_sql(statements)


if __name__ == "__main__":
    run_all()
