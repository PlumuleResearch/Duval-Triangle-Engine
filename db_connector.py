"""
Database Connector for Duval Triangle Engine
=============================================
Provides adapters for:
  • SQLite  (default / local dev)
  • PostgreSQL
  • MySQL / MariaDB
  • CSV file
  • In-memory mock data (for demo / testing)

Each adapter implements fetch_latest(n) and fetch_all() returning list[DGASample].
"""

import csv
import sqlite3
import datetime
import random
from abc import ABC, abstractmethod
from typing import Optional

from duval_engine import DGASample


# ---------------------------------------------------------------------------
# Base Adapter
# ---------------------------------------------------------------------------

class BaseDBAdapter(ABC):
    """Abstract base class — all adapters must implement these two methods."""

    @abstractmethod
    def fetch_latest(self, n: int = 20) -> list[DGASample]:
        """Fetch the n most recent DGA readings."""

    @abstractmethod
    def fetch_all(self) -> list[DGASample]:
        """Fetch all available DGA readings."""

    @abstractmethod
    def close(self):
        """Release any open connections."""


# ---------------------------------------------------------------------------
# SQLite Adapter
# ---------------------------------------------------------------------------

SQLITE_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS dga_readings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    transformer_id  TEXT    NOT NULL,
    timestamp       TEXT    NOT NULL,
    ch4_ppm         REAL    NOT NULL,
    c2h4_ppm        REAL    NOT NULL,
    c2h2_ppm        REAL    NOT NULL
);
"""

SQLITE_INSERT = """
INSERT INTO dga_readings (transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm)
VALUES (?, ?, ?, ?, ?);
"""


class SQLiteAdapter(BaseDBAdapter):
    """
    Reads DGA data from a local SQLite database file.

    Expected table schema (auto-created if missing):
        dga_readings(id, transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm)

    Usage:
        adapter = SQLiteAdapter("transformer_dga.db")
        samples = adapter.fetch_latest(50)
    """

    def __init__(self, db_path: str = "transformer_dga.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self):
        self.conn.execute(SQLITE_CREATE_TABLE)
        self.conn.commit()

    def insert_sample(self, sample: DGASample):
        """Helper to insert a single sample (useful for testing)."""
        self.conn.execute(SQLITE_INSERT, (
            sample.transformer_id,
            sample.timestamp,
            sample.ch4_ppm,
            sample.c2h4_ppm,
            sample.c2h2_ppm,
        ))
        self.conn.commit()

    def fetch_latest(self, n: int = 20) -> list[DGASample]:
        cur = self.conn.execute(
            "SELECT * FROM dga_readings ORDER BY timestamp DESC LIMIT ?", (n,)
        )
        return [_row_to_sample(row, "sqlite") for row in cur.fetchall()]

    def fetch_all(self) -> list[DGASample]:
        cur = self.conn.execute(
            "SELECT * FROM dga_readings ORDER BY timestamp ASC"
        )
        return [_row_to_sample(row, "sqlite") for row in cur.fetchall()]

    def fetch_by_transformer(self, transformer_id: str) -> list[DGASample]:
        cur = self.conn.execute(
            "SELECT * FROM dga_readings WHERE transformer_id=? ORDER BY timestamp ASC",
            (transformer_id,)
        )
        return [_row_to_sample(row, "sqlite") for row in cur.fetchall()]

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# PostgreSQL Adapter
# ---------------------------------------------------------------------------

class PostgreSQLAdapter(BaseDBAdapter):
    """
    Reads DGA data from a PostgreSQL database.

    Requires: pip install psycopg2-binary

    Expected table schema:
        CREATE TABLE dga_readings (
            id              SERIAL PRIMARY KEY,
            transformer_id  VARCHAR(64)  NOT NULL,
            timestamp       TIMESTAMPTZ  NOT NULL,
            ch4_ppm         FLOAT        NOT NULL,
            c2h4_ppm        FLOAT        NOT NULL,
            c2h2_ppm        FLOAT        NOT NULL
        );

    Usage:
        adapter = PostgreSQLAdapter(
            host="localhost", port=5432,
            dbname="transformers", user="admin", password="secret"
        )
        samples = adapter.fetch_latest(50)
    """

    def __init__(self, host: str, port: int, dbname: str,
                 user: str, password: str, table: str = "dga_readings"):
        try:
            import psycopg2
        except ImportError:
            raise ImportError("Install psycopg2: pip install psycopg2-binary")
        self.conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        self.table = table

    def fetch_latest(self, n: int = 20) -> list[DGASample]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm "
                f"FROM {self.table} ORDER BY timestamp DESC LIMIT %s", (n,)
            )
            return [_tuple_to_sample(row, "postgresql") for row in cur.fetchall()]

    def fetch_all(self) -> list[DGASample]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm "
                f"FROM {self.table} ORDER BY timestamp ASC"
            )
            return [_tuple_to_sample(row, "postgresql") for row in cur.fetchall()]

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# MySQL Adapter
# ---------------------------------------------------------------------------

class MySQLAdapter(BaseDBAdapter):
    """
    Reads DGA data from a MySQL / MariaDB database.

    Requires: pip install mysql-connector-python

    Same table schema as PostgreSQL adapter above.

    Usage:
        adapter = MySQLAdapter(
            host="localhost", port=3306,
            database="transformers", user="root", password="secret"
        )
    """

    def __init__(self, host: str, port: int, database: str,
                 user: str, password: str, table: str = "dga_readings"):
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("Install mysql-connector-python: pip install mysql-connector-python")
        self.conn = mysql.connector.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        self.table = table

    def fetch_latest(self, n: int = 20) -> list[DGASample]:
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm "
            f"FROM {self.table} ORDER BY timestamp DESC LIMIT %s", (n,)
        )
        return [_tuple_to_sample(row, "mysql") for row in cur.fetchall()]

    def fetch_all(self) -> list[DGASample]:
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm "
            f"FROM {self.table} ORDER BY timestamp ASC"
        )
        return [_tuple_to_sample(row, "mysql") for row in cur.fetchall()]

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# CSV Adapter
# ---------------------------------------------------------------------------

class CSVAdapter(BaseDBAdapter):
    """
    Reads DGA data from a CSV file.

    Expected CSV columns (header required):
        transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm

    Usage:
        adapter = CSVAdapter("dga_data.csv")
        samples = adapter.fetch_all()
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._cache: list[DGASample] = []
        self._load()

    def _load(self):
        self._cache = []
        with open(self.filepath, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self._cache.append(DGASample(
                    transformer_id=row["transformer_id"],
                    timestamp=row["timestamp"],
                    ch4_ppm=float(row["ch4_ppm"]),
                    c2h4_ppm=float(row["c2h4_ppm"]),
                    c2h2_ppm=float(row["c2h2_ppm"]),
                    source="csv",
                ))

    def fetch_latest(self, n: int = 20) -> list[DGASample]:
        return self._cache[-n:]

    def fetch_all(self) -> list[DGASample]:
        return list(self._cache)

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Mock / In-Memory Adapter (for demo and testing)
# ---------------------------------------------------------------------------

_FAULT_PROFILES = {
    "PD":  dict(ch4=(95, 5),  c2h4=(1, 1),   c2h2=(1, 1)),
    "D1":  dict(ch4=(40, 20), c2h4=(10, 8),   c2h2=(50, 20)),
    "D2":  dict(ch4=(10, 8),  c2h4=(40, 15),  c2h2=(50, 20)),
    "DT":  dict(ch4=(30, 15), c2h4=(60, 15),  c2h2=(10, 8)),
    "T1":  dict(ch4=(85, 5),  c2h4=(12, 4),   c2h2=(1, 1)),
    "T2":  dict(ch4=(60, 10), c2h4=(38, 6),   c2h2=(2, 1)),
    "T3":  dict(ch4=(20, 8),  c2h4=(75, 8),   c2h2=(5, 3)),
}


def _random_gas(mean: float, std: float) -> float:
    return max(0.1, random.gauss(mean, std))


class MockAdapter(BaseDBAdapter):
    """
    Generates synthetic DGA samples drawn from Gaussian distributions
    centered on each fault zone — useful for demo, CI, and unit tests.

    Usage:
        adapter = MockAdapter(n_transformers=3, n_readings=100)
        samples = adapter.fetch_latest(20)
    """

    def __init__(self, n_transformers: int = 4, n_readings: int = 200,
                 seed: int = 42):
        random.seed(seed)
        self._samples: list[DGASample] = []
        zones = list(_FAULT_PROFILES.keys())
        base_time = datetime.datetime(2025, 1, 1)

        for i in range(n_readings):
            t_id = f"TX-{(i % n_transformers) + 1:03d}"
            zone = random.choice(zones)
            p = _FAULT_PROFILES[zone]
            ch4  = _random_gas(*p["ch4"])
            c2h4 = _random_gas(*p["c2h4"])
            c2h2 = _random_gas(*p["c2h2"])
            ts = (base_time + datetime.timedelta(hours=i * 6)).isoformat()
            self._samples.append(DGASample(
                transformer_id=t_id,
                timestamp=ts,
                ch4_ppm=ch4,
                c2h4_ppm=c2h4,
                c2h2_ppm=c2h2,
                source="mock",
            ))

    def fetch_latest(self, n: int = 20) -> list[DGASample]:
        return self._samples[-n:]

    def fetch_all(self) -> list[DGASample]:
        return list(self._samples)

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_sample(row: sqlite3.Row, source: str) -> DGASample:
    return DGASample(
        transformer_id=row["transformer_id"],
        timestamp=str(row["timestamp"]),
        ch4_ppm=float(row["ch4_ppm"]),
        c2h4_ppm=float(row["c2h4_ppm"]),
        c2h2_ppm=float(row["c2h2_ppm"]),
        source=source,
    )


def _tuple_to_sample(row: tuple, source: str) -> DGASample:
    return DGASample(
        transformer_id=str(row[0]),
        timestamp=str(row[1]),
        ch4_ppm=float(row[2]),
        c2h4_ppm=float(row[3]),
        c2h2_ppm=float(row[4]),
        source=source,
    )
