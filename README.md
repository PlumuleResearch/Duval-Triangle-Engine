# Duval Triangle Dynamic DGA Engine

A Python-based real-time transformer fault diagnosis system implementing the
**Duval Triangle Method** (IEC 60599 / IEEE C57.104).

---

## Project Structure

```
duval_triangle/
├── duval_engine.py     # Core fault zone math & diagnosis logic
├── db_connector.py     # Database adapters (SQLite, PostgreSQL, MySQL, CSV, Mock)
├── dashboard.py        # Plotly Dash real-time web dashboard
├── cli.py              # Command-line interface
└── requirements.txt
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Launch the live dashboard (uses mock data by default)
```bash
cd duval_triangle
python dashboard.py
```
Open **http://127.0.0.1:8050** in your browser.

---

## Connecting to Your Database

Edit the `get_adapter()` function in `dashboard.py`:

### SQLite
```python
from db_connector import SQLiteAdapter
def get_adapter():
    return SQLiteAdapter("transformer_dga.db")
```

### PostgreSQL
```python
from db_connector import PostgreSQLAdapter
def get_adapter():
    return PostgreSQLAdapter(
        host="localhost", port=5432,
        dbname="transformers", user="admin", password="secret"
    )
```

### MySQL / MariaDB
```python
from db_connector import MySQLAdapter
def get_adapter():
    return MySQLAdapter(
        host="localhost", port=3306,
        database="transformers", user="root", password="secret"
    )
```

### CSV File
```python
from db_connector import CSVAdapter
def get_adapter():
    return CSVAdapter("dga_data.csv")
```

**Expected CSV columns:** `transformer_id, timestamp, ch4_ppm, c2h4_ppm, c2h2_ppm`

---

## Database Table Schema

```sql
CREATE TABLE dga_readings (
    id              SERIAL PRIMARY KEY,          -- or INTEGER AUTOINCREMENT for SQLite
    transformer_id  VARCHAR(64)  NOT NULL,
    timestamp       TIMESTAMPTZ  NOT NULL,        -- or TEXT for SQLite
    ch4_ppm         FLOAT        NOT NULL,
    c2h4_ppm        FLOAT        NOT NULL,
    c2h2_ppm        FLOAT        NOT NULL
);
```

---

## CLI Usage

### Diagnose a single reading
```bash
python cli.py diagnose --ch4 35 --c2h4 95 --c2h2 45
```

### Seed a SQLite database with test data
```bash
python cli.py seed-db --db transformer_dga.db --rows 300 --transformers 5
```

### Batch diagnose from a CSV file
```bash
python cli.py batch-csv --file dga_data.csv --verbose
```

---

## Fault Zones (Duval Triangle 1)

| Zone | Name                        | Severity |
|------|-----------------------------|----------|
| PD   | Partial Discharge           | MEDIUM   |
| D1   | Discharge — Low Energy      | MEDIUM   |
| D2   | Discharge — High Energy     | CRITICAL |
| DT   | Thermal + Electrical        | HIGH     |
| T1   | Thermal Fault < 300 °C      | LOW      |
| T2   | Thermal Fault 300–700 °C    | MEDIUM   |
| T3   | Thermal Fault > 700 °C      | HIGH     |

---

## Coordinate System

The Duval Triangle uses **triangular coordinates** (CH₄%, C₂H₄%, C₂H₂%) converted
to 2-D Cartesian via:

```
x = %C2H4 + %CH4 * cos(60°)
y = %CH4  * sin(60°)
```

Point classification uses `matplotlib.path.Path.contains_point()` for fast
polygon membership tests.

---

## References
- IEC 60599:2022 — Mineral oil-impregnated equipment in service
- IEEE C57.104-2008 — Guide for gases generated in oil-immersed transformers
- Duval, M. (1989). "Dissolved gas analysis: It can save your transformer."
- Singh & Bandyopadhyay (2010). "Duval Triangle — A Noble Technique for DGA."
