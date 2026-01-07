<!-- Animated Header Wave -->
<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=0:1e3a5f,50:2d5a87,100:3d7ab5&height=200&section=header&text=EMS%20Data%20Warehouse&fontSize=45&fontColor=ffffff&animation=fadeIn&fontAlignY=35&desc=Kimball%20Dimensional%20Model%20%7C%20Technical%20Assessment&descSize=18&descAlignY=55&descAlign=50" alt="Header"/>
</p>

<!-- Typing Animation -->
<p align="center">
  <a href="https://github.com/ryanxbutcher/ryanxbutcher.github.io">
    <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=600&size=22&duration=3000&pause=1000&color=3D7AB5&center=true&vCenter=true&multiline=true&repeat=true&width=600&height=80&lines=Indiana+EMS+Incidents+2024;1.5M%2B+Records+%7C+11+Dimensions+%7C+Star+Schema" alt="Typing SVG" />
  </a>
</p>

<!-- Tech Stack Badges -->
<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/SQL_Server-2019+-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white" alt="SQL Server"/>
  <img src="https://img.shields.io/badge/SQLite-Portable-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQLite"/>
  <img src="https://img.shields.io/badge/SSIS-ETL-5C2D91?style=for-the-badge&logo=.net&logoColor=white" alt="SSIS"/>
</p>

<!-- Status Badges -->
<p align="center">
  <img src="https://img.shields.io/badge/✓_Status-Complete-22c55e?style=flat-square" alt="Status"/>
  <img src="https://img.shields.io/badge/📊_Records-1.5M+-0066cc?style=flat-square" alt="Records"/>
  <img src="https://img.shields.io/badge/⭐_Dimensions-11-9333ea?style=flat-square" alt="Dimensions"/>
  <img src="https://img.shields.io/badge/📐_Kimball-Star_Schema-eab308?style=flat-square" alt="Kimball"/>
</p>

---

<!-- Clean ASCII Banner -->
```
+===========================================================================+
|                                                                           |
|    ███████ ███    ███ ███████     ██████   █████  ████████  █████         |
|    ██      ████  ████ ██          ██   ██ ██   ██    ██    ██   ██        |
|    █████   ██ ████ ██ ███████     ██   ██ ███████    ██    ███████        |
|    ██      ██  ██  ██      ██     ██   ██ ██   ██    ██    ██   ██        |
|    ███████ ██      ██ ███████     ██████  ██   ██    ██    ██   ██        |
|                                                                           |
|                        WAREHOUSE PROJECT                                  |
|                                                                           |
+===========================================================================+
```

<p align="center">
  <strong>J. Ryan Butcher</strong> · Master Architect & Medical Informatics Awesomalist
</p>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [Data Model](#-data-model)
- [ETL Pipeline](#-etl-pipeline)
- [Data Quality Rules](#-data-quality-rules)
- [Configuration](#-configuration)
- [SSIS Implementation](#-ssis-implementation)
- [Logging & Monitoring](#-logging--monitoring)
- [Design Decisions](#-design-decisions)

---

## 🎯 Overview

End-to-end ETL solution that ingests EMS incident data from CSV, stages it, transforms it, and loads it into a **Kimball-style dimensional data warehouse**.

### ✨ Key Features

<table>
<tr>
<td width="50%">

**🔧 Technical Implementation**
- Dual ETL: Python + SSIS packages
- Dual Database: SQLite + SQL Server DDL
- Kimball Star Schema (11 dimensions)
- SCD Type 1 & 2 handling
- Batch processing (50K rows/batch)

</td>
<td width="50%">

**📊 Production Features**
- Comprehensive run/step/error logging
- Config-driven parameterization (YAML)
- Data quality validation engine
- Re-runnable (idempotent) loads
- Unknown member handling (-1 keys)

</td>
</tr>
</table>

---

## 🚀 Quick Start

### Prerequisites

```
Python 3.9+  •  pip  •  Git
```

### Installation & Run

```bash
# Clone the repository
git clone https://github.com/ryanxbutcher/ryanxbutcher.github.io.git
cd ryanxbutcher.github.io

# Install dependencies
pip install -r etl/python/requirements.txt

# Download the dataset (414MB)
curl -L "https://hub.mph.in.gov/dataset/8404e4ae-c244-48c1-95ee-c2eff5e177e6/resource/249f8df8-7728-4cab-a167-7c93c5e43eee/download/ems_runs_2024.csv" \
     -o data/input/ems_runs_2024.csv

# Run the ETL
cd etl/python
python main.py
```

### Verify Results

```bash
# Query the populated warehouse
sqlite3 db/ems-warehouse.db "SELECT COUNT(*) as incidents FROM FACT_EMS_INCIDENT;"
sqlite3 db/ems-warehouse.db "SELECT county_name, COUNT(*) as cnt FROM FACT_EMS_INCIDENT f JOIN DIM_COUNTY c ON f.county_key=c.county_key GROUP BY county_name ORDER BY cnt DESC LIMIT 5;"
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ETL PIPELINE FLOW                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│    📁 CSV Source          🔍 Validation         🗃️ Staging                   │
│         │                      │                     │                      │
│         ▼                      ▼                     ▼                      │
│    ┌─────────┐           ┌─────────┐           ┌─────────┐                  │
│    │ Extract │──────────▶│  Stage  │──────────▶│Transform│                  │
│    │ (Chunk) │           │  (Raw)  │           │ (Clean) │                  │
│    └─────────┘           └─────────┘           └────┬────┘                  │
│                                                     │                       │
│                               ┌─────────────────────┼─────────────────────┐ │
│                               │                     │                     │ │
│                               ▼                     ▼                     ▼ │
│                          ┌─────────┐           ┌─────────┐          ┌──────┐│
│                          │  Dims   │           │  Facts  │          │Reject││
│                          │ (SCD)   │           │ (Load)  │          │ Log  ││
│                          └─────────┘           └─────────┘          └──────┘│
│                                                                             │
│    ⚙️ Config-Driven    📝 Full Logging    🔄 Re-runnable    ✅ Validated      
└─────────────────────────────────────────────────────────────────────────────┘
```

### 📁 Repository Structure

```
ryanxbutcher.github.io/
│
├── 📂 sql/                          # SQL Server DDL Scripts
│   ├── 01-create-staging.sql        # Staging table
│   ├── 02-create-dimensions.sql     # 11 dimension tables
│   ├── 03-create-facts.sql          # Fact table + indexes
│   ├── 04-create-logging.sql        # ETL logging tables
│   ├── 05-create-indexes.sql        # Columnstore + analytics
│   └── 06-seed-dimensions.sql       # Unknown members + date dim
│
├── 📂 etl/
│   ├── 📂 python/                   # Python ETL (Primary)
│   │   ├── main.py                  # 🎯 Orchestrator
│   │   ├── config.py                # Configuration loader
│   │   ├── extract.py               # CSV chunked reader
│   │   ├── stage.py                 # Staging operations
│   │   ├── transform.py             # Data transformations
│   │   ├── load-dimensions.py       # Dimension SCD logic
│   │   ├── load-facts.py            # Fact batch loading
│   │   ├── logging-utils.py         # Logging framework
│   │   ├── data-quality.py          # Validation rules
│   │   └── requirements.txt         # Dependencies
│   │
│   └── 📂 ssis/                     # SSIS Package (Demo)
│       └── ems-etl-master.dtsx      # Master control flow
│
├── 📂 config/                       # Configuration
│   ├── config.yaml                  # Base configuration
│   ├── config.dev.yaml              # Dev overrides
│   └── config.prod.yaml             # Prod overrides
│
├── 📂 data/
│   ├── 📂 input/                    # Source files
│   ├── 📂 rejected/                 # Failed records
│   └── 📂 archive/                  # Processed files
│
├── 📂 db/                           # SQLite database
├── 📂 logs/                         # ETL logs
└── 📄 README.md                     # This file
```

---

## 📊 Data Model

### 🎯 Grain Definition

> **One row per EMS incident/run** — the atomic unit representing a single emergency medical service response event.

### ⭐ Star Schema

<table>
<tr>
<td>

```
          ┌─────────────┐
          │  DIM_DATE   │
          └──────┬──────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
┌───┴───┐  ┌─────┴─────┐  ┌───┴───┐
│ TIME  │  │   FACT    │  │COUNTY │
│ OF DAY│  │    EMS    │  │(SCD2) │
└───────┘  │ INCIDENT  │  └───────┘
           │           │
    ┌──────┤  1.5M+    ├──────┐
    │      │  Records  │      │
┌───┴───┐  └─────┬─────┘  ┌───┴────┐
│COMPLT │        │        │PROVIDER│
└───────┘    ┌───┴───┐    │  ORG   │
         ┌───┤       ├───┐│(SCD2)  │
         │   │ +8    │   │└────────┘
         │   │ more  │   │
         │   │ dims  │   │
         ▼   └───────┘   ▼
```

</td>
<td>

| Dimension | SCD | Key Attributes |
|-----------|-----|----------------|
| `DIM_DATE` | 0 | year, month, quarter |
| `DIM_TIME_OF_DAY` | 0 | hour, shift, period |
| `DIM_COUNTY` | **2** | county, state, region |
| `DIM_CHIEF_COMPLAINT` | 1 | complaint, category |
| `DIM_ANATOMIC_LOCATION` | 1 | location, body_region |
| `DIM_SYMPTOM` | 1 | symptom, category |
| `DIM_PROVIDER_IMPRESSION` | 1 | impression, acuity |
| `DIM_DISPOSITION` | 1 | disposition (ED/Hosp) |
| `DIM_DESTINATION_TYPE` | 1 | destination, category |
| `DIM_PROVIDER_ORGANIZATION` | **2** | structure, service |
| `DIM_SERVICE_LEVEL` | 1 | level, scope_tier |

</td>
</tr>
</table>

### 📏 Fact Table Measures

| Measure | Type | Description |
|---------|------|-------------|
| `provider_to_scene_mins` | Additive | Response time |
| `provider_to_dest_mins` | Additive | Transport time |
| `dispatch_to_arrival_mins` | Derived | Calculated |
| `scene_time_mins` | Derived | Time on scene |
| `injury_flg` | Semi-additive | 1=Injury |
| `naloxone_given_flg` | Semi-additive | 1=Naloxone |
| `incident_count` | Additive | Always 1 |

### 🔑 Unknown Member Strategy

All dimensions use **surrogate key = -1** for unknown/NULL values:
- Prevents orphaned fact records
- Enables complete aggregations
- Industry-standard approach

---

## ⚙️ ETL Pipeline

### Python Modules

| Module | Purpose | Key Features |
|--------|---------|--------------|
| `main.py` | Orchestrator | CLI, progress bars, error handling |
| `config.py` | Configuration | YAML loading, env merging |
| `extract.py` | CSV Reader | Chunked processing, validation |
| `stage.py` | Staging | Raw preservation, audit columns |
| `transform.py` | Transforms | Cleaning, derived columns |
| `load-dimensions.py` | Dimensions | SCD logic, lookup caching |
| `load-facts.py` | Facts | Batch inserts, key resolution |
| `logging-utils.py` | Logging | DB + file logging |
| `data-quality.py` | Validation | Rule engine, error capture |

### Processing Strategy

```python
# Memory-efficient chunked processing
for chunk in extract_csv_chunks(file, batch_size=50000):
    stage_records(chunk)           # Preserve raw values

for batch in staging_batches:
    for record in batch:
        result = transform_record(record)
        if result.is_valid:
            keys = resolve_dimension_keys(result)
            fact_records.append({**result, **keys})
        else:
            log_rejection(result)

    load_fact_batch(fact_records)  # Bulk insert
```

---

## 🔍 Data Quality Rules

| Field | Rule | Action |
|-------|------|--------|
| `INCIDENT_DT` | Required, valid date | ❌ Reject if invalid |
| `INCIDENT_COUNTY` | NULL allowed | Map to -1 |
| Response times | Must be ≥ 0 | Set NULL if negative |
| `INJURY_FLG` | Yes/No format | Map to 1/0 |
| `NALOXONE_GIVEN_FLG` | 0/1 format | Validate, default 0 |
| All text fields | Trim whitespace | Clean automatically |

---

## 🔧 Configuration

### config.yaml

```yaml
environment: dev

database:
  type: sqlite                    # or sqlserver
  sqlite_path: ./db/ems-warehouse.db
  sqlserver:
    server: localhost
    database: EMS_DW

etl:
  batch_size: 50000               # Rows per batch
  load_type: full                 # full or incremental
  source_path: ./data/input/

logging:
  level: INFO
  log_file: ./logs/etl.log
```

### Environment Switching

```bash
python main.py --env dev     # Development (SQLite)
python main.py --env prod    # Production (SQL Server)
```

---

## 📦 SSIS Implementation

The `.dtsx` package demonstrates:

- **Package Parameters**: File path, connection string, batch size
- **Control Flow**: Initialize → Stage → Dims → Facts → Finalize
- **Data Flow**: Fast Load with Row Count
- **Error Handling**: OnError event handlers
- **Logging**: SQL Tasks calling stored procedures

---

## 📝 Logging & Monitoring

### Logging Tables

```sql
ETL_RUN_LOG      -- One row per execution
ETL_STEP_LOG     -- One row per step
ETL_ERROR_LOG    -- Detailed error capture
```

### Sample Log Output

```
2026-01-06 12:30:00 | INFO     | ════════════════════════════════════════
2026-01-06 12:30:00 | INFO     | ETL Run Started - Run ID: 42
2026-01-06 12:30:00 | INFO     | Source: ems_runs_2024.csv | Environment: dev
2026-01-06 12:30:01 | INFO     | [Step 1] Extract - SUCCESS (0.8s) | Read: 1,544,076
2026-01-06 12:32:15 | INFO     | [Step 2] Stage - SUCCESS (134s) | Inserted: 1,544,076
2026-01-06 12:45:30 | INFO     | [Step 3] Transform & Load - SUCCESS (792s)
2026-01-06 12:45:30 | INFO     | ETL Run Completed - Status: SUCCESS
```

---

## 📋 Design Decisions

| Decision | Rationale |
|----------|-----------|
| **SQLite for demo** | Zero setup, reviewers can query immediately |
| **SQL Server DDL** | Production-ready, matches target environment |
| **50K batch size** | Balance memory usage vs. performance |
| **Type 2 SCD for County/Provider** | Geographic and organizational changes over time |
| **Shared DIM_DISPOSITION** | ED and Hospital use same code set |
| **-1 for unknown** | Industry standard, prevents NULL FKs |

### Assumptions

1. Source data follows NEMSIS-compliant format
2. Incident date is primary date of record
3. One source row = one complete EMS incident
4. Service levels follow standard EMS certification hierarchy

---

## 📊 Sample Queries

### Response Time by County

```sql
SELECT c.county_name,
       ROUND(AVG(f.provider_to_scene_mins), 2) as avg_response,
       COUNT(*) as incidents
FROM FACT_EMS_INCIDENT f
JOIN DIM_COUNTY c ON f.county_key = c.county_key
JOIN DIM_DATE d ON f.date_key = d.date_key
WHERE d.year_num = 2024
GROUP BY c.county_name
ORDER BY avg_response;
```

### Naloxone Trends

```sql
SELECT d.month_name,
       SUM(f.naloxone_given_flg) as naloxone_count,
       ROUND(100.0 * SUM(f.naloxone_given_flg) / COUNT(*), 2) as pct
FROM FACT_EMS_INCIDENT f
JOIN DIM_DATE d ON f.date_key = d.date_key
WHERE d.year_num = 2024
GROUP BY d.month_num, d.month_name
ORDER BY d.month_num;
```

---

<p align="center">

```
        / \__         Woof ! Woof ! I'd hire him !!!
       (    @\___    / But I am biased because he feeds me.. yum! yum!
       /         O
      /   (_____/
     /_____/   U
```

</p>
