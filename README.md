# Automated ELT Pipeline with dbt + Snowflake + Airflow

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![dbt](https://img.shields.io/badge/dbt-1.7+-orange?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-017CEE?logo=apacheairflow)
![GitHub Actions](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=githubactions)
![License](https://img.shields.io/badge/License-MIT-green)

A production-grade ELT pipeline that extracts data from multiple public APIs (Reddit, OpenWeather, and a SaaS PostgreSQL database), loads raw data into Snowflake, and transforms it using dbt with full testing, documentation, and lineage. Apache Airflow orchestrates the daily refresh, with CI/CD via GitHub Actions and Slack alerting on failures.


## Demo

![Project Demo](screenshots/project-demo.png)

*Pipeline execution dashboard showing Airflow DAG status, extraction metrics, dbt model lineage, and terminal output*

## Architecture

```
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”      â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”      â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â” 
â”‚   Reddit API    â”‚     â”‚ OpenWeather API  â”‚     â”‚  PostgreSQL DB  â”‚
â”‚  (PRAW/httpx)   â”‚     â”‚   (REST/JSON)    â”‚     â”‚  (SaaS Source)  â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”˜     â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”˜     â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”˜
         â”‚                       â”‚                       â”‚
         â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”´â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                     â”‚                       â”‚
                     â–¼                       â–¼
            â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â” 
            â”‚         Python Extractors            â”‚
            â”‚   (Async httpx + rate limiting)       â”‚
            â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                             â”‚
                             â–¼
            â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â” 
            â”‚      Snowflake Raw Schema            â”‚
            â”‚   (RAW_REDDIT, RAW_WEATHER,          â”‚
            â”‚    RAW_SAAS)                         â”‚
            â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                             â”‚
                             â–¼
            â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â” 
            â”‚          dbt Transformations          â”‚
            â”‚                                       â”‚
            â”‚  Staging â”€â”€â–¶ Intermediate â”€â”€â–¶ Marts   â”‚
            â”‚                                       â”‚
            â”‚  â€¢ not_null / unique tests            â”‚
            â”‚  â€¢ accepted_values tests              â”‚
            â”‚  â€¢ freshness checks                   â”‚
            â”‚  â€¢ Full lineage graph                 â”‚
            â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                             â”‚
                             â–¼
            â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â” 
            â”‚        Analytics / Dashboards         â”‚
            â”‚     (Metabase / Looker Studio)        â”‚
            â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜

            Orchestration: Apache Airflow (Daily DAG)
            CI/CD: GitHub Actions (dbt test on PR)
            Alerting: Slack Webhooks
```

## Key Business Insights

1.  **Reddit Sentiment Trends**: Track subreddit activity, post sentiment, and engagement patterns over time to identify trending topics and community health metrics.
2.  **Weather-Correlated Behavior**: Join weather data with Reddit activity to uncover how weather patterns influence online engagement - a cross-domain analytical insight that demonstrates real business value.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Extraction | Python 3.9+, httpx, PRAW, psycopg2 |
| Loading | Snowflake Connector for Python |
| Transformation | dbt Core 1.7+ |
| Orchestration | Apache Airflow 2.7+ |
| Warehouse | Snowflake |
| CI/CD | GitHub Actions |
| Alerting | Slack Webhooks |
| Dashboards | Metabase / Looker Studio |
| Containerization | Docker + Docker Compose |

## Project Structure

```
â”œâ”€â”€ dags/                          # Airflow DAG definitions
â”‚   â”œâ”€â”€ elt_daily_pipeline.py      # Main daily ELT DAG
â”‚   â””â”€â”€ dbt_run_dag.py             # dbt-specific DAG
â”œâ”€â”€ dbt_project/                   # dbt project root
â”‚   â”œâ”€â”€ models/
â”‚   â”‚   â”œâ”€â”€ staging/               # 1:1 source mappings, cleaning
â”‚   â”‚   â”œâ”€â”€ intermediate/          # Business logic joins
â”‚   â”‚   â””â”€â”€ marts/                 # Final analytics tables
â”‚   â”œâ”€â”€ tests/                     # Custom data tests
â”‚   â”œâ”€â”€ macros/                    # Reusable SQL macros
â”‚   â”œâ”€â”€ seeds/                     # Static reference data
â”‚   â””â”€â”€ snapshots/                 # SCD Type 2 snapshots
â”œâ”€â”€ extractors/                    # Python extraction modules
â”‚   â”œâ”€â”€ reddit_extractor.py
â”‚   â”œâ”€â”€ weather_extractor.py
â”‚   â””â”€â”€ saas_db_extractor.py
â”œâ”€â”€ loaders/                       # Snowflake loading modules
â”‚   â””â”€â”€ snowflake_loader.py
â”œâ”€â”€ config/                        # Configuration files
â”‚   â””â”€â”€ settings.py
â”œâ”€â”€ scripts/                       # Setup & utility scripts
â”‚   â”œâ”€â”€ setup_snowflake.sql
â”‚   â””â”€â”€ setup_airflow.sh
â”œâ”€â”€ tests/                         # Python unit tests
â”œâ”€â”€ .github/workflows/             # CI/CD pipelines
â”‚   â””â”€â”€ dbt_ci.yml
â”œâ”€â”€ docker-compose.yml
â”œâ”€â”€ Dockerfile
â”œâ”€â”€ requirements.txt
â””â”€â”€ .env.example
```

## Setup Instructions

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Snowflake account (free trial works)
- Reddit API credentials
- OpenWeather API key (free tier)
- Slack webhook URL (optional)

### 1. Clone & Configure

```bash
git clone https://github.com/your-username/elt-pipeline-dbt-snowflake-airflow.git
cd elt-pipeline-dbt-snowflake-airflow
cp .env.example .env
# Edit .env with your credentials
```

### 2. Set Up Snowflake

```bash
# Run the Snowflake setup script in your Snowflake worksheet
# or use snowsql:
snowsql -f scripts/setup_snowflake.sql
```

### 3. Install Dependencies

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Initialize dbt

```bash
cd dbt_project
dbt deps
dbt seed
dbt run
dbt test
```

### 5. Start Airflow (Docker)

```bash
docker-compose up -d
# Access Airflow UI at http://localhost:8080
# Default credentials: airflow / airflow
```

### 6. Enable the DAG

Navigate to Airflow UI â†’ enable `elt_daily_pipeline` DAG â†’ trigger manually or wait for schedule.

## Test Results

All unit tests pass - covering extraction logic, data normalization, incremental watermarks, Snowflake loading patterns, and dedup window functions.

![Test Results](screenshots/test-results.png)

**15 tests passed** across 4 test suites:
- `TestRedditExtraction` - post normalization, engagement ratio, rate limiting
- `TestWeatherExtraction` - temperature fields, Câ†’F conversion, city coordinates
- `TestSaasDBExtraction` - watermark updates, incremental queries, batch sizing
- `TestSnowflakeLoading` - VARIANT JSON serialization, dedup logic

## CI/CD

Every pull request triggers:
1. `dbt compile` - validates SQL syntax
2. `dbt test` - runs all data tests against a CI schema
3. Linting via `sqlfluff`

## License

MIT

## About the Maintainer

This project is actively maintained by Jyothi Sree, a Senior Data Engineer with 6+ years of experience in building large-scale batch and streaming pipelines across various cloud platforms and data technologies. Jyothi specializes in data modeling, warehousing, Lakehouse architecture, CDC, orchestration, and governance, delivering analytics-ready datasets for enterprise production environments.

- **Email**: Jyothisree.work@gmail.com
- **LinkedIn**: https://www.linkedin.com/in/jyothisree123/