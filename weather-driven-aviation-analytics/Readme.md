# Aviation Analytics & Weather-Driven Disruption Pipeline

Building a production-ready ELT pipeline that correlates severe weather events with flight delays and cancellations.

## Data Sources
- **Flights**: US Bureau of Transportation Statistics (BTS) Flight Data (monthly CSVs).
  - [BTS data library](https://www.transtats.bts.gov/DataIndex.asp) -> then click on aviation in data finder -> airline -Airline On-Time Performance Data -> Reporting Carrier On-Time Performance (1987-present)
- **Weather**: NOAA Global Historical Climatology Network (GHCN) or Meteostat API.
  - 

## Problem Statement
The objective is to build a robust pipeline that handles late-arriving data and schema changes, providing a secure, governed dashboard for airline operational teams.

## Pipeline Architecture

### 🥉 Bronze Layer (Ingestion)
Ingest raw flight logs (CSV) and weather data (JSON) using Databricks Auto Loader (`cloudFiles`).
- **The Trick**: Simulate streaming data and handle corrupted JSON files using a `_rescued_data` column.

### 🥈 Silver Layer (Processing & Delta)
Clean, filter, and join the datasets using PySpark.
- **The Trick**: Handle schema evolution when the weather API introduces new fields mid-stream.
- **The Trick**: Update historical flight records with late-arriving cancellation statuses using Delta `MERGE INTO` (Upserts).
- **The Trick**: Use Delta Time Travel to reverse accidental overwrites.

### 🥇 Gold Layer (Aggregations)
Create business-level tables optimized for BI.
- **The Trick**: Optimize tables for read-heavy queries using `OPTIMIZE` and `ZORDER` by date and airport code.

## ⚙️ Orchestration & Governance
- **Databricks Workflows**: Schedule the pipeline with task dependencies between layers.
- **Unity Catalog**: Register all tables and configure table-level access control.

## 📊 Dashboarding
Build a Databricks SQL Dashboard visualizing the top 5 airports impacted by snowstorms.