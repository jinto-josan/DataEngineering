# Eco-Pulse: Air Quality Intelligence Pipeline

**Eco-Pulse** is an advanced data engineering pipeline for analyzing air quality metrics across Indian cities. It integrates real-time air quality data from the OpenAQ API with geographical and demographic information to provide comprehensive environmental intelligence.

<img width="3070" height="1790" alt="Screenshot 2026-07-11 at 9 51 14 AM" src="https://github.com/user-attachments/assets/4361a0f0-6402-42bc-a242-ae01c14b2548" />
<img width="3222" height="1670" alt="Screenshot 2026-07-11 at 9 50 19 AM" src="https://github.com/user-attachments/assets/f03160ec-567e-444e-a052-94c30b40a423" />


## 📋 Overview

Eco-Pulse implements a modern medallion architecture (Bronze-Silver-Gold) to process air quality index (AQI) data, population statistics, and surface temperature measurements. The pipeline ingests streaming data, performs data quality transformations, and generates analytics-ready datasets for environmental monitoring.

## 🏗️ Architecture

The project follows the **Medallion Architecture** pattern:

### Bronze Layer (Raw Data)
- **`aqi-bronze.py`** - Ingests streaming AQI data from OpenAQ in parquet format
  - Uses Databricks Auto Loader (cloudFiles) for schema inference
  - Captures metadata (file arrival timestamp, source file path)
  - Handles schema evolution with rescued data column
  - Streams data to Delta Lake bronze table

### Silver Layer (Cleaned & Deduplicated)
- **`aqi-silver.py`** - Transforms raw AQI data with quality checks and deduplication
- **`aqi-silver-batch.py`** - Batch processing variant for non-streaming scenarios

### Gold Layer (Business Intelligence)
- **`aqi-gold.py`** - Aggregates cleaned data into analytics-ready datasets
  - City-level AQI aggregations
  - Time-series analytics
  - Environmental health indicators

## 📊 Data Sources & Processing

### 1. **Air Quality Index (AQI) Data**
**Source:** [OpenAQ API](https://openaq.org/)
- Real-time PM2.5, PM10, NO₂, O₃, and other pollutant measurements
- Global monitoring stations with focus on Indian cities

**Processing Pipeline:**
```
data-generation-aqi.py → Map cities to OpenAQ location IDs → Fetch historical records
                      → Bronze Layer (streaming) → Silver Layer (transform) → Gold Layer (analytics)
```

**Key Features:**
- Maps 100K+ population Indian cities to OpenAQ sensor locations (within 25km radius)
- Filters for PM2.5 sensors (deepest lung tissue penetration)
- Leverages CPCB (Central Pollution Control Board) data where available
- Rate-limited API calls with intelligent backoff

### 2. **Geographic Reference Data**
**Source:** [GeoNames Database](https://www.geonames.org/)
- Indian cities latitude/longitude, elevation, timezone
- Administrative divisions and population data
- Feature classifications (cities, capitals, towns)

**Processing:**
```
data-generation-population.py → Parse GeoNames CSV → Population dimension table
```

**Schema includes:**
- City coordinates and elevation
- Population metrics
- Administrative hierarchies
- Timezone information

### 3. **Surface Temperature Data**
**Source:** Climate/Weather APIs
- Surface temperature measurements for correlation analysis
- Seasonal variation analysis

**Processing:**
```
data-generation-surface-temp.py → Temperature aggregations → Silver/Gold layers
```

### 4. **Streaming Simulation**
**Source:** Simulated AQI data stream
```
aqi-streaming-simulation.py → Generates synthetic AQI records → Bronze layer ingestion
```

## 🗂️ Project Structure

```
eco-pulse/
├── README.md                              # This file
├── Data Generation & Ingestion
│   ├── data-generation-aqi.py            # Fetch AQI data from OpenAQ API
│   ├── data-generation-population.py     # Load city population dimensions
│   ├── data-generation-surface-temp.py   # Temperature data ingestion
│   └── aqi-streaming-simulation.py       # Generate simulated AQI streams
├── Bronze Layer
│   └── aqi-bronze.py                     # Auto Loader → streaming ingestion
├── Silver Layer
│   ├── aqi-silver.py                     # Data cleaning & deduplication
│   └── aqi-silver-batch.py               # Batch variant
├── Gold Layer
│   └── aqi-gold.py                       # Analytics aggregations
└── databricks-helpers/                    # Utility modules for Databricks integration
```

## 🚀 Getting Started

### Prerequisites
- Databricks workspace with Delta Lake enabled
- Python 3.8+
- Required libraries:
  - `pyspark`
  - `pandas`
  - `openaq` (Python SDK for OpenAQ API)
  - `databricks-helpers` (local module)

### Setup Instructions

1. **Clone & Install Dependencies**
   ```bash
   git clone https://github.com/jinto-josan/DataEngineering.git
   cd eco-pulse
   pip install -e ./databricks-helpers
   pip install openaq
   ```

2. **Configure Databricks Connection**
   - Set up `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables
   - Create a catalog named `aura_india` in your Databricks workspace
   - Update workspace paths in helper configuration

3. **Configure OpenAQ API Key**
   - Register at [OpenAQ Platform](https://platform.openaq.org/)
   - Set API key in `data-generation-aqi.py`:
     ```python
     API_KEY = "your_api_key_here"
     ```

4. **Run Pipeline in Order**
   ```bash
   # 1. Generate dimension tables
   databricks notebooks run --notebook-path eco-pulse/data-generation-population.py
   
   # 2. Map cities to OpenAQ locations
   databricks notebooks run --notebook-path eco-pulse/data-generation-aqi.py
   
   # 3. Start bronze layer ingestion
   databricks notebooks run --notebook-path eco-pulse/aqi-bronze.py
   
   # 4. Transform in silver layer
   databricks notebooks run --notebook-path eco-pulse/aqi-silver.py
   
   # 5. Generate analytics in gold layer
   databricks notebooks run --notebook-path eco-pulse/aqi-gold.py
   ```

## 📈 Data Flow

```
OpenAQ API / GeoNames              Simulated Streams
      ↓                                    ↓
Data Generation Layer (population, AQI, temperature)
      ↓
Raw Data Volume (Databricks Unity Catalog)
      ↓
┌─────────────────────────────────┐
│  BRONZE LAYER (Auto Loader)     │
│  - Raw AQI data (parquet)       │
│  - File metadata capture        │
└─────────────────────────────────┘
      ↓
┌─────────────────────────────────┐
│  SILVER LAYER (Transformations) │
│  - Data cleaning                │
│  - Deduplication                │
│  - Quality checks               │
│  - Join with dimensions         │
└─────────────────────────────────┘
      ↓
┌─────────────────────────────────┐
│  GOLD LAYER (Analytics)         │
│  - AQI aggregations (city/time) │
│  - Health indicators            │
│  - Trend analysis               │
└─────────────────────────────────┘
      ↓
BI Tools / Dashboards / ML Models
```

## 🔑 Key Features

### ✨ Auto Schema Inference
- Databricks Auto Loader (`cloudFiles`) automatically infers schema from incoming parquet files
- Gracefully handles schema evolution
- Rescued data column captures malformed records

### 🔄 Streaming Architecture
- Real-time AQI data ingestion
- Checkpoint-based fault tolerance
- Append-mode output for time-series data

### 🗺️ Geospatial Intelligence
- 25km radius search for AQI sensors around cities
- Intelligent API rate limiting with backoff
- PM2.5 sensor prioritization for respiratory health analysis

### 📊 Dimensional Modeling
- City dimension tables with hierarchies
- Temporal aggregations (hourly, daily, weekly, monthly)
- Multi-dimensional AQI analytics

## 📝 Table Schemas

### Bronze AQI Table
```
Column                  | Type      | Description
─────────────────────────────────────────────────
location_id            | Long      | OpenAQ location identifier
city                   | String    | City name
latitude               | Double    | Geographic latitude
longitude              | Double    | Geographic longitude
parameter              | String    | Pollutant type (PM2.5, PM10, etc.)
value                  | Double    | Measured value
unit                   | String    | Unit of measurement
timestamp              | Timestamp | Measurement timestamp
file_arrival_timestamp | Timestamp | When data arrived in system
source_file_path       | String    | Original S3 file path
```

### City Location Mapping Table
```
Column        | Type      | Description
───────────────────────────────────────
city          | String    | Indian city name
latitude      | Double    | City center latitude
longitude     | Double    | City center longitude
location_id   | Long      | Mapped OpenAQ location ID
distance      | Double    | Distance to sensor (km)
status        | String    | SUCCESS/NOT_FOUND/Error
is_mobile     | Boolean   | Mobile monitoring station?
is_monitor    | Boolean   | Permanent monitoring station?
datetime_first| Timestamp | First measurement date
datetime_last | Timestamp | Last measurement date
```

## 🛠️ Helper Utilities

The `databricks-helpers/` module provides:
- `DatabricksHelpers` class for workspace navigation
- Catalog/schema/volume directory management
- Table existence checks
- Standardized path conventions

## 📊 Analytics Examples

### City-Level AQI Summary
```sql
SELECT 
  city,
  DATE_TRUNC('day', timestamp) as date,
  AVG(value) as avg_aqi,
  MAX(value) as max_aqi,
  MIN(value) as min_aqi,
  STDDEV(value) as aqi_volatility
FROM aura_india.gold_aqi
WHERE parameter = 'pm25'
GROUP BY city, DATE_TRUNC('day', timestamp)
ORDER BY city, date DESC
```

### Pollutant Comparison
```sql
SELECT 
  parameter,
  city,
  AVG(value) as avg_value,
  COUNT(*) as measurement_count
FROM aura_india.silver_aqi
GROUP BY parameter, city
ORDER BY avg_value DESC
```

## 🔐 Security & Best Practices

- **API Key Management**: Use Databricks Secrets for storing OpenAQ API key
- **Rate Limiting**: Implements intelligent backoff to respect API quotas
- **Data Privacy**: PII-safe aggregations for reporting
- **Schema Validation**: Rescued data columns prevent ingestion failures
- **Checkpoint Recovery**: Exactly-once semantics for streaming data

## 🚨 Troubleshooting

### Issue: API Rate Limit Exceeded
**Solution:** The pipeline includes exponential backoff. Increase `sleep_time` multiplier in `data-generation-aqi.py`

### Issue: Schema Mismatch in Bronze Layer
**Solution:** Check `_rescued_data_bronze` column for malformed records. The `mergeSchema` option allows evolution.

### Issue: OpenAQ Sensor Not Found
**Solution:** Increase `radius` limit in city-to-location mapping (currently 25km). Some remote areas may lack sensors.

### Issue: Databricks Volume Path Not Found
**Solution:** Verify `databricks_helpers.databricks_helper` configuration matches your catalog/schema setup

## 📚 References

- [OpenAQ API Documentation](https://api.openaq.org/v3/docs)
- [GeoNames Database](https://www.geonames.org/export/web-services.html)
- [Databricks Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Databricks Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [Medallion Architecture](https://www.databricks.com/blog/2021/08/09/data-exfiltration-attacks-and-prevention.html)

## 📄 License

This project is part of the DataEngineering repository. See parent repository for license details.

## 👤 Author

**Jinto Josan** - [GitHub](https://github.com/jinto-josan)

---

**Last Updated:** July 11, 2026
