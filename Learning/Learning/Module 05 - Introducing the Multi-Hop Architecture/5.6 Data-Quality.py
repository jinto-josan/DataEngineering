# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality in the Multi-Hop Architecture
# MAGIC
# MAGIC In the previous exercises, we built Bronze, Silver, and Gold layers for our EV charging data. But how do we ensure the data flowing through these layers is correct, complete, and trustworthy?
# MAGIC
# MAGIC This notebook explores **data quality practices** that should be applied throughout the multi-hop architecture. The goal is simple but critical: **after each layer, we want a consistent, quality-assured state**.
# MAGIC
# MAGIC We'll explore three key areas:
# MAGIC 1. **Input Validation** - Understanding and validating data before transformation
# MAGIC 2. **Transformation Checks** - Verifying correctness during data processing
# MAGIC 3. **Output Verification** - Ensuring outputs meet quality standards before handoff

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Data Quality Matters: Real-World Examples
# MAGIC
# MAGIC Before diving into techniques, let's acknowledge what we're really fighting against. In the real world, data arrives broken in countless creative ways. These aren't edge cases - they're the norm. Understanding common failure modes helps you build more robust pipelines and ask better questions about your source data.
# MAGIC
# MAGIC **Structural Issues:**
# MAGIC - **Null values** where data should exist - a sensor that failed to report, a required field left blank by a user, or an upstream system that silently drops data under load
# MAGIC - **Malformed data** - JSON with missing brackets, CSVs with unescaped commas that shift all subsequent columns, XML with invalid characters, or records truncated mid-field due to buffer limits
# MAGIC - **Unexpected newlines** - a single record split across multiple lines because someone included a line break in an address field or comment, breaking row-based parsing
# MAGIC - **Merged fields** - "John Smith, john@email.com, +1-555-0123" crammed into a single "name" column because someone exported from a system that used a different delimiter
# MAGIC
# MAGIC **Temporal and Completeness Issues:**
# MAGIC - **Missing time frames** - gaps in hourly data because a system was down for maintenance, a network partition dropped messages, or daylight saving time caused duplicate/missing hours
# MAGIC - **Late-arriving data** - records that show up days after they should have, requiring reprocessing or special handling in aggregations
# MAGIC - **Duplicate records** - the same event recorded twice because of retry logic, message queue redelivery, or "just to be safe" manual re-runs
# MAGIC
# MAGIC **Encoding and Internationalization:**
# MAGIC - **Illegal or unexpected characters** - emoji in customer names crashing legacy systems, control characters from mainframe exports, invisible Unicode characters (zero-width spaces) that make "identical" strings not match
# MAGIC - **Internationalization issues** - dates as DD/MM/YYYY vs MM/DD/YYYY (is 01/02/2024 January 2nd or February 1st?), Spanish date formats like "15 de enero de 2024" or "15-ene-2024" with localized month names, numbers with commas as decimal separators (1.000,50 vs 1,000.50), timezone-naive timestamps that shift by hours depending on interpretation
# MAGIC
# MAGIC **Semantic and Business Logic Issues:**
# MAGIC - **Categorical proxies instead of proper nulls** - "999" meaning "unknown", "-1" for "not applicable", "N/A" as a string, empty strings instead of null - all requiring special handling
# MAGIC - **Generic formats without metadata** - CSVs without headers where you're guessing which column is which, fixed-width files without documentation, binary formats with no schema
# MAGIC - **Orphan references** - foreign keys pointing to records that don't exist (deleted? never created? wrong environment?), breaking joins and referential integrity
# MAGIC - **Synthetic or test data in production** - records from "John Doe" at "123 Test Street" or "test@example.com" that slipped through environment boundaries and now pollute your analytics
# MAGIC
# MAGIC These aren't hypothetical scenarios - they're Tuesday. Every experienced data engineer has war stories about each of these. The question isn't whether you'll encounter them, but whether you'll catch them before they corrupt your downstream outputs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## A Mindset for Data Quality
# MAGIC
# MAGIC Beyond specific techniques and tools, the most important thing you can develop is a disciplined mental approach to working with data. Tools check what you tell them to check; your intuition catches what you didn't think to automate.
# MAGIC
# MAGIC ### Set Expectations Before Every Operation
# MAGIC
# MAGIC **Before running each cell, ask yourself: "What do I expect to see?"**
# MAGIC
# MAGIC Have a hypothesis. If you're filtering to transactions from 2023, estimate roughly how many rows you expect based on what you know about the data volume. If you're joining two tables, predict whether the row count will go up (many-to-many or one-to-many), down (inner join filtering), or stay roughly the same (one-to-one). If you're aggregating, sanity-check whether the resulting numbers make business sense.
# MAGIC
# MAGIC This isn't about being right every time - it's about engaging actively with your data rather than passively running code and hoping for the best.
# MAGIC
# MAGIC ### Investigate Every Surprise
# MAGIC
# MAGIC **When reality differs from expectation: investigate, don't ignore.**
# MAGIC
# MAGIC A mismatch between expectation and result is valuable information. There are only two possibilities:
# MAGIC 1. Your mental model was wrong - great, you learned something important about the data
# MAGIC 2. The data has an issue - great, you caught it before it propagated downstream
# MAGIC
# MAGIC Either way, understanding the "why" prevents bugs and builds your intuition. The worst thing you can do is shrug and move on. That unexpected row count, that weird distribution, that column with 30% nulls when you expected 0% - these are clues. Follow them.
# MAGIC
# MAGIC ### Design Outputs for Your Audience
# MAGIC
# MAGIC **For visual outputs: think like your audience, not like yourself.**
# MAGIC
# MAGIC If your deliverable is a chart, dashboard, or report, step back and evaluate it from your stakeholder's perspective:
# MAGIC - Does it tell a clear story in the first 10 seconds?
# MAGIC - Could someone with less technical context than you understand the key insight without explanation?
# MAGIC - Is it actionable - does it help someone make a decision or take an action?
# MAGIC - Have you removed jargon, unnecessary complexity, and "interesting to engineers but not to the business" details?
# MAGIC
# MAGIC A visualization that requires a 10-minute explanation isn't a good visualization - it's a draft.
# MAGIC
# MAGIC ### Be Your Own First Customer
# MAGIC
# MAGIC **For data products: try using your own output before handing it off.**
# MAGIC
# MAGIC If you're producing a dataset, API, or table for downstream teams, don't just validate it technically - actually try to use it:
# MAGIC - Build a simple query or analysis that a consumer might build. Is it intuitive or frustrating?
# MAGIC - Read your own documentation as if you'd never seen the data before. Does it actually help?
# MAGIC - Check the metadata: Are column descriptions meaningful ("transaction identifier" vs just "id")? Are units specified? Are valid value ranges documented?
# MAGIC - Ask yourself honestly: Would you enjoy working with this data if someone else had produced it?
# MAGIC
# MAGIC The few hours you spend as your own first customer will save your downstream colleagues days of confusion and back-and-forth questions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Three Pillars of Data Quality
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                        DATA QUALITY FRAMEWORK                               │
# MAGIC ├─────────────────────┬─────────────────────┬─────────────────────────────────┤
# MAGIC │  INPUT VALIDATION   │ TRANSFORMATION CHECK│    OUTPUT VERIFICATION          │
# MAGIC │                     │                     │                                 │
# MAGIC │  • Schema checks    │ • Row count diffs   │  • Sampling & inspection        │
# MAGIC │  • Null analysis    │ • Join verification │  • Visualization                │
# MAGIC │  • Domain values    │ • Filter validation │  • Data contracts               │
# MAGIC │  • Volume tracking  │ • Calculation sanity│  • Replicability                │
# MAGIC │                     │                     │                                 │
# MAGIC │     BEFORE          │      DURING         │        AFTER                    │
# MAGIC └─────────────────────┴─────────────────────┴─────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automation vs. Human Judgment
# MAGIC
# MAGIC Data quality is a balance between **automated checks** that can run continuously and **human judgment** for edge cases and business context.
# MAGIC
# MAGIC | Aspect | Automated | Human Judgment |
# MAGIC |--------|-----------|----------------|
# MAGIC | Schema validation | Programmatic type checks | Semantic meaning of fields |
# MAGIC | Null handling | Count and percentage | Which nulls are acceptable |
# MAGIC | Value ranges | Statistical bounds | Business rule exceptions |
# MAGIC | Anomaly detection | Statistical outliers | Valid edge cases |
# MAGIC
# MAGIC In this notebook, we'll show **both approaches** - reusable functions for automation and exploratory techniques for investigation.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup
# MAGIC
# MAGIC Before we begin, let's set up our environment and load data from all three layers (Bronze, Silver, Gold) so we can demonstrate quality checks at each stage.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

exercise_name = "data_quality_demo"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()
print(f"Your current working directory is: {working_directory}")

helpers.clean_working_directory()

# COMMAND ----------

# Download Bronze layer data (raw ingested)
bronze_url = "https://raw.githubusercontent.com/kelseymok/charge-point-simulator-v1.6/main/out/1680355141.csv.gz"
bronze_filepath = helpers.download_to_local_dir(bronze_url)

# Download Silver layer data
silver_urls = {
    "meter_values_request": "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/MeterValuesRequest/part-00000-tid-468425781006758111-f9d48bc3-3b4c-497e-8e9c-77cf63db98f8-207-1-c000.snappy.parquet",
    "start_transaction_request": "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionRequest/part-00000-tid-9191649339140138460-0a4f58e5-1397-41cc-a6a1-f6756f3332b6-218-1-c000.snappy.parquet",
    "start_transaction_response": "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionResponse/part-00000-tid-5633887168695670016-762a6dfa-619c-412d-b7b8-158ee41df1b2-185-1-c000.snappy.parquet",
    "stop_transaction_request": "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StopTransactionRequest/part-00000-tid-5108689541678827436-b76f4703-dabf-439a-825d-5343aabc03b6-196-1-c000.snappy.parquet"
}

silver_filepaths = {name: helpers.download_to_local_dir(url) for name, url in silver_urls.items()}

print("All data downloaded successfully!")

# COMMAND ----------

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, lit, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

def read_csv(filepath: str) -> DataFrame:
    return spark.read.csv(filepath, header=True, inferSchema=True)
def read_parquet(filepath: str) -> DataFrame:
    return spark.read.parquet(filepath, header=True, inferSchema=True)

# Load all DataFrames
print(bronze_filepath)
bronze_df = read_csv(bronze_filepath)

silver_meter_values_df = read_parquet(silver_filepaths["meter_values_request"])
silver_start_request_df = read_parquet(silver_filepaths["start_transaction_request"])
silver_start_response_df = read_parquet(silver_filepaths["start_transaction_response"])
silver_stop_request_df = read_parquet(silver_filepaths["stop_transaction_request"])

# Coming from 5.5 Excercise - Gold
gold_filepath = Path(f"{working_directory}/../products/total_energy").resolve()
gold_df = read_parquet(str(gold_filepath))

print("DataFrames loaded:")
print(f"  Bronze: {bronze_df.count()} rows")
print(f"  Silver (MeterValuesRequest): {silver_meter_values_df.count()} rows")
print(f"  Silver (StartTransactionRequest): {silver_start_request_df.count()} rows")
print(f"  Silver (StartTransactionResponse): {silver_start_response_df.count()} rows")
print(f"  Silver (StopTransactionRequest): {silver_stop_request_df.count()} rows")
print(f"  Gold: {gold_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Input Validation
# MAGIC
# MAGIC ### Concept: "Before You Transform, Understand Your Data"
# MAGIC
# MAGIC Input validation is about building trust in your source data before applying any transformations. Think of it as a health check - you want to understand:
# MAGIC
# MAGIC 1. **Schema** - Does the data match expected structure and types?
# MAGIC 2. **Volume** - Is the row count within expected bounds?
# MAGIC 3. **Completeness** - How much data is missing (nulls)?
# MAGIC 4. **Domain values** - Are categorical values within expected sets?
# MAGIC
# MAGIC **Why invest in input validation?** The principle of "garbage in, garbage out" is well-known, but the damage from bad input data is often worse than wasted compute. Corrupted data that makes it through to Gold layer tables can poison dashboards, trigger incorrect business decisions, and erode stakeholder trust in your data platform. The cost of catching a schema mismatch at ingestion is a failed pipeline and an alert. The cost of catching it in production is explaining to executives why last month's revenue report was wrong. Input validation is cheap insurance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Schema Validation (Bronze Layer)
# MAGIC
# MAGIC The first thing to check is whether the data schema matches expectations. Schema drift (unexpected changes) can break downstream processing.
# MAGIC
# MAGIC **Why this matters:** Schema is the contract between data producer and consumer. When a source system changes a column name, adds a new field, or modifies a data type, your pipeline can fail silently or produce incorrect results. By validating schemas at ingestion, you catch these issues immediately rather than discovering corrupted Gold layer data days later. This is especially critical in environments where you don't control the source systems - third-party APIs, vendor data feeds, or other teams' databases can change without notice.

# COMMAND ----------

# Check Bronze schema
print("Bronze Layer Schema:")
bronze_df.printSchema()

# COMMAND ----------

def validate_schema(df: DataFrame, expected_columns: list, name: str = "DataFrame") -> bool:
    """Validate that a DataFrame has the expected columns."""
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    
    missing = expected_set - actual_columns
    extra = actual_columns - expected_set
    
    print(f"Schema validation for {name}:")
    print(f"  Expected columns: {sorted(expected_columns)}")
    print(f"  Actual columns:   {sorted(df.columns)}")
    
    if missing:
        print(f"  MISSING columns: {missing}")
    if extra:
        print(f"  EXTRA columns: {extra}")
    
    is_valid = len(missing) == 0
    print(f"  Result: {'PASS' if is_valid else 'FAIL'}")
    return is_valid

# Validate Bronze schema
expected_bronze_columns = ["message_id", "message_type", "charge_point_id", "action", "write_timestamp", "body"]
validate_schema(bronze_df, expected_bronze_columns, "Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Row Count Validation
# MAGIC
# MAGIC Volume checks help detect data loss, duplication, or unexpected spikes. Establish baselines and monitor for significant deviations.
# MAGIC
# MAGIC **Why this matters:** Row counts are a simple but powerful signal. If your daily data feed typically has 10,000 records and today you received 100, something is wrong - perhaps the source system had an outage, or your extraction query failed. Conversely, if you received 1,000,000 records, you might have accidentally pulled historical data or hit a duplication bug. These problems are invisible if you only look at individual records, but obvious when you track volume over time.

# COMMAND ----------

def validate_row_count(df: DataFrame, min_rows: int, max_rows: int, name: str = "DataFrame") -> bool:
    """Validate that row count is within expected bounds."""
    actual_count = df.count()
    is_valid = min_rows <= actual_count <= max_rows
    
    print(f"Row count validation for {name}:")
    print(f"  Expected range: [{min_rows}, {max_rows}]")
    print(f"  Actual count:   {actual_count}")
    print(f"  Result: {'PASS' if is_valid else 'FAIL'}")
    
    return is_valid

# Example: Bronze should have between 1000 and 100000 rows
validate_row_count(bronze_df, min_rows=1000, max_rows=100000, name="Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Null Analysis
# MAGIC
# MAGIC Understanding null patterns is critical. Some nulls are expected (optional fields), while others indicate data quality issues.
# MAGIC
# MAGIC **Why this matters:** Nulls are not inherently bad - they often represent legitimate missing data (e.g., a customer who hasn't provided a phone number). However, unexpected nulls can indicate extraction failures, schema mismatches, or upstream data corruption. A column that was 100% populated last week but is now 50% null signals a problem. By establishing baseline null rates and monitoring deviations, you can distinguish between "normal" missing data and actual quality issues that need investigation.

# COMMAND ----------

def analyze_nulls(df: DataFrame, name: str = "DataFrame") -> DataFrame:
    """Analyze null counts and percentages for all columns."""
    total_rows = df.count()
    
    # Build null count expressions for each column
    null_counts = []
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        null_counts.append((col_name, null_count, round(null_pct, 2)))
    
    print(f"\nNull Analysis for {name} ({total_rows} total rows):")
    print("-" * 50)
    print(f"{'Column':<25} {'Nulls':<10} {'Pct':<10}")
    print("-" * 50)
    
    for col_name, null_count, null_pct in null_counts:
        status = "" if null_count == 0 else " <-- NULLS PRESENT"
        print(f"{col_name:<25} {null_count:<10} {null_pct:<10}%{status}")
    
    return null_counts

# Analyze nulls in Bronze layer
bronze_null_analysis = analyze_nulls(bronze_df, "Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Domain Value Validation 
# MAGIC
# MAGIC For categorical fields, we should check that values fall within expected domains. Unexpected values may indicate data corruption or schema evolution.
# MAGIC
# MAGIC **Why this matters:** Categorical fields have a finite set of valid values - status codes, country names, transaction types. When a new value appears (e.g., "PENDING_REVIEW" in a status field that previously only had "APPROVED" and "REJECTED"), it could mean the source system added a new state (legitimate evolution) or that data corruption introduced garbage values. Either way, your downstream logic needs to handle it. Domain validation surfaces these changes immediately so you can update your code deliberately rather than having dashboards show "Unknown" categories.

# COMMAND ----------

# Check distinct values for 'action' column in Bronze
print("Distinct 'action' values in Bronze layer:")
display(bronze_df.select("action").distinct().orderBy("action"))

# COMMAND ----------

def validate_domain_values(df: DataFrame, column: str, expected_values: list, name: str = "DataFrame") -> bool:
    """Validate that a column only contains expected values."""
    actual_values = [row[column] for row in df.select(column).distinct().collect()]
    unexpected = set(actual_values) - set(expected_values)
    
    print(f"Domain validation for {name}.{column}:")
    print(f"  Expected values: {sorted(expected_values)}")
    print(f"  Actual values:   {sorted(actual_values)}")
    
    if unexpected:
        print(f"  UNEXPECTED values: {unexpected}")
    
    is_valid = len(unexpected) == 0
    print(f"  Result: {'PASS' if is_valid else 'FAIL'}")
    return is_valid

# Validate expected actions in Bronze
expected_actions = [
    "BootNotification", "Heartbeat", "MeterValues", 
    "StartTransaction", "StopTransaction", "StatusAction"
]
validate_domain_values(bronze_df, "action", expected_actions, "Bronze")

# COMMAND ----------

# Check message_type values (should be 2 for Request, 3 for Response)
print("Distinct 'message_type' values in Bronze layer:")
display(bronze_df.groupBy("message_type").count().orderBy("message_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Silver Layer Validation
# MAGIC
# MAGIC After Bronze processing, we should validate that Silver layer data has the expected structure and completeness.
# MAGIC
# MAGIC **Why this matters:** The Silver layer represents your first transformation of raw data - parsing JSON, splitting columns, applying type conversions. These transformations can fail in subtle ways: a regex that doesn't match edge cases, a type cast that silently produces nulls, or a column rename that breaks. Validating Silver output ensures that the cleaning and structuring logic worked correctly before you build business logic on top of it. Think of it as a checkpoint: if Silver is wrong, everything downstream will be wrong too.

# COMMAND ----------

# Validate Silver StartTransactionRequest schema
print("Silver StartTransactionRequest Schema:")
silver_start_request_df.printSchema()

expected_silver_start_columns = ["charge_point_id", "message_id", "transaction_id", "meter_start", "timestamp"]
validate_schema(silver_start_request_df, expected_silver_start_columns, "Silver StartTransactionRequest")

# COMMAND ----------

# Null analysis for Silver layer
analyze_nulls(silver_start_request_df, "Silver StartTransactionRequest")
analyze_nulls(silver_stop_request_df, "Silver StopTransactionRequest")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Input Validation Summary
# MAGIC
# MAGIC | Check | Automation Level | When to Apply |
# MAGIC |-------|------------------|---------------|
# MAGIC | Schema validation | High - can be fully automated | Every pipeline run |
# MAGIC | Row count bounds | Medium - bounds need human setup | Every pipeline run |
# MAGIC | Null analysis | High - automated counting | Every pipeline run |
# MAGIC | Domain values | Medium - expected values need definition | Categorical columns |
# MAGIC
# MAGIC **Key Takeaway**: Build automated checks for structure, use human judgment for thresholds and acceptable values.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: Transformation Checks
# MAGIC
# MAGIC ### Concept: "Checking Transformations as They Happen"
# MAGIC
# MAGIC The most insidious bugs occur during transformations - filters that remove too many rows, joins that create duplicates, or calculations that produce impossible values. The key principle is **always compare before and after**.
# MAGIC
# MAGIC **Why check during transformation, not just at the end?** Consider a pipeline with 10 transformation steps. If you only validate the final output and something is wrong, you have to trace backward through all 10 steps to find the bug. But if you validated at each step, you know immediately which transformation introduced the problem. This is the difference between "something is wrong somewhere" and "the join in step 4 created duplicates." In complex pipelines, transformation-level checks reduce debugging time from hours to minutes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 The "Before and After" Principle
# MAGIC
# MAGIC For every transformation, we should be able to explain:
# MAGIC - How many rows went in?
# MAGIC - How many rows came out?
# MAGIC - Is that difference expected?
# MAGIC
# MAGIC **Why this matters:** Data pipelines are a series of transformations, and each step can introduce errors. A filter might remove more rows than intended due to a typo in the condition. A join might create duplicates due to unexpected many-to-many relationships. An aggregation might collapse data incorrectly. By tracking row counts before and after each operation, you create an audit trail that makes debugging much easier. When something goes wrong, you can identify exactly which step caused the problem.

# COMMAND ----------

def check_row_count_change(before_df: DataFrame, after_df: DataFrame, operation: str) -> dict:
    """Track row count changes through a transformation."""
    before_count = before_df.count()
    after_count = after_df.count()
    change = after_count - before_count
    pct_change = (change / before_count * 100) if before_count > 0 else 0
    
    print(f"Row count change for '{operation}':")
    print(f"  Before: {before_count}")
    print(f"  After:  {after_count}")
    print(f"  Change: {change:+d} ({pct_change:+.1f}%)")
    
    return {
        "operation": operation,
        "before": before_count,
        "after": after_count,
        "change": change,
        "pct_change": pct_change
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Filter Verification
# MAGIC
# MAGIC When filtering data, always verify that the filter is working as intended and not removing too much (or too little).
# MAGIC
# MAGIC **Why this matters:** Filters are deceptively simple but commonly cause bugs. A condition like `status != 'cancelled'` might accidentally filter out nulls (because `NULL != 'cancelled'` is `NULL`, not `TRUE`). A date filter like `date > '2024-01-01'` might use string comparison instead of date comparison, producing unexpected results. By explicitly checking both kept and removed rows, you verify that the filter behaves as intended. The sum of kept + removed should always equal the original count - if it doesn't, something is very wrong.

# COMMAND ----------

# Example: Filter Bronze to only StartTransaction requests
start_transactions_only = bronze_df.filter(
    (col("action") == "StartTransaction") & (col("message_type") == 2)
)

check_row_count_change(bronze_df, start_transactions_only, "Filter to StartTransaction requests")

# COMMAND ----------

# MAGIC %md
# MAGIC **Remember, count is a costly action! Consider executing this in debug mode only to keep the production pipeline fast.**  
# MAGIC

# COMMAND ----------

def verify_filter(df: DataFrame, filter_condition, condition_name: str) -> bool:
    """Verify a filter removes only the intended rows."""
    before_count = df.count()
    after_df = df.filter(filter_condition)
    after_count = after_df.count()
    
    # Check inverse - rows that were removed
    removed_df = df.filter(~filter_condition)
    removed_count = removed_df.count()
    
    print(f"Filter verification for '{condition_name}':")
    print(f"  Total rows:    {before_count}")
    print(f"  Kept rows:     {after_count}")
    print(f"  Removed rows:  {removed_count}")
    print(f"  Sum check:     {after_count + removed_count} (should equal {before_count})")
    
    is_valid = (after_count + removed_count) == before_count
    print(f"  Result: {'PASS' if is_valid else 'FAIL'}")
    
    return is_valid

# Verify the filter
verify_filter(
    bronze_df, 
    (col("action") == "StartTransaction") & (col("message_type") == 2),
    "StartTransaction requests"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Join Verification
# MAGIC
# MAGIC Joins are a common source of data quality issues:
# MAGIC - **Key uniqueness**: Are join keys unique on the expected side?
# MAGIC - **Fanout detection**: Did a 1:1 join become 1:n (creating duplicates)?
# MAGIC - **Match rate**: What percentage of rows found a match?
# MAGIC
# MAGIC **Why this matters:** Joins are where most data pipeline bugs hide. You expect a 1:1 relationship between orders and customers, but one customer has two records (a duplicate), and suddenly your revenue report is doubled. You expect all transactions to have a matching account, but 5% are orphaned. These issues are especially dangerous because the query "works" - it produces output without errors - but the output is wrong. Pre-join key analysis and post-join row count verification catch these problems before they corrupt your analytics.

# COMMAND ----------

def analyze_join_key(df: DataFrame, key_column: str, name: str = "DataFrame") -> dict:
    """Analyze a join key for uniqueness and null values."""
    total_rows = df.count()
    distinct_keys = df.select(key_column).distinct().count()
    null_keys = df.filter(col(key_column).isNull()).count()
    
    is_unique = distinct_keys == (total_rows - null_keys)
    
    print(f"Join key analysis for {name}.{key_column}:")
    print(f"  Total rows:     {total_rows}")
    print(f"  Distinct keys:  {distinct_keys}")
    print(f"  Null keys:      {null_keys}")
    print(f"  Key is unique:  {is_unique}")
    
    return {
        "total": total_rows,
        "distinct": distinct_keys,
        "nulls": null_keys,
        "is_unique": is_unique
    }

# Analyze join keys before joining
analyze_join_key(silver_start_request_df, "message_id", "StartTransactionRequest")
print()
analyze_join_key(silver_start_response_df, "message_id", "StartTransactionResponse")

# COMMAND ----------

def verify_join(left_df: DataFrame, right_df: DataFrame, join_key: str, 
                join_type: str = "inner", left_name: str = "left", right_name: str = "right") -> dict:
    """Verify a join operation and detect potential issues."""
    left_count = left_df.count()
    right_count = right_df.count()
    
    # Perform the join
    joined_df = left_df.alias("l").join(
        right_df.alias("r"), 
        col(f"l.{join_key}") == col(f"r.{join_key}"), 
        join_type
    )
    joined_count = joined_df.count()
    
    # Analyze key overlap
    left_keys = set([row[join_key] for row in left_df.select(join_key).distinct().collect()])
    right_keys = set([row[join_key] for row in right_df.select(join_key).distinct().collect()])
    matching_keys = len(left_keys & right_keys)
    
    # Detect fanout
    expected_max = max(left_count, right_count)
    has_fanout = joined_count > expected_max
    
    print(f"Join verification ({left_name} {join_type} {right_name} on {join_key}):")
    print(f"  Left count:     {left_count}")
    print(f"  Right count:    {right_count}")
    print(f"  Joined count:   {joined_count}")
    print(f"  Matching keys:  {matching_keys}")
    print(f"  Fanout detected: {has_fanout}")
    
    if has_fanout:
        print(f"  WARNING: Join produced more rows than largest input!")
    
    return {
        "left_count": left_count,
        "right_count": right_count,
        "joined_count": joined_count,
        "matching_keys": matching_keys,
        "has_fanout": has_fanout,
        "joined_df": joined_df
    }

# Verify the StartTransaction Request/Response join
join_result = verify_join(
    silver_start_response_df, 
    silver_start_request_df, 
    "message_id", 
    "inner",
    "StartResponse",
    "StartRequest"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Calculation Sanity Checks
# MAGIC
# MAGIC For calculated fields, verify that results fall within plausible ranges. Domain knowledge is essential here.
# MAGIC
# MAGIC **Why this matters:** Calculations can produce mathematically correct but semantically nonsensical results. A charging session that lasted -3 hours is impossible. An energy reading of 50,000 kWh for a single session is implausible (typical EVs hold 40-100 kWh). These values indicate bugs: perhaps timestamps are in different timezones, or meter readings are cumulative rather than per-session. Domain experts know what "reasonable" looks like - embedding that knowledge into range checks catches calculation errors that pure data validation would miss.

# COMMAND ----------

# Check Gold layer calculated values
print("Gold Layer Schema:")
gold_df.printSchema()

# COMMAND ----------

def check_value_range(df: DataFrame, column: str, min_val: float, max_val: float, 
                      name: str = "DataFrame") -> dict:
    """Check if values fall within expected range."""
    from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg
    
    stats = df.select(
        spark_min(col(column)).alias("min"),
        spark_max(col(column)).alias("max"),
        spark_avg(col(column)).alias("avg")
    ).collect()[0]
    
    actual_min = stats["min"]
    actual_max = stats["max"]
    actual_avg = stats["avg"]
    
    out_of_range = df.filter(
        (col(column) < min_val) | (col(column) > max_val)
    ).count()
    
    is_valid = out_of_range == 0
    
    print(f"Range check for {name}.{column}:")
    print(f"  Expected range:  [{min_val}, {max_val}]")
    print(f"  Actual min:      {actual_min}")
    print(f"  Actual max:      {actual_max}")
    print(f"  Actual avg:      {actual_avg:.2f}" if actual_avg else "  Actual avg:      N/A")
    print(f"  Out of range:    {out_of_range} rows")
    print(f"  Result: {'PASS' if is_valid else 'FAIL'}")
    
    return {
        "min": actual_min,
        "max": actual_max,
        "avg": actual_avg,
        "out_of_range": out_of_range,
        "is_valid": is_valid
    }

# Check total_time: should be positive and reasonable (0-24 hours for a charge session)
check_value_range(gold_df, "total_time", min_val=0, max_val=24, name="Gold")
print()

# Check total_energy: should be positive and reasonable (0-100 kWh for typical EV)
check_value_range(gold_df, "total_energy", min_val=0, max_val=100, name="Gold")

# COMMAND ----------

# Statistical summary of Gold calculated fields
print("Statistical summary of Gold layer:")
display(gold_df.select("total_time", "total_energy").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation Checks Summary
# MAGIC
# MAGIC | Check | Automation Level | When to Apply |
# MAGIC |-------|------------------|---------------|
# MAGIC | Row count before/after | High - fully automated | Every transformation |
# MAGIC | Filter verification | High - automated | After every filter |
# MAGIC | Join key analysis | High - automated | Before every join |
# MAGIC | Fanout detection | High - automated | After every join |
# MAGIC | Value range checks | Medium - ranges need definition | Calculated columns |
# MAGIC
# MAGIC **Key Takeaway**: Instrument your transformations with before/after comparisons. Unexpected changes indicate bugs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: Output Verification
# MAGIC
# MAGIC ### Concept: "Validating Outputs Before Handoff"
# MAGIC
# MAGIC Before data leaves your pipeline (especially Gold layer data consumed by downstream systems or analysts), you must verify it meets quality standards. Think like a consumer: **what would break if this data is wrong?**
# MAGIC
# MAGIC **Why verify outputs separately from transformation checks?** Transformation checks verify that each step worked correctly in isolation. Output verification takes the consumer's perspective: does this data meet their needs? A pipeline can execute perfectly (no bugs, no errors) and still produce data that's useless or dangerous to consumers. Maybe the data is technically correct but missing the last 24 hours. Maybe all the calculations are right but there are duplicates in the primary key. Output verification is your last line of defense before data reaches dashboards, ML models, or regulatory reports.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Sampling and Inspection
# MAGIC
# MAGIC Visual inspection of samples helps catch issues that automated checks miss. Look at:
# MAGIC - Random samples for general quality
# MAGIC - Edge cases (min/max values, nulls)
# MAGIC - Recently added or modified records
# MAGIC
# MAGIC **Why this matters:** Automated checks are necessary but not sufficient. They verify rules you've already thought of, but data quality issues often come from scenarios you didn't anticipate. Looking at actual records with human eyes catches patterns that are hard to codify: a customer name that's obviously a test entry ("John Doe123"), an address that's clearly malformed, or a timestamp that's in the wrong century. Regular sampling keeps you connected to the actual data, not just statistics about it.

# COMMAND ----------

# Random sample of Gold data
print("Random sample from Gold layer (5 rows):")
display(gold_df.sample(fraction=0.1, seed=42).limit(5))

# COMMAND ----------

# Edge cases: min and max total_time
#
# Why inspect edge cases? Random sampling gives you a feel for "typical" data,
# but edge cases (minimum and maximum values) often reveal problems that typical
# records hide:
#
# - MINIMUM values might expose:
#   - Zero-duration sessions (did the transaction actually happen?)
#   - Negative values (calculation bug - perhaps timestamps were swapped?)
#   - Suspiciously short sessions (data truncation or parsing errors?)
#
# - MAXIMUM values might expose:
#   - Unrealistically long sessions (a 72-hour charge session is implausible)
#   - Overflow errors or sentinel values (999999 often means "unknown")
#   - Stuck transactions that never properly closed
#
# This quick check complements statistical summaries - outliers can hide in
# averages but are immediately obvious when you look at the actual records.

from pyspark.sql.functions import min as spark_min, max as spark_max

print("Minimum total_time transactions:")
min_time = gold_df.select(spark_min("total_time")).collect()[0][0]
display(gold_df.filter(col("total_time") == min_time))

print("\nMaximum total_time transactions:")
max_time = gold_df.select(spark_max("total_time")).collect()[0][0]
display(gold_df.filter(col("total_time") == max_time))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Visualization for Quality
# MAGIC
# MAGIC Charts help identify patterns, outliers, and anomalies that are hard to spot in tabular data.
# MAGIC
# MAGIC **Why this matters:** The human visual system is remarkably good at pattern recognition. A histogram instantly shows if your distribution is skewed, bimodal, or has unexpected gaps. A scatter plot reveals correlations (or their absence) at a glance. A box plot highlights outliers that might be buried in millions of rows. These visual insights often reveal problems that summary statistics hide: two distributions can have the same mean and standard deviation but look completely different when plotted. Visualization is how you develop intuition about your data's true shape.

# COMMAND ----------

import plotly.express as px
import pandas as pd

# Convert to Pandas for visualization
gold_pandas = gold_df.toPandas()

# Histogram of total_time
fig = px.histogram(
    gold_pandas, 
    x="total_time", 
    nbins=30,
    title="Distribution of Charging Session Duration (hours)",
    labels={"total_time": "Total Time (hours)", "count": "Number of Sessions"}
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **How to interpret this histogram:**
# MAGIC
# MAGIC A histogram shows the *distribution* of values - how frequently each range of values occurs. For charging session duration, ask yourself:
# MAGIC
# MAGIC - **Is the shape plausible?** We expect most charging sessions to cluster around typical use cases (quick top-ups of 15-30 minutes, or longer charges of 1-3 hours). A reasonable distribution might be right-skewed (many short sessions, fewer long ones).
# MAGIC
# MAGIC - **Are there unexpected gaps?** Missing bars in the middle of the distribution could indicate data loss or filtering errors.
# MAGIC
# MAGIC - **Are there suspicious spikes?** A large spike at exactly 0, 1, or 24 hours might indicate default values, rounding errors, or placeholder data rather than real measurements.
# MAGIC
# MAGIC - **Does the range make sense?** Charging sessions shorter than a few minutes or longer than 12+ hours are unusual and worth investigating.
# MAGIC
# MAGIC **What we see here:** Look at where the bulk of the data falls. Is this consistent with how EV charging typically works? If you see anomalies, they might indicate data quality issues upstream.

# COMMAND ----------

# Histogram of total_energy
fig = px.histogram(
    gold_pandas, 
    x="total_energy", 
    nbins=30,
    title="Distribution of Energy Dispensed (kWh)",
    labels={"total_energy": "Total Energy (kWh)", "count": "Number of Sessions"}
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **How to interpret this histogram:**
# MAGIC
# MAGIC For energy dispensed (kWh), consider the physical constraints of EV charging:
# MAGIC
# MAGIC - **Expected range:** Most EVs have batteries between 40-100 kWh. A single charging session typically delivers 10-60 kWh (partial charges are common). Values above 100 kWh for a single session are suspicious.
# MAGIC
# MAGIC - **Distribution shape:** We'd expect a spread of values reflecting different battery sizes and charging needs. A very uniform distribution might indicate synthetic or placeholder data. A strong peak at certain values could indicate common battery sizes or charging habits.
# MAGIC
# MAGIC - **Zero or near-zero values:** A spike at 0 kWh could indicate failed sessions, test data, or transactions where charging didn't actually occur.
# MAGIC
# MAGIC - **Negative values:** Physically impossible - energy can't flow backward in this context. Any negative values indicate calculation bugs.
# MAGIC
# MAGIC **What we see here:** Does the distribution reflect realistic charging behavior? The shape should tell a story about how people actually use these charging stations.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Box plot to identify outliers
fig = px.box(
    gold_pandas, 
    y=["total_time", "total_energy"],
    title="Box Plot: Identifying Outliers in Gold Metrics"
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **How to interpret this box plot:**
# MAGIC
# MAGIC A box plot summarizes the distribution in five key numbers and highlights outliers:
# MAGIC
# MAGIC - **The box:** Shows the interquartile range (IQR) - the middle 50% of values. The line inside is the median (50th percentile).
# MAGIC - **The whiskers:** Extend to the furthest points within 1.5 × IQR from the box edges.
# MAGIC - **Individual points:** Values beyond the whiskers are potential outliers.
# MAGIC
# MAGIC **What to look for:**
# MAGIC
# MAGIC - **Symmetry:** Is the median centered in the box? Asymmetry indicates skewed data (common and often acceptable).
# MAGIC - **Outliers:** A few outliers are normal in real data. Many outliers, or outliers that are extremely far from the box, warrant investigation.
# MAGIC - **Box size:** A very narrow box means most values are tightly clustered. A wide box indicates high variability.
# MAGIC
# MAGIC **Comparing the two metrics:** Do `total_time` and `total_energy` show similar outlier patterns? If one has many outliers and the other doesn't, the outliers might indicate recording errors rather than genuine extreme sessions.
# MAGIC
# MAGIC **Is this correct?** Box plots don't lie about the data - but you need domain knowledge to decide if the outliers are *data quality problems* or *legitimate edge cases* (e.g., a commercial fleet vehicle that charges longer than typical consumer cars).

# COMMAND ----------

# Scatter plot: time vs energy relationship
fig = px.scatter(
    gold_pandas, 
    x="total_time", 
    y="total_energy",
    title="Charging Time vs Energy Dispensed",
    labels={"total_time": "Total Time (hours)", "total_energy": "Total Energy (kWh)"}
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **How to interpret this scatter plot:**
# MAGIC
# MAGIC A scatter plot reveals the *relationship* between two variables. For charging time vs. energy dispensed, we expect a clear physical relationship:
# MAGIC
# MAGIC **Expected pattern:**
# MAGIC - **Positive correlation:** More time charging should generally mean more energy delivered. Points should trend upward from left to right.
# MAGIC - **Upper bound:** There's a physical limit - charging rate (kW) × time (hours) = maximum energy. Points shouldn't exceed what's physically possible for the charger type.
# MAGIC - **Variation is normal:** Different vehicles charge at different rates, and batteries slow down charging as they fill up. We expect scatter, not a perfect line.
# MAGIC
# MAGIC **Warning signs:**
# MAGIC - **No correlation:** If points are randomly scattered with no trend, something is wrong - either time and energy aren't being measured from the same sessions, or one measurement is broken.
# MAGIC - **Impossible points:** High energy in very short time (exceeding charger capacity) or very long time with zero energy (session didn't charge anything).
# MAGIC - **Horizontal/vertical clusters:** A cluster of points at exactly the same time or energy value might indicate default values or data truncation.
# MAGIC
# MAGIC **Is this correct?** The relationship should make physical sense. If you see a 1-hour session that delivered 200 kWh, that's impossible with current technology (most fast chargers max out around 150-350 kW). Such points indicate data errors.
# MAGIC
# MAGIC **Domain knowledge matters:** Understanding that Level 2 chargers deliver ~7-19 kW while DC fast chargers deliver 50-350 kW helps you validate whether the time/energy combinations are plausible.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Data Contracts
# MAGIC
# MAGIC A **data contract** is an explicit agreement about what consumers can expect from your data. It should specify:
# MAGIC - Schema (column names, types)
# MAGIC - Null policies (which columns can be null)
# MAGIC - Value constraints (ranges, allowed values)
# MAGIC - Freshness guarantees
# MAGIC
# MAGIC **Why this matters:** Without explicit contracts, data producers and consumers operate on implicit assumptions - and those assumptions inevitably diverge. The producer thinks it's fine to occasionally have null transaction IDs (rare edge cases). The consumer's dashboard crashes on nulls because they assumed IDs are always present. Data contracts make expectations explicit and testable. They're the data equivalent of API documentation: they define what's guaranteed, so consumers know what they can rely on and producers know what they must maintain.

# COMMAND ----------

def validate_gold_output_contract(df: DataFrame) -> dict:
    """
    Validate Gold layer output against its data contract.
    
    Contract:
    - Required columns: charge_point_id, transaction_id, meter_start, meter_stop,
                       start_timestamp, stop_timestamp, total_time, total_energy
    - No nulls in: charge_point_id, transaction_id, total_time, total_energy
    - total_time must be >= 0
    - total_energy must be >= 0
    """
    results = {"passed": [], "failed": []}
    
    # Check 1: Required columns
    required_columns = [
        "charge_point_id", "transaction_id", "meter_start", "meter_stop",
        "start_timestamp", "stop_timestamp", "total_time", "total_energy"
    ]
    missing = set(required_columns) - set(df.columns)
    if not missing:
        results["passed"].append("All required columns present")
    else:
        results["failed"].append(f"Missing columns: {missing}")
    
    # Check 2: No nulls in critical columns
    no_null_columns = ["charge_point_id", "transaction_id", "total_time", "total_energy"]
    for col_name in no_null_columns:
        if col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count == 0:
                results["passed"].append(f"No nulls in {col_name}")
            else:
                results["failed"].append(f"{null_count} nulls found in {col_name}")
    
    # Check 3: total_time >= 0
    if "total_time" in df.columns:
        negative_time = df.filter(col("total_time") < 0).count()
        if negative_time == 0:
            results["passed"].append("total_time >= 0")
        else:
            results["failed"].append(f"{negative_time} rows with negative total_time")
    
    # Check 4: total_energy >= 0
    if "total_energy" in df.columns:
        negative_energy = df.filter(col("total_energy") < 0).count()
        if negative_energy == 0:
            results["passed"].append("total_energy >= 0")
        else:
            results["failed"].append(f"{negative_energy} rows with negative total_energy")
    
    # Print results
    print("Gold Output Contract Validation:")
    print("=" * 50)
    
    print(f"\nPASSED ({len(results['passed'])}):")
    for check in results["passed"]:
        print(f"  [OK] {check}")
    
    if results["failed"]:
        print(f"\nFAILED ({len(results['failed'])}):")
        for check in results["failed"]:
            print(f"  [XX] {check}")
    
    overall = len(results["failed"]) == 0
    print(f"\nOverall Result: {'PASS' if overall else 'FAIL'}")
    
    return results

# Validate Gold output
contract_results = validate_gold_output_contract(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Replicability and Idempotency
# MAGIC
# MAGIC Good data pipelines should be:
# MAGIC - **Deterministic**: Same inputs always produce same outputs
# MAGIC - **Idempotent**: Running the pipeline multiple times produces the same result
# MAGIC
# MAGIC This is important for debugging and reprocessing.
# MAGIC
# MAGIC **Why this matters:** When something goes wrong, you need to reproduce the problem. If your pipeline uses `random()` or `now()` or processes data in non-deterministic order, you can't reliably recreate yesterday's bug. Idempotency is equally critical: if a pipeline fails halfway through, you want to restart it without creating duplicates or inconsistent state. Pipelines that aren't idempotent require careful manual intervention to recover from failures - and that manual intervention often introduces new errors.

# COMMAND ----------

def check_idempotency(df1: DataFrame, df2: DataFrame, key_columns: list, tolerance: float = 0.2) -> bool:
    """
    Check if two DataFrames (from repeated pipeline runs) produce similar results.
    
    Pipelines can have varying data volumes across runs, but should stay within
    an expected frame. Default tolerance is +/-20%.
    """
    count1 = df1.count()
    count2 = df2.count()
    
    # Allow row counts to vary within tolerance (e.g., +/-20%)
    lower_bound = int(count1 * (1 - tolerance))
    upper_bound = int(count1 * (1 + tolerance))
    
    if not (lower_bound <= count2 <= upper_bound):
        print(f"Idempotency check FAILED: Row counts differ beyond {tolerance*100:.0f}% tolerance")
        print(f"  Run 1: {count1} rows")
        print(f"  Run 2: {count2} rows (expected {lower_bound}-{upper_bound})")
        return False
    
    # Check for matching keys
    keys1 = set([tuple(row) for row in df1.select(key_columns).collect()])
    keys2 = set([tuple(row) for row in df2.select(key_columns).collect()])
    
    if keys1 == keys2:
        print(f"Idempotency check PASSED: {count1} vs {count2} rows (within {tolerance*100:.0f}% tolerance), matching keys")
        return True
    else:
        diff = keys1.symmetric_difference(keys2)
        print(f"Idempotency check FAILED: {len(diff)} key differences")
        return False

# Demo: Compare Gold to itself (should pass)
# In practice, compare outputs from different pipeline runs
check_idempotency(gold_df, gold_df, ["charge_point_id", "transaction_id"], tolerance=0.2)

# COMMAND ----------

# Check for duplicate primary keys (another form of integrity check)
def check_primary_key_uniqueness(df: DataFrame, key_columns: list, name: str = "DataFrame") -> bool:
    """Check that the specified columns form a unique primary key."""
    total_rows = df.count()
    distinct_keys = df.select(key_columns).distinct().count()
    
    is_unique = total_rows == distinct_keys
    
    print(f"Primary key uniqueness check for {name}:")
    print(f"  Key columns:    {key_columns}")
    print(f"  Total rows:     {total_rows}")
    print(f"  Distinct keys:  {distinct_keys}")
    print(f"  Duplicates:     {total_rows - distinct_keys}")
    print(f"  Result: {'PASS' if is_unique else 'FAIL'}")
    
    return is_unique

check_primary_key_uniqueness(gold_df, ["charge_point_id", "transaction_id"], "Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Verification Summary
# MAGIC
# MAGIC | Check | Automation Level | When to Apply |
# MAGIC |-------|------------------|---------------|
# MAGIC | Random sampling | Low - requires human inspection | Periodically, on changes |
# MAGIC | Edge case inspection | Medium - automated selection, human review | Every release |
# MAGIC | Distribution visualization | Medium - automated charts, human interpretation | Periodically |
# MAGIC | Data contract validation | High - fully automated | Every pipeline run |
# MAGIC | Idempotency check | High - fully automated | After reprocessing |
# MAGIC
# MAGIC **Key Takeaway**: Combine automated contract checks with periodic human inspection of samples and visualizations.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: Framework Summary
# MAGIC
# MAGIC ### Quality Gates at Each Stage
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │   SOURCE    │     │   BRONZE    │     │   SILVER    │     │    GOLD     │
# MAGIC │   (Raw)     │────▶│  (Ingested) │────▶│ (Cleaned)   │────▶│ (Aggregated)│
# MAGIC └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
# MAGIC                           │                   │                   │
# MAGIC                           ▼                   ▼                   ▼
# MAGIC                     ┌───────────┐       ┌───────────┐       ┌───────────┐
# MAGIC                     │  INPUT    │       │TRANSFORM  │       │  OUTPUT   │
# MAGIC                     │VALIDATION │       │  CHECKS   │       │VERIFICATION
# MAGIC                     ├───────────┤       ├───────────┤       ├───────────┤
# MAGIC                     │• Schema   │       │• Row count│       │• Sampling │
# MAGIC                     │• Volume   │       │• Join QA  │       │• Viz      │
# MAGIC                     │• Nulls    │       │• Filter QA│       │• Contract │
# MAGIC                     │• Domain   │       │• Calc QA  │       │• Idempot. │
# MAGIC                     └───────────┘       └───────────┘       └───────────┘
# MAGIC ```

# COMMAND ----------

def run_quality_checks(df: DataFrame, layer_name: str, config: dict) -> dict:
    """
    Run a comprehensive set of quality checks on a DataFrame.
    
    Config example:
    {
        "expected_columns": ["col1", "col2"],
        "min_rows": 100,
        "max_rows": 10000,
        "no_null_columns": ["col1"],
        "domain_checks": {"col1": ["val1", "val2"]},
        "range_checks": {"col2": (0, 100)}
    }
    """
    print(f"\n{'='*60}")
    print(f"Quality Checks for {layer_name}")
    print(f"{'='*60}")
    
    results = {"passed": 0, "failed": 0, "details": []}
    
    # Schema check
    if "expected_columns" in config:
        missing = set(config["expected_columns"]) - set(df.columns)
        if not missing:
            results["passed"] += 1
            results["details"].append(("Schema", "PASS", "All expected columns present"))
        else:
            results["failed"] += 1
            results["details"].append(("Schema", "FAIL", f"Missing: {missing}"))
    
    # Row count check
    if "min_rows" in config and "max_rows" in config:
        count = df.count()
        if config["min_rows"] <= count <= config["max_rows"]:
            results["passed"] += 1
            results["details"].append(("Row count", "PASS", f"{count} rows (expected {config['min_rows']}-{config['max_rows']})"))
        else:
            results["failed"] += 1
            results["details"].append(("Row count", "FAIL", f"{count} rows (expected {config['min_rows']}-{config['max_rows']})"))
    
    # Null checks
    if "no_null_columns" in config:
        for col_name in config["no_null_columns"]:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count == 0:
                    results["passed"] += 1
                    results["details"].append((f"Nulls ({col_name})", "PASS", "No nulls"))
                else:
                    results["failed"] += 1
                    results["details"].append((f"Nulls ({col_name})", "FAIL", f"{null_count} nulls found"))
    
    # Range checks
    if "range_checks" in config:
        for col_name, (min_val, max_val) in config["range_checks"].items():
            if col_name in df.columns:
                out_of_range = df.filter((col(col_name) < min_val) | (col(col_name) > max_val)).count()
                if out_of_range == 0:
                    results["passed"] += 1
                    results["details"].append((f"Range ({col_name})", "PASS", f"All values in [{min_val}, {max_val}]"))
                else:
                    results["failed"] += 1
                    results["details"].append((f"Range ({col_name})", "FAIL", f"{out_of_range} values out of range"))
    
    # Print results
    print(f"\n{'Check':<25} {'Status':<8} {'Details'}")
    print("-" * 60)
    for check, status, details in results["details"]:
        print(f"{check:<25} {status:<8} {details}")
    
    print(f"\nSummary: {results['passed']} passed, {results['failed']} failed")
    
    return results

# COMMAND ----------

# Run comprehensive checks on Gold layer
gold_config = {
    "expected_columns": ["charge_point_id", "transaction_id", "total_time", "total_energy"],
    "min_rows": 1,
    "max_rows": 10000,
    "no_null_columns": ["charge_point_id", "transaction_id", "total_time", "total_energy"],
    "range_checks": {
        "total_time": (0, 24),
        "total_energy": (0, 100)
    }
}

gold_results = run_quality_checks(gold_df, "Gold Layer", gold_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tools Ecosystem
# MAGIC
# MAGIC While we've built custom functions in this notebook, there are mature tools for production data quality:
# MAGIC
# MAGIC | Tool | Description | Best For |
# MAGIC |------|-------------|----------|
# MAGIC | **[Great Expectations](https://greatexpectations.io/)** | Python library for data validation | Comprehensive rule-based testing |
# MAGIC | **[Delta Lake Constraints](https://docs.delta.io/latest/delta-constraints.html)** | Built-in constraints for Delta tables | Schema enforcement, check constraints |
# MAGIC | **[dbt Tests](https://docs.getdbt.com/docs/build/data-tests)** | Testing framework integrated with dbt | SQL-based transformation testing |
# MAGIC | **[Apache Deequ](https://github.com/awslabs/deequ)** | Spark-native data quality library | Large-scale Spark pipelines |
# MAGIC | **[Soda](https://www.soda.io/)** | Data observability platform | Monitoring and alerting |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Quality at every stage**: Don't wait until the end to check data quality. Validate at Bronze (input), Silver (transformation), and Gold (output).
# MAGIC
# MAGIC 2. **Automate what you can**: Schema validation, null counts, row count tracking, and join verification can all be automated and run on every pipeline execution.
# MAGIC
# MAGIC 3. **Human judgment for context**: Thresholds, acceptable ranges, and business rule exceptions require human input. Automated checks flag issues; humans decide what's acceptable.
# MAGIC
# MAGIC 4. **Document contracts**: Make your quality expectations explicit through data contracts. This helps both pipeline maintainers and data consumers.
# MAGIC
# MAGIC 5. **Visualize periodically**: Charts reveal patterns and anomalies that tabular checks miss. Build dashboards for ongoing monitoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflection Questions
# MAGIC
# MAGIC 1. What data quality checks would you add to the Bronze layer ingestion in this EV charging pipeline?
# MAGIC
# MAGIC 2. How would you handle a situation where 5% of transactions have negative `total_energy` values? Is this a data bug or a valid edge case?
# MAGIC
# MAGIC 3. If you were building a production pipeline, which quality checks would you make blocking (stop the pipeline) vs alerting (notify but continue)?
# MAGIC
# MAGIC 4. How would you communicate data quality issues to downstream consumers (analysts, dashboards, ML models)?

# COMMAND ----------

# Clean up
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Navigation
# MAGIC
# MAGIC | Previous Topic | Next Module |
# MAGIC |----------------|-------------|
# MAGIC | <a href="$./5.5 Exercise - Gold" target="_self">Exercise - Gold</a> | <a href="$../Module 06 - Visualisation & Accessibility/6.0 Table of Contents" target="_self">Visualisation & Accessibility</a> |

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>