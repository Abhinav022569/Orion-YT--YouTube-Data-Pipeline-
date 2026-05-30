# Orion-YT: Automated YouTube Data Pipeline

A cloud-native, event-driven ETL pipeline that ingests YouTube trending video statistics and category reference data across 10 regions, transforms it using a medallion architecture (Bronze → Silver → Gold), enforces automated data quality gates, and produces analytics-ready datasets — all orchestrated by AWS Step Functions.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
  - [Bronze Layer (Raw Data)](#bronze-layer-raw-data)
  - [Silver Layer (Cleansed Data)](#silver-layer-cleansed-data)
  - [Data Quality Gate](#data-quality-gate)
  - [Gold Layer (Business Aggregations)](#gold-layer-business-aggregations)
- [Gold Layer Output Tables](#gold-layer-output-tables)
- [Prerequisites](#prerequisites)
- [AWS Infrastructure Setup](#aws-infrastructure-setup)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Supported Regions](#supported-regions)
- [Data Sources](#data-sources)

---

## Overview

This pipeline automates the end-to-end collection, sanitization, and processing of YouTube trending video metrics. It eliminates manual workflows (such as local bash script uploads of static files) by directly integrating with the live YouTube Data API v3, applying robust deduplication, checking governance rules via an analytical data quality gate, and generating three high-performance business analytics tables:

- **Trending Analytics** — Daily trending video summaries per region detailing view density, counts, and calculated audience interaction ratios.
- **Channel Analytics** — Multi-regional performance logs per creator tracking historical records, total reach, and computed ranking positions.
- **Category Analytics** — Time-series tracking mapping category-level engagement variations and continuous daily view-share metrics.

The entire workload handles **10 global regions** and executes on a serverless schedule using Amazon EventBridge.

---

## Architecture

The system utilizes a decoupled serverless data lake layout patterned around the **Medallion Architecture**:
Data Sources     Bronze Layer        Silver Layer         Quality Gate          Gold Layer         Analytics
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────┐    ┌──────────────┐    ┌──────────┐
│ YouTube  │    │              │    │              │    │            │    │  trending_   │    │          │
│ API v3   │───>│  Raw JSON    │───>│  Cleansed    │───>│  DQ Lambda │───>│  analytics   │───>│  Athena  │
│          │    │  (S3)        │    │  Parquet     │    │  Validates │    │              │    │          │
├──────────┤    │              │    │  (S3)        │    │  row count │    │  channel_    │    ├──────────┤
│ Historical│   │  Raw CSV     │    │              │    │  nulls     │    │  analytics   │    │  Quick-  │
│ Kaggle   │───>│  (S3)        │    │  Reference   │    │  schema    │    │              │    │  Sight   │
│ Data     │    │              │    │  Parquet     │    │  freshness │    │  category_   │    │          │
└──────────┘    └──────────────┘    └──────────────┘    └────────────┘    │  analytics   │    └──────────┘
│           └──────────────┘
fail │
▼
┌────────────┐
│  SNS Alert │
└────────────┘
**Orchestration** is completely managed by AWS Step Functions, coordinating workflows synchronously, providing modular catch-exception states, executing parallel staging, and issuing automated notification triggers.

---

## Tech Stack

| Component | Technology |
|---|---|
| **Compute** | AWS Lambda, AWS Glue (PySpark Serverless Spark) |
| **Storage** | Amazon S3 (Snappy Compressed Apache Parquet format) |
| **Orchestration** | AWS Step Functions State Machines |
| **Scheduling** | Amazon EventBridge |
| **Metadata Management** | AWS Glue Data Catalog |
| **Query Engine** | Amazon Athena |
| **Alerting & Signaling** | Amazon SNS |
| **Monitoring** | Amazon CloudWatch Logs |
| **Access Security** | AWS IAM (Least-Privilege Resource Policies) |
| **Languages** | Python 3, PySpark, SQL |
| **Core Libraries** | Pandas, AWS Wrangler, Boto3 |

---

## Project Structure
Orion-YT/
│
├── lambdas/
│   ├── youtube_api_ingestion/          # Ingestion Layer
│   │   └── lambda_function.py         # Pulls live metrics & categories from Google YouTube API
│   └── json_to_parquet/               # Reference Layer Transformation
│       └── lambda_function.py         # flattens nested structural categories to Parquet via AWS Wrangler
│
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py # PySpark ETL: Raw files → Deduplicated Silver Statistics
│   └── silver_to_gold_analytics.py    # PySpark Aggregator: Decorares reference data and builds Gold tables
│
├── data_quality/
│   └── dq_lambda.py                   # Serverless Quality Gate running sampled SQL tests via Athena
│
├── step_functions/
│   └── pipeline_orchestration.json    # Complete Step Functions State Machine engine schema
│
├── iam_permission/
│   ├── s3_lambda_yt_pipeline_policy.json # Dedicated IAM boundaries for S3, Glue, and Athena Lambdas
│   ├── yt-data-pipeline-glue-access.json  # Dedicated IAM boundaries for Glue Spark storage access
│   └── yt-data-pipeline-sfn-access.json  # Orchestration execution policies for Step Functions
│
└── scripts/
├── aws_copy.sh                    # Helper script to load Kaggle historical seed tables to S3
└── information.md                 # System resource catalog references and bucket identifiers
---

## Data Flow

### Bronze Layer (Raw Data)
The `youtube_api_ingestion` Lambda function triggers on an automated schedule to hit the YouTube Data API v3, pulling 50 records matching the `mostPopular` charts alongside localized video category schema definitions. 

Data is written cleanly into the Bronze bucket (`yt-data-pipeline-bronze-ap-south-1-demo`) using Hive partitioning constraints:
- **Raw Statistics:** `youtube/raw_statistics/region={region}/date={yyyy-mm-dd}/hour={hh}/`
- **Category Reference Mapping:** `youtube/raw_statistics_reference_data/region={region}/date={yyyy-mm-dd}/`

*Historical datasets can be backfilled inside these exact prefix structures using the provided `scripts/aws_copy.sh` script.*

### Silver Layer (Cleansed Data)
Once raw data ingestion completes, Step Functions forks data cleanup workloads into two parallel paths:

1. **Statistics Cleaning (`yt-data-pipeline-bronze-to-silver-dev` Glue Job):** Ingests raw JSON data or Kaggle CSV data from the catalog database (`yt_pipeline_bronze_dev`). It handles flattening operations, standardizes text regions, casts parameters (`views`, `likes`, `dislikes`, `comment_count`) into `LongType`, parses text timestamps, extracts derived analytical dimensions (`like_ratio` and `engagement_rate`), and runs deduplication via window partitioning (`Window.partitionBy("video_id", "region", "trending_date_parsed").orderBy(F.col("_processed_at").desc())`).
2. **Reference Mapping (`yt-data-pipeline-json-to-parquet-dev` Lambda):** Reads mixed-type nested reference JSON items, unpacks attributes using `pandas.json_normalize`, asserts structure, applies category deduplication, and writes structural columns along with ingestion data lineage keys (`_ingestion_timestamp`, `_source_file`).

All outputs are written into `s3://yt-data-pipeline-silver-ap-south-1-demo/` formatted as optimized Snappy-Parquet structures.

### Data Quality Gate
Before compiling final presentation tables, the Step Function invokes the `dq_lambda.py` data validation checkpoint. The module runs serverless query tasks via **Amazon Athena** against a 10,000-row sample to validate the following rules:

| Check Rule | Applied Logic Metric Constraints |
|---|---|
| **Row Count** | Asserts whether tables contain active baseline row limits (`>= 10` total rows). |
| **Null Verification** | Inspects core structural columns (`video_id`, `views`, etc.) to ensure null metrics are `<= 5.0%`. |
| **Schema Match** | Verifies that all required logical processing target columns exist in the catalog schema. |
| **Range Checks** | Validates metrics consistency (catches negative metrics or impossible view counts `> 50B`). |
| **Data Freshness** | Validates that processed timestamps are inside an active business window (`< 48` hours old). |

*If any data quality validation rule fails, `quality_passed` is evaluated as `false`, causing the state machine to instantly halt downstream pipelines and broadcast the detailed error payloads directly to the **Amazon SNS** topic.*

### Gold Layer (Business Aggregations)
Upon clear validation passing, the `yt-data-pipeline-silver-to-gold-dev` Spark Glue job triggers. It broadcasts the reference data lookup structure to join the human-readable category strings back to the core data and compiles three presentation-layer tables.

---

## Gold Layer Output Tables

### `trending_analytics`
Provides continuous daily aggregate insight summaries categorized per tracking country and snapshot window.
- **S3 Prefix Location:** `s3://yt-data-pipeline-gold-ap-south-1-demo/youtube/trending_analytics/`
- **Partition Layout:** `region`

| Column | Data Type | Description |
|---|---|---|
| `region` | string | Lowercase standardized region code identifier |
| `trending_date_parsed` | date | The exact parsed calendar snapshot date |
| `total_videos` | long | Total volume count of trending videos on that date |
| `total_views` | long | Cumulative view metrics matching the tracking date |
| `total_likes` | long | Cumulative audience likes recorded |
| `total_dislikes` | long | Cumulative audience dislikes recorded |
| `total_comments` | long | Cumulative user comments submitted |
| `avg_views_per_video` | double | Calculated average view density per video entry |
| `avg_like_ratio` | double | Mean computed interaction metrics over views |
| `avg_engagement_rate` | double | Combined audience engagement index percentage |
| `max_views` | long | Peak view metrics reached by a video on that day |
| `unique_channels` | long | Unique count of distinct active creators on the chart |
| `unique_categories` | long | Unique count of active category groupings participating |
| `_aggregated_at` | timestamp | Pipeline processing computation timestamp |

### `channel_analytics`
Compiles structural performance metrics across creators, complete with rank tracking inside individual countries.
- **S3 Prefix Location:** `s3://yt-data-pipeline-gold-ap-south-1-demo/youtube/channel_analytics/`
- **Partition Layout:** `region`

| Column | Data Type | Description |
|---|---|---|
| `channel_title` | string | Name identifier of the YouTube Channel |
| `region` | string | Lowercase standardized country tracking layout |
| `total_videos` | long | Count of independent distinct videos that hit trending charts |
| `total_views` | long | Aggregate views captured across trending records |
| `total_likes` | long | Aggregate likes achieved across trending records |
| `total_comments` | long | Aggregate comments achieved across trending records |
| `avg_views_per_video` | double | Mean view distribution per upload |
| `avg_engagement_rate` | double | Mean engagement rate achieved by creator |
| `peak_views` | long | Absolute maximum view count record captured by channel |
| `times_trending` | long | Total accumulated volume days channel held a trending position |
| `first_trending` | date | Earliest calendar date tracking channel entry on chart |
| `last_trending` | date | Most recent calendar date tracking channel entry on chart |
| `categories` | string | Comma-separated list string mapping all categories published in |
| `rank_in_region` | int | Calculated performance ranking inside the country using Spark Window rows |
| `_aggregated_at` | timestamp | Pipeline processing computation timestamp |

### `category_analytics`
Evaluates market share changes and consumer taste distributions over time across category groupings.
- **S3 Prefix Location:** `s3://yt-data-pipeline-gold-ap-south-1-demo/youtube/category_analytics/`
- **Partition Layout:** `region`

| Column | Data Type | Description |
|---|---|---|
| `category_name` | string | Human-readable category title string (Fallback to 'Unknown') |
| `category_id` | long | Category lookup identifier key |
| `region` | string | Lowercase standardized country tracking layout |
| `trending_date_parsed` | date | Current parsed snapshot calendar tracking date |
| `video_count` | long | Number of active trending uploads inside the category segment |
| `total_views` | long | Combined tracking views generated by the category segment |
| `total_likes` | long | Combined tracking likes generated by the category segment |
| `total_comments` | long | Combined tracking user comment metrics generated |
| `avg_engagement_rate` | double | Mean category-wide engagement performance score |
| `unique_channels` | long | Unique tracking count of creators publishing within this vertical |
| `view_share_pct` | double | Window calculated ratio of regional view capture percentage |
| `_aggregated_at` | timestamp | Pipeline processing computation timestamp |

---

## Prerequisites

- **AWS Management Account** with deployment access policies across S3, Lambda, Glue, Athena, SNS, and Step Functions.
- **Google Cloud Platform API Key** with the `YouTube Data API v3` service enabled.
- **AWS CLI Core Engine** installed and initialized locally.
- **Python 3.9+** execution environment.

---

## AWS Infrastructure Setup

Provision your data lake storage layout using the AWS CLI:
```bash
aws s3 mb s3://yt-data-pipeline-bronze-ap-south-1-demo
aws s3 mb s3://yt-data-pipeline-silver-ap-south-1-demo
aws s3 mb s3://yt-data-pipeline-gold-ap-south-1-demo
aws s3 mb s3://yt-data-pipeline-script-ap-south-1-demo

Initialize your metadata storage environments within the Glue Data Catalog:
aws glue create-database --database-input '{"Name": "yt_pipeline_bronze_dev"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_silver_dev"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_gold_dev"}'

Configure your automated tracking alert topic endpoint:
aws sns create-topic --name yt-data-pipeline-alerts-dev
aws sns subscribe \
  --topic-arn arn:aws:sns:ap-south-1:700951986601:yt-data-pipeline-alerts-dev \
  --protocol email \
  --notification-endpoint secure_admin_alerts@yourdomain.com

Configuration
Core Environment Variable Matrices
Live Ingestion Lambda (yt-data-pipeline-youtube-ingestion-dev)
YOUTUBE_API_KEY: Google API developer console entry key.

S3_BUCKET_BRONZE: yt-data-pipeline-bronze-ap-south-1-demo

YOUTUBE_REGIONS: Default target array configuration (US,GB,CA,DE,FR,IN,JP,KR,MX,RU).

SNS_ALERT_TOPIC_ARN: arn:aws:sns:ap-south-1:700951986601:yt-data-pipeline-alerts-dev

Reference Mapping Lambda (yt-data-pipeline-json-to-parquet-dev)
S3_BUCKET_SILVER: yt-data-pipeline-silver-ap-south-1-demo

GLUE_DB_SILVER: yt_pipeline_silver_dev

GLUE_TABLE_REFERENCE: clean_reference_data

Quality Control Lambda (yt-data-pipeline-data-quality-dev)
SNS_ALERT_TOPIC_ARN: Topic ARN location to push structured JSON alerts on failure thresholds.

DQ_MIN_ROW_COUNT: Evaluates baseline volume. (Defaults to 10).

DQ_MAX_NULL_PERCENT: Bound null allowance check constraint. (Defaults to 5.0).

Deployment
1. Upload PySpark Logic Drivers to S3
aws s3 cp glue_jobs/bronze_to_silver_statistics.py s3://yt-data-pipeline-script-ap-south-1-demo/glue_jobs/
aws s3 cp glue_jobs/silver_to_gold_analytics.py s3://yt-data-pipeline-script-ap-south-1-demo/glue_jobs/

2. Deploy Lambda Applications
Package and upload code bundles for each function:
cd lambdas/youtube_api_ingestion
zip -r function_package.zip lambda_function.py
aws lambda create-function \
  --function-name yt-data-pipeline-youtube-ingestion-dev \
  --runtime python3.9 \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function_package.zip \
  --role arn:aws:iam::700951986601:role/your-lambda-execution-role-name \
  --timeout 300 \
  --memory-size 256
# Repeat structural deployment commands for json_to_parquet and data_quality lambdas

3. Initialize Serverless Glue Jobs
aws glue create-job \
  --name yt-data-pipeline-bronze-to-silver-dev \
  --role arn:aws:iam::700951986601:role/your-glue-service-role \
  --command '{"Name":"glueetl","ScriptLocation":"s3://yt-data-pipeline-script-ap-south-1-demo/glue_jobs/bronze_to_silver_statistics.py"}' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X

4. Provision AWS Step Functions Workflow State Machine
aws stepfunctions create-state-machine \
  --name yt-data-pipeline-orchestration \
  --definition file://step_functions/pipeline_orchestration.json \
  --role-arn arn:aws:iam::700951986601:role/your-step-functions-orchestration-role

Running the Pipeline
Scheduled Execution
Configure automated recurring pipeline sweeps at 6-hour boundaries using Amazon EventBridge:
aws events put-rule \
  --name yt-data-pipeline-six-hour-trigger \
  --schedule-expression "rate(6 hours)"

aws events put-targets \
  --rule yt-data-pipeline-six-hour-trigger \
  --targets '[{"Id":"1","Arn":"arn:aws:states:ap-south-1:700951986601:stateMachine:yt-data-pipeline-orchestration","RoleArn":"arn:aws:iam::700951986601:role/your-eventbridge-role"}]'

Manual Trigger
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-south-1:700951986601:stateMachine:yt-data-pipeline-orchestration

Detailed Pipeline Phase Transitions
1. IngestFromYouTubeAPI ──► Fetches live data configurations → Stores inside S3 Bronze
2. WaitForS3Consistency ──► Delays run for 10 seconds to solve eventual consistency conditions
3. ProcessInParallel    ──► Forks Concurrent Branches:
                            ├── Lambda: json_to_parquet (Category structures cleanup)
                            └── Glue Job: yt-data-pipeline-bronze-to-silver-dev (Core processing)
4. RunDataQualityChecks ──► Invokes dq_lambda.py tests via serverless Athena queries
5. EvaluateDataQuality  ──► Choice logic branch. Aborts on failure or advances downstream
6. RunSilverToGoldGlueJob─► Evaluates analytical window calculations into Gold Layer database
7. NotifySuccess        ──► Dispatches automated validation summaries to administration emails

Monitoring and Alerting
Step Functions Dashboard: Visual state trees displaying step execution latency, retries, and failure states.

Amazon CloudWatch Logs: Captures granular stdout logs from Lambda and PySpark workers.

Amazon Athena Validations: Allows ad-hoc analytical checks directly across production parquet tables:

-- Query Example: Analyze top trending channels across the Indian Region by view count
SELECT channel_title, total_videos, total_views, rank_in_region
FROM yt_pipeline_gold_dev.channel_analytics
WHERE region = 'in'
ORDER BY total_views DESC
LIMIT 10;

Supported RegionsThe pipeline continuously processes, scales, and manages structured logs mapping 10 global regional components:Regional CodeCountryRegional CodeCountryusUnited StatesinIndiagbUnited KingdomjpJapancaCanadakrSouth KoreadeGermanymxMexicofrFranceruRussia

Data Sources
YouTube Data API v3: Real-time source for trending metrics, metadata fields, and region category catalog data models.

Kaggle YouTube Historical Logs: Historical seed templates used to verify data consistency and perform large-scale backfill evaluations.
