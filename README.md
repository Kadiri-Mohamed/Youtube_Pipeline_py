# YouTube Data Pipeline with Python, PostgreSQL, Docker & Airflow

Complete data engineering pipeline for extracting YouTube channel data from the YouTube Data API v3, storing raw data as JSON, transforming it, loading it into PostgreSQL, and orchestrating the workflow with Apache Airflow and Docker.

## Architecture

```text
YouTube Data API v3
        |
        v
DAG 1 - Extraction
        |
        v
Raw JSON
        |
        v
DAG 2 - Warehouse
        |
        v
PostgreSQL - Staging
        |
        v
Transformation
        |
        v
PostgreSQL - Core
        |
        v
Power BI (Bonus)
```

## Objectives

- Consume the YouTube Data API v3.
- Identify a YouTube channel and its uploads playlist.
- Retrieve all video IDs using pagination.
- Retrieve video details in batches.
- Store extracted data as dated JSON files.
- Load data into PostgreSQL Staging.
- Transform data into an analysis-ready format.
- Load transformed data into PostgreSQL Core.
- Synchronize data using INSERT, UPDATE and DELETE.
- Orchestrate the pipeline with Apache Airflow.
- Containerize the environment with Docker Compose.
- Prepare the Core layer for Power BI analysis.

## Technologies

| Technology | Usage |
|---|---|
| Python | Extraction and transformation |
| YouTube Data API v3 | Source |
| JSON | Raw data storage |
| PostgreSQL | Data warehouse |
| Apache Airflow | Orchestration |
| Docker | Containerization |
| Docker Compose | Multi-container environment |
| Redis | Celery broker |
| CeleryExecutor | Airflow task execution |
| Power BI | Visualization bonus |
| Git / GitHub | Version control |

## Project Structure

```text
youtube-data-pipeline/
│
├── dags/
│   ├── youtube_extraction_dag.py
│   ├── youtube_warehouse_dag.py
│   └── first_pipeline.py
│
├── data/
│   └── YT_data_YYYY-MM-DD.json
│
├── tests/
│   ├── test_extraction.py
│   ├── test_transformation.py
│   └── ...
│
├── .env
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Pipeline Workflow

### DAG 1 - Extraction

DAG ID:

```text
youtube_extraction
```

Schedule:

```text
@daily
```

Workflow:

```text
extract_channel
      ↓
get_playlist
      ↓
get_video_ids
      ↓
get_video_details
      ↓
save_json
      ↓
trigger_warehouse
```

### 1. Extract Channel

The pipeline reads the YouTube API key and channel handle from Airflow Variables.

```text
CHANNEL_HANDLE
      ↓
YouTube API
      ↓
Channel ID
```

### 2. Retrieve Uploads Playlist

The channel ID is used to retrieve the playlist containing the channel's uploaded videos.

```text
Channel ID
      ↓
channels.list()
      ↓
Uploads Playlist ID
```

### 3. Retrieve Video IDs

The pipeline uses `maxResults=50` and follows `nextPageToken` until all videos are retrieved.

```text
Page 1 → 50 videos
Page 2 → 50 videos
Page 3 → 50 videos
...
Last page
```

This allows the pipeline to process channels with more than 50 videos.

### 4. Retrieve Video Details

Video IDs are grouped into batches of up to 50 IDs per API request.

The extracted fields are:

| Field | Description |
|---|---|
| `video_id` | YouTube video ID |
| `title` | Video title |
| `published_at` | Publication date |
| `duration` | Original ISO 8601 duration |
| `views` | Number of views |
| `likes` | Number of likes |
| `comments` | Number of comments |

### 5. Save Raw JSON

The extracted data is saved using:

```text
YT_data_YYYY-MM-DD.json
```

Example:

```text
YT_data_2026-09-18.json
```

The raw JSON preserves the extracted source data before transformation.

---

# DAG 2 - Warehouse Update

DAG ID:

```text
youtube_warehouse
```

Workflow:

```text
read_json
      ↓
update_staging
      ↓
transform
      ↓
update_core
```

## read_json

The task searches for:

```text
YT_data_*.json
```

and loads the latest JSON file.

## update_staging

The Staging schema and table are created if they do not already exist.

```sql
CREATE SCHEMA IF NOT EXISTS staging;
```

Table:

```text
staging.youtube_videos
```

Columns:

```text
video_id
title
published_at
duration
views
likes
comments
```

Staging is used to receive and prepare the extracted source data before transformation.

## transform

The transformation prepares data for the Core layer.

### Duration conversion

YouTube uses ISO 8601 durations.

Examples:

```text
PT45S      → 45
PT5M20S    → 320
PT1H10M30S → 4230
```

The result is stored as:

```text
duration_seconds
```

### Date conversion

An ISO 8601 publication date such as:

```text
2026-09-18T10:30:00Z
```

is converted into a Python `datetime`.

### Numeric conversion

The following fields are converted to integers:

```text
views
likes
comments
```

Missing values are handled using:

```text
0
```

## update_core

The Core schema and table are created if necessary.

```sql
CREATE SCHEMA IF NOT EXISTS core;
```

Table:

```text
core.youtube_videos
```

Columns:

```text
video_id
title
published_at
duration_seconds
views
likes
comments
```

Core contains transformed and analysis-ready data.

---

# Data Synchronization

The warehouse is synchronized with the latest source data.

## INSERT

New videos are inserted when their `video_id` does not already exist.

## UPDATE

Existing videos are updated when their values change.

The implementation uses PostgreSQL:

```sql
ON CONFLICT (video_id)
DO UPDATE SET ...
```

This is particularly useful for changing metrics such as views, likes and comments.

## DELETE

Videos that exist in the database but are no longer present in the latest source are deleted.

Conceptually:

```text
Database IDs
      -
Source IDs
      =
Videos to DELETE
```

Synchronization is applied to both:

```text
staging.youtube_videos
core.youtube_videos
```

---

# PostgreSQL Data Warehouse

The project uses two schemas:

```text
PostgreSQL
│
├── staging
│   └── youtube_videos
│
└── core
    └── youtube_videos
```

## Staging

Purpose:

- Receive extracted data.
- Preserve the source representation.
- Prepare data before transformation.
- Synchronize source records.

## Core

Purpose:

- Store transformed data.
- Use appropriate data types.
- Store analysis-ready values.
- Provide a clean source for analytics.

---

# Docker Architecture

Docker Compose runs the services required by the project.

```text
+--------------------+
| Airflow Webserver  |
+--------------------+

+--------------------+
| Airflow Scheduler  |
+--------------------+

+--------------------+
| Airflow Worker     |
+--------------------+

+--------------------+
| PostgreSQL         |
+--------------------+

+--------------------+
| Redis              |
+--------------------+
```

### Airflow Webserver

Provides the Airflow web interface.

Default port:

```text
8080
```

### Airflow Scheduler

Schedules DAGs and manages task execution.

### Airflow Worker

Executes tasks with the CeleryExecutor.

### PostgreSQL

Stores Airflow metadata and the YouTube warehouse.

The project warehouse database is:

```text
youtube
```

### Redis

Acts as the Celery message broker.

---

# Environment Variables

Sensitive configuration is stored in `.env`.

Example:

```env
API_KEY=YOUR_YOUTUBE_API_KEY
CHANNEL_HANDLE=@YOUR_CHANNEL
```

Database credentials are also configured through environment variables used by Docker Compose.

Never commit real credentials to GitHub.

Recommended `.gitignore`:

```gitignore
.env
__pycache__/
*.pyc
.venv/
.ipynb_checkpoints/
```

---

# Installation

## Prerequisites

Install:

- Docker Desktop
- Git
- YouTube Data API v3 access

Make sure Docker Desktop is running.

## Clone the Repository

```bash
git clone <your-repository-url>
cd youtube-data-pipeline
```

## Configure Environment Variables

Create a `.env` file:

```env
API_KEY=YOUR_YOUTUBE_API_KEY
CHANNEL_HANDLE=@YOUR_CHANNEL
```

Use your own API key and channel handle.

## Build the Environment

```bash
docker compose build
```

## Start the Services

```bash
docker compose up -d
```

Check the containers:

```bash
docker compose ps
```

---

# Running the Project

## Open Airflow

Open:

```text
http://localhost:8080
```

## Run DAG 1

Open:

```text
youtube_extraction
```

Trigger it manually or wait for its daily schedule.

Expected workflow:

```text
extract_channel
      ↓
get_playlist
      ↓
get_video_ids
      ↓
get_video_details
      ↓
save_json
      ↓
trigger_warehouse
```

The last task triggers:

```text
youtube_warehouse
```

## Run DAG 2

The warehouse DAG executes:

```text
read_json
      ↓
update_staging
      ↓
transform
      ↓
update_core
```

After execution, the transformed data should be available in:

```text
core.youtube_videos
```

---

# Testing

The project should test the main pipeline components.

## API Tests

Verify:

- API client creation.
- Channel extraction.
- Playlist retrieval.
- Video detail retrieval.

## Pagination Tests

Verify that multiple pages are processed correctly.

Example:

```text
50 + 50 + 50 + ...
```

## JSON Tests

Verify:

- JSON file creation.
- Valid JSON format.
- Required fields.
- Correct number of records.

## Staging Tests

Verify:

- INSERT.
- UPDATE.
- DELETE.
- Table structure.

## Transformation Tests

Verify:

- Duration conversion.
- Date conversion.
- Numeric conversion.
- Missing values.

## Core Tests

Verify:

- INSERT.
- UPDATE.
- DELETE.
- Correct transformed values.

## Airflow Tests

Verify:

- DAG parsing.
- Task dependencies.
- DAG triggering.

## Docker Tests

Verify:

- All containers start.
- Airflow connects to PostgreSQL.
- Airflow connects to Redis.
- DAGs are visible in Airflow.

---

# Power BI

Power BI can be connected to the PostgreSQL Core layer.

Recommended table:

```text
core.youtube_videos
```

Possible dashboard metrics:

- Total videos.
- Total views.
- Total likes.
- Total comments.
- Average video duration.
- Views by video.
- Likes by video.
- Comments by video.
- Videos published over time.

Example dashboard:

```text
+------------------------------------------------+
|              YouTube Analytics                 |
+------------------------------------------------+
| Total Videos | Total Views | Likes | Comments  |
+------------------------------------------------+
|                                                |
| Views by Video                                 |
|                                                |
+----------------------+-------------------------+
| Videos Over Time    | Engagement Metrics      |
|                     |                         |
+----------------------+-------------------------+
```

Power BI is an optional bonus component.

---

# API Limits and Pagination

The extraction process takes YouTube API limitations into account.

## Pagination

The playlist API is requested with:

```python
maxResults=50
```

The pipeline checks:

```text
nextPageToken
```

and continues until there are no more pages.

## Request Batching

Video details are retrieved in batches of up to 50 IDs:

```text
410 video IDs

Batch 1 → 50
Batch 2 → 50
Batch 3 → 50
...
Batch 9 → 10
```

This avoids making one request per video.

---

# Security

Never commit sensitive information to GitHub.

Do not commit:

```text
.env
API keys
Database passwords
Private credentials
```

If generated files become too large, they can also be excluded from Git:

```gitignore
data/*.json
*.html
```

Only use these rules if those generated files are not intended to be versioned.

---

# Troubleshooting

## Airflow cannot connect to PostgreSQL

Check:

```bash
docker compose ps
```

Make sure PostgreSQL is running and healthy.

Check that the Airflow connection is configured correctly.

## DAG does not appear in Airflow

Check:

- The DAG file is inside the mounted `dags/` directory.
- The Python file has no syntax errors.
- Airflow logs for DAG parsing errors.

## No JSON file found

Make sure the extraction DAG successfully executed:

```text
save_json
```

The JSON file should exist in:

```text
/opt/airflow/data
```

inside the Airflow containers.

## PostgreSQL connection refused

Verify:

- PostgreSQL is running.
- The correct database is configured.
- The Airflow connection ID is correct.
- PostgreSQL is healthy.

The warehouse connection used by the DAG is:

```text
postgres_db_yt_elt
```

---

# Future Improvements

Possible improvements include:

- More robust API error handling.
- Retry policies for temporary API failures.
- Data quality checks.
- Structured logging.
- Incremental extraction.
- Additional analytical tables or views.
- More advanced Power BI dashboards.
- CI tests with GitHub Actions.
- Pipeline monitoring.

These improvements are optional and are not required for the core project.

---

# Author

**Mohamed Kadiri**

Project:

**YouTube Data Pipeline with Python, PostgreSQL, Docker & Airflow**

Date:

**September 2026**
