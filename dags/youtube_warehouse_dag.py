from datetime import datetime
import json
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="youtube_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["youtube", "warehouse"],
)
def youtube_warehouse():

    @task
    def read_json():
        data_dir = Path("/opt/airflow/data")

        json_files = sorted(data_dir.glob("YT_data_*.json"))

        if not json_files:
            raise FileNotFoundError("No YouTube JSON file found.")

        latest_file = json_files[-1]

        with open(latest_file, "r", encoding="utf-8") as file:
            videos = json.load(file)

        print(f"Reading file: {latest_file}")
        print(f"Number of videos: {len(videos)}")

        return videos
    @task
    def update_staging(videos):

        hook = PostgresHook(
            postgres_conn_id="postgres_db_yt_elt"
        )

        connection = hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS staging;

            CREATE TABLE IF NOT EXISTS staging.youtube_videos (
                video_id VARCHAR(20) PRIMARY KEY,
                title TEXT,
                published_at TIMESTAMP,
                duration VARCHAR(50),
                views BIGINT,
                likes BIGINT,
                comments BIGINT
            );
        """)

        # Insert & Update

        for video in videos:

            cursor.execute("""
                INSERT INTO staging.youtube_videos (
                    video_id,
                    title,
                    published_at,
                    duration,
                    views,
                    likes,
                    comments
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)

                ON CONFLICT (video_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    duration = EXCLUDED.duration,
                    views = EXCLUDED.views,
                    likes = EXCLUDED.likes,
                    comments = EXCLUDED.comments;
            """, (
                video["video_id"],
                video["title"],
                video["published_at"],
                video["duration"],
                video["views"],
                video["likes"],
                video["comments"]
            ))

        # Delete

        source_video_ids = {
            video["video_id"]
            for video in videos
        }

        cursor.execute("""
            SELECT video_id
            FROM staging.youtube_videos;
        """)

        staging_video_ids = {
            row[0]
            for row in cursor.fetchall()
        }

        videos_to_delete = staging_video_ids - source_video_ids

        for video_id in videos_to_delete:
            cursor.execute("""
                DELETE FROM staging.youtube_videos
                WHERE video_id = %s;
            """, (video_id,))

        connection.commit()

        print(f"Source videos: {len(source_video_ids)}")
        print(f"Deleted videos: {len(videos_to_delete)}")

        cursor.close()
        connection.close()

    videos = read_json()
    update_staging(videos)


youtube_warehouse()