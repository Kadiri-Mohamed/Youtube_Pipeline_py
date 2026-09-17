from datetime import datetime
import json
from pathlib import Path
import re

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="youtube_warehouse",
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
        return videos
    def duration_to_seconds(duration):
        if not duration:
            return None

        match = re.fullmatch(
            r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?",
            duration
        )

        if not match:
            return None

        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)

        return hours * 3600 + minutes * 60 + seconds
    @task
    def transform(videos):
    
        transformed_videos = []
    
        for video in videos:
        
            transformed_video = {
                "video_id": video["video_id"],
                "title": video["title"],
                "published_at": datetime.fromisoformat(
                    video["published_at"].replace("Z", "+00:00")
                ),
                "duration_seconds": duration_to_seconds(
                    video["duration"]
                ),
                "views": int(video["views"]) if video["views"] is not None else 0,
                "likes": int(video["likes"]) if video["likes"] is not None else 0,
                "comments": int(video["comments"]) if video["comments"] is not None else 0,
            }
    
            transformed_videos.append(transformed_video)
    
        print(f"Transformed videos: {len(transformed_videos)}")
    
        return transformed_videos
    @task
    def update_core(videos):

        hook = PostgresHook(
            postgres_conn_id="postgres_db_yt_elt"
        )

        connection = hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS core;

            CREATE TABLE IF NOT EXISTS core.youtube_videos (
                video_id VARCHAR(20) PRIMARY KEY,
                title TEXT,
                published_at TIMESTAMP,
                duration_seconds INTEGER,
                views BIGINT,
                likes BIGINT,
                comments BIGINT
            );
        """)

        for video in videos:

            cursor.execute("""
                INSERT INTO core.youtube_videos (
                    video_id,
                    title,
                    published_at,
                    duration_seconds,
                    views,
                    likes,
                    comments
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)

                ON CONFLICT (video_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    duration_seconds = EXCLUDED.duration_seconds,
                    views = EXCLUDED.views,
                    likes = EXCLUDED.likes,
                    comments = EXCLUDED.comments;
            """, (
                video["video_id"],
                video["title"],
                video["published_at"],
                video["duration_seconds"],
                video["views"],
                video["likes"],
                video["comments"]
            ))
            
        source_video_ids = {
            video["video_id"]
            for video in videos
        }
        
        cursor.execute("""
            SELECT video_id
            FROM core.youtube_videos;
        """)
        
        core_video_ids = {
            row[0]
            for row in cursor.fetchall()
        }
        
        videos_to_delete = core_video_ids - source_video_ids
        
        for video_id in videos_to_delete:
            cursor.execute("""
                DELETE FROM core.youtube_videos
                WHERE video_id = %s;
            """, (video_id,))

        connection.commit()

        cursor.close()
        connection.close()

        print(f"Core updated with {len(videos)} videos")
    
    
    
    videos = read_json()

    staged_videos = update_staging(videos)

    transformed_videos = transform(staged_videos)

    update_core(transformed_videos)


youtube_warehouse()