from datetime import datetime
import json
from pathlib import Path

from airflow.decorators import dag, task


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

    read_json()


youtube_warehouse()