from datetime import datetime

import json
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from googleapiclient.discovery import build
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="youtube_extraction",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["youtube", "extraction"],
)
def youtube_extraction():

    @task
    def extract_channel():
        api_key = Variable.get("API_KEY")
        channel_handle = Variable.get("CHANNEL_HANDLE")

        youtube = build(
            "youtube",
            "v3",
            developerKey=api_key
        )

        response = youtube.search().list(
            part="snippet",
            q=channel_handle,
            type="channel",
            maxResults=1
        ).execute()

        channel_id = response["items"][0]["snippet"]["channelId"]

        print(f"Channel Handle: {channel_handle}")
        print(f"Channel ID: {channel_id}")

        return channel_id
    
    @task
    def get_playlist(channel_id):
        api_key = Variable.get("API_KEY")

        youtube = build(
            "youtube",
            "v3",
            developerKey=api_key
        )

        response = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()

        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        print(f"Channel ID: {channel_id}")
        print(f"Uploads Playlist ID: {playlist_id}")

        return playlist_id
    
    @task
    def get_video_ids(playlist_id):
        api_key = Variable.get("API_KEY")

        youtube = build(
            "youtube",
            "v3",
            developerKey=api_key
        )

        video_ids = []
        page_token = None

        while True:
            response = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token
            ).execute()

            for item in response["items"]:
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            page_token = response.get("nextPageToken")

            if not page_token:
                break

        print(f"Number of videos: {len(video_ids)}")
        print(f"Video IDs: {video_ids}")

        return video_ids
    
    @task
    def get_video_details(video_ids):
        api_key = Variable.get("API_KEY")

        youtube = build(
            "youtube",
            "v3",
            developerKey=api_key
        )

        videos = []

        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i + 50]

            response = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=",".join(batch)
            ).execute()

            for item in response["items"]:
                video = {
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "published_at": item["snippet"]["publishedAt"],
                    "duration": item["contentDetails"]["duration"],
                    "views": item["statistics"].get("viewCount"),
                    "likes": item["statistics"].get("likeCount"),
                    "comments": item["statistics"].get("commentCount"),
                }

                videos.append(video)

        print(f"Number of video details: {len(videos)}")

        return videos
    
    @task
    def save_json(videos):
        data_dir = Path("/opt/airflow/data")
        data_dir.mkdir(parents=True, exist_ok=True)

        file_name = f"YT_data_{datetime.now().strftime('%Y-%m-%d')}.json"
        file_path = data_dir / file_name

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(videos, file, ensure_ascii=False, indent=4)

        print(f"JSON file saved: {file_path}")

        return str(file_path)

    channel_id = extract_channel()
    playlist_id = get_playlist(channel_id)
    video_ids = get_video_ids(playlist_id)
    videos = get_video_details(video_ids)
    json_file = save_json(videos)

    trigger_warehouse = TriggerDagRunOperator(
        task_id="trigger_warehouse",
        trigger_dag_id="youtube_warehouse",
    )
    json_file >> trigger_warehouse


youtube_extraction()