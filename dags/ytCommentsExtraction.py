from airflow import DAG
from airflow.sdk import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from googleapiclient.discovery import build
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime
import json
import os

load_dotenv()

BUCKET_NAME = "th-pm-election-2026-project"
YT_API_KEY = os.getenv('YT_API_KEY')
searches = ["เลือกตั้ง69", "พรรคประชาชน", "พรรคเพื่อไทย", "พรรคภูมิใจไทย", "พรรคประชาธิปัตย์"]
GCP_CONN_ID = "GCPBucket"

@task
def find_and_save_videos(query, published_before=None, published_after=None, **context):
    youtube = build("youtube", "v3", developerKey=YT_API_KEY)

    search_params = {
        "q": query,
        "part": "snippet, id",
        "maxResults" : 5,
        "type" : "video"
    }

    if published_after:
        search_params["publishedAfter"] = published_after
    if published_before:
        search_params["publishedBefore"] = published_before

    request = youtube.search().list(**search_params)
    response = request.execute()

    query_slug = query.replace(" ", "_").lower()
    file_path = f"bronze/youtube/searches/dt={context['ds']}/{query_slug}.json"
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    hook.upload(
        bucket_name = BUCKET_NAME,
        object_name = file_path,
        data = json.dumps(response)
    )
    return {
        "query": query,
        "video_ids": [item['id']['videoId'] for item in response.get('items', [])]
    }

@task
def fetch_and_save_comments(search_data, **context):
    query = search_data['query']
    video_ids = search_data['video_ids']
    query_slug = query.replace(" ", "_").lower()
    youtube = build("youtube", "v3", developerKey=YT_API_KEY)
    all_comments = []
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    for id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet, replies",
                videoId=id,
                maxResults=50
            )
            response = request.execute()

            file_path = f"bronze/youtube/comments/dt={context['ds']}/q={query_slug}/v={id}.json"
            hook.upload(
                bucket_name=BUCKET_NAME,
                object_name=file_path,
                data=json.dumps(response)
            )
        except Exception as e:
            print(f"skipping {id}")

    return all_comments


@task
def update_bq_external_table(table_id, source_uri_prefix):
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
    project_id = hook.project_id
    dataset_id = "election_2026_bronze"
    
    external_config = {
        "sourceUris": [f"gs://{BUCKET_NAME}/{source_uri_prefix}*"],
        "sourceFormat": "NEWLINE_DELIMITED_JSON",
        "autodetect": True,
        "hivePartitioningOptions": {
            "mode": "AUTO",
            "sourceUriPrefix": f"gs://{BUCKET_NAME}/{source_uri_prefix}",
            "requirePartitionFilter": False
        }
    }

    table_resource = {
        "tableReference": {
            "projectId": project_id,
            "datasetId": dataset_id,
            "tableId": table_id,
        },
        "externalDataConfiguration": external_config,
    }

    hook.run_table_upsert(dataset_id=dataset_id, table_resource=table_resource)
    print(f"Table {dataset_id}.{table_id} updated to point to {source_uri_prefix}")
    

with DAG(
    dag_id='youtube_comments_with_context_manager',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    video_id_batches = find_and_save_videos.expand(query=searches)
    comment_id_batches = fetch_and_save_comments.expand(search_data=video_id_batches)
    
    video_update = update_bq_external_table(
        table_id="raw_youtube_searches",
        source_uri_prefix="bronze/youtube/searches/"
    )
    
    comment_update = update_bq_external_table(
        table_id="raw_youtube_comments",
        source_uri_prefix="bronze/youtube/comments/"
    )
    
    comment_id_batches >> [video_update, comment_update]