import googleapiclient.discovery
from googleapiclient.discovery import build
import google_auth_oauthlib.flow
import googleapiclient.errors
from google.cloud import bigquery
from dotenv import load_dotenv
import os
import json


load_dotenv()


# -*- coding: utf-8 -*-

# Sample Python code for youtube.comments.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/code-samples#python


def get_videos(search, publishedAfter=None, publishedBefore=None):
    scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]

    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = os.getenv('YT_API_KEY')

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY)

    request = youtube.search().list(
        part="snippet",
        maxResults=25,
        q=search
    )
    response = request.execute()

    return response


def get_comments(video_id):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = os.getenv('YT_API_KEY')

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY)

    request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_id
    )
    response = request.execute()

    return response


def get_search_mockup():
    with open("./mockup-responses/search.json", 'r', encoding='utf-8') as file:
        response = json.load(file)
        return response
    

def get_comments_mockup():
    '''
    return mockup data from youtube api.
    '''
    with open("./mockup-responses/commentThreads.json", 'r', encoding='utf-8') as file:
        response = json.load(file)
        return response


def search_video_ids(query, published_before=None, published_after=None):
    YT_API_KEY = os.getenv('YT_API_KEY')
    youtube = build("youtube", "v3", developerKey=YT_API_KEY)

    search_params = {
        "q": query,
        "part": "id",
        "maxResults" :5,
        "type" : "video"
    }

    # Add optional date filters if provided
    if published_after:
        search_params["publishedAfter"] = published_after
    if published_before:
        search_params["publishedBefore"] = published_before

    request = youtube.search().list(**search_params)
    response = request.execute()

    return response


def create_external_table():
    client = bigquery.Client()
    
    table_id = "your_project.election_bronze.yt_comments_raw"
    
    external_config = bigquery.ExternalConfig("JSON")
    external_config.source_uris = ["gs://your-bucket-name/youtube_comments/*.json"]
    external_config.autodetect = True
    
    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config
    
    # This creates or updates the table link to GCS
    client.create_table(table, exists_ok=True)
    print(f"External table {table_id} is live and pointing to GCS.")