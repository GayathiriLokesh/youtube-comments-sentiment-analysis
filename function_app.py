import azure.functions as func
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData
import json
import time
from datetime import datetime

# YouTube API setup
API_KEY = 'API_KEY'  # Replace with your YouTube API key
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Azure Blob Storage setup for storing fetch progress and last fetched time
blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=stgyoutubecomments;AccountKey=ACCOUNT_KEY;EndpointSuffix=core.windows.net")
container_name = 'youtube-comments-metadata'
progress_blob_name = 'fetch_progress.json'  # Stores progress of fetching (video_id, nextPageToken)
last_fetch_blob_name = 'last_fetched.json'  # Stores the last fetch time

progress_blob_client = blob_service_client.get_blob_client(container=container_name, blob=progress_blob_name)
last_fetch_blob_client = blob_service_client.get_blob_client(container=container_name, blob=last_fetch_blob_name)

# Azure Event Hub setup for sending comments
event_hub_conn_str = "Endpoint=sb://youtube-comments-namespace.servicebus.windows.net/;SharedAccessKeyName=SendPolicy;SharedAccessKey=ACCESS_KEY;EntityPath=youtube-comment-stream"
event_hub_name = "youtube-comment-stream"
producer = EventHubProducerClient.from_connection_string(conn_str=event_hub_conn_str, eventhub_name=event_hub_name)


# Function to get last fetch time from Blob
def get_last_fetch_time_from_blob():
    try:
        last_fetched_data = last_fetch_blob_client.download_blob().readall()
        last_fetched_json = json.loads(last_fetched_data)
        return last_fetched_json['last_fetched']
    except Exception:
        logging.info("No previous fetch time found, defaulting to YTD.")
        return datetime(datetime.utcnow().year, 1, 1).isoformat() + "Z"


# Function to update the last fetched time in Blob
def update_last_fetch_time_in_blob():
    last_fetched = datetime.utcnow().isoformat() + "Z"
    last_fetch_blob_client.upload_blob(json.dumps({"last_fetched": last_fetched}), overwrite=True)
    logging.info(f"Updated last fetched time to {last_fetched}")


# Function to get last fetch progress (nextPageToken, video_id)
def get_last_fetch_progress():
    try:
        progress_data = progress_blob_client.download_blob().readall()
        progress_json = json.loads(progress_data)
        return progress_json
    except Exception:
        return {}


# Function to update fetch progress (nextPageToken, video_id)
def update_fetch_progress(video_id, nextPageToken):
    current_progress = get_last_fetch_progress()
    current_progress[video_id] = {"nextPageToken": nextPageToken}
    progress_blob_client.upload_blob(json.dumps(current_progress), overwrite=True)
    logging.info(f"Updated fetch progress for video {video_id} with nextPageToken {nextPageToken}")


# Function to clear fetch progress when all comments are fetched
def clear_fetch_progress(video_id):
    current_progress = get_last_fetch_progress()
    if video_id in current_progress:
        del current_progress[video_id]
    progress_blob_client.upload_blob(json.dumps(current_progress), overwrite=True)
    logging.info(f"Cleared fetch progress for video {video_id}")


# Function to fetch comments for a video, handling pagination with nextPageToken
def fetch_comments(video_id, nextPageToken=None):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100,
            pageToken=nextPageToken
        )
        response = request.execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comment_id = item['snippet']['topLevelComment']['id']
            comment['id'] = comment_id
            comments.append(comment)
    except HttpError as e:
        logging.error(f"An error occurred while fetching comments: {e}")
        return comments, None

    return comments, response.get('nextPageToken')


# Function to send comments to Azure Event Hub
def send_to_event_hub(comments):
    event_hub_conn_str = "Endpoint=sb://youtube-comments-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ACCESS_KEY;EntityPath=youtube-comment-stream"
    event_hub_name = "youtube-comment-stream"
    producer = EventHubProducerClient.from_connection_string(conn_str=event_hub_conn_str, eventhub_name=event_hub_name)
    
    # Create an empty batch
    event_data_batch = producer.create_batch()

    for comment in comments:
        try:
            # Try to add the comment to the batch
            event_data_batch.add(EventData(json.dumps(comment)))  
        except ValueError:
            # If adding a comment exceeds the batch size, send the current batch and start a new one
            producer.send_batch(event_data_batch)
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(comment)))

    # Send any remaining comments in the last batch
    if len(event_data_batch) > 0:
        producer.send_batch(event_data_batch)

    producer.close()



# Function to fetch recent videos based on last fetched time
def search_recent_azure_videos(max_results=5, last_fetch_time=None):
    request = youtube.search().list(
        q='Azure',
        part='snippet',
        type='video',
        maxResults=max_results,
        publishedAfter=last_fetch_time
    )
    response = request.execute()
    video_ids = [item['id']['videoId'] for item in response['items']]
    return video_ids


# Main function to fetch and process new comments
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
@app.route(route="fetchYoutubeComments", methods=['POST'])
def fetchYoutubeComments(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing request to fetch YouTube comments.')

    # Step 1: Fetch last fetch time and fetch progress
    last_fetch_time = get_last_fetch_time_from_blob()
    fetch_progress = get_last_fetch_progress()  # Get progress of video ID and nextPageToken

    logging.info(f"Fetching data starting from: {last_fetch_time}")

    # Step 2: Fetch new videos published after last fetch time
    video_ids = search_recent_azure_videos(max_results=20, last_fetch_time=last_fetch_time)
    if not video_ids:
        logging.info("No new videos found.")
        return func.HttpResponse("No new videos found", status_code=200)

    logging.info(f"Found {len(video_ids)} videos.")
    all_comments = []

    # Step 3: Fetch comments for each video
    for video_id in video_ids:
        logging.info(f"Fetching comments for video ID: {video_id}")
        nextPageToken = fetch_progress.get(video_id, {}).get('nextPageToken')

        while True:
            comments, nextPageToken = fetch_comments(video_id, nextPageToken)
            if not comments:
                logging.info(f"No more comments for video ID: {video_id}.")
                break

            all_comments.extend(comments)

            if nextPageToken:
                update_fetch_progress(video_id, nextPageToken)
                logging.info(f"More comments to fetch for video {video_id}. Will resume on next run.")
                break

            clear_fetch_progress(video_id)
            logging.info(f"All comments for video {video_id} have been fetched.")
            break

    # Step 4: Send comments to Event Hub
    if all_comments:
        logging.info(f"Sending {len(all_comments)} comments to Event Hub.")
        send_to_event_hub(all_comments)

    # Step 5: Only update last fetch time if all comments for all videos are fetched
    if not any(fetch_progress.values()):
        update_last_fetch_time_in_blob()
        logging.info("Updated last fetch time after fetching all comments.")
    else:
        logging.info("Not all comments were fetched, will resume in the next run.")

    return func.HttpResponse(f"Fetched and processed comments from {len(video_ids)} videos", status_code=200)
