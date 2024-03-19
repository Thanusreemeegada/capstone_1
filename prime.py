import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, inspect
import os
from dotenv import load_dotenv

load_dotenv()
# Set up API credentials
DEVELOPER_KEY = os.getenv('API_KEY')
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# Create a YouTube API client
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

# Connect to SQL database
db_username = 'root'
db_password = 'Thanu#2003'
db_host = '127.0.0.1'
db_name = 'guvi'
engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:3306/{db_name}')


if st.button("Clear DB"):
    # Create a MetaData instance
    metadata = MetaData()

    # Get the inspector for the database
    inspector = inspect(engine)

    # Get a list of all table names
    table_names = inspector.get_table_names()

    # Drop each table
    for table_name in table_names:
        table = Table(table_name, metadata, autoload_with=engine)
        table.drop(engine)

# Function to extract channel data from YouTube API
def get_channel_data(channel_id):
   request = youtube.channels().list(
       part="snippet,contentDetails,statistics",
       id=channel_id
   )
   response = request.execute()
   
   # Extract channel details
   channel_details = {
       "Channel_Name": response["items"][0]["snippet"]["title"],
       "Channel_Id": channel_id,
       "Subscription_Count": response["items"][0]["statistics"]["subscriberCount"],
       "Channel_Views": response["items"][0]["statistics"]["viewCount"],
       "Channel_Description": response["items"][0]["snippet"]["description"],
       "Playlist_Id": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
   }
   
   return channel_details

def get_video_data(playlist_id):
   videos = []
   next_page_token = None
   
   while True:
       request = youtube.playlistItems().list(
           part="snippet,contentDetails",
           playlistId=playlist_id,
           maxResults=20,
           pageToken=next_page_token
       )
       response = request.execute()
       
       for item in response["items"]:
           video_id = item["contentDetails"]["videoId"]
           video_request = youtube.videos().list(
               part="snippet,contentDetails,statistics",
               id=video_id
           )
           video_response = video_request.execute()
           video_details = {
               "Video_Id": video_id,
               "Playlist_id": playlist_id,
               "Video_Name": video_response["items"][0]["snippet"]["title"],
               "Video_Description": video_response["items"][0]["snippet"]["description"],
               "PublishedAt": video_response["items"][0]["snippet"]["publishedAt"],
               "View_Count": video_response["items"][0]["statistics"]["viewCount"],
               "Favorite_Count": video_response["items"][0]["statistics"]["favoriteCount"],
               "Comment_Count": video_response["items"][0]["statistics"]["commentCount"],
               "Duration": video_response["items"][0]["contentDetails"]["duration"],
               "Thumbnail": video_response["items"][0]["snippet"]["thumbnails"]["default"]["url"],
               "Caption_Status": video_response["items"][0]["contentDetails"]["caption"]
           }

           videos.append(video_details)
       
       next_page_token = response.get("nextPageToken")
       if not next_page_token:
           break
   
   return videos

def get_comment_data(video_id):
    request = youtube.commentThreads().list(
            part="snippet,replies",
            maxResults=5,
            videoId=video_id
    )
    response = request.execute()
     
    comments = [] 
    for item in response["items"]:
        comments.append({
            "comment_id":item["id"],
            "video_id": item["snippet"]["topLevelComment"]["snippet"]["videoId"],
            "text": item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
            "date": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
            "author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],  
        })
    return comments 


###############################################################################################

# Streamlit App
def main():
    st.title("YouTube Data Harvesting and Warehousing")
    # Get channel ID from user
    channel_id = st.text_input("Enter YouTube Channel ID")
    if st.button("Get Channel Data"):
        try:
            channel_ids = channel_id.split(",")
            for i in range(len(channel_ids)):
                channel_ids[i] = channel_ids[i].strip()

                # Get channel data
                channel_data = get_channel_data(channel_ids[i])

                # Display channel data
                st.subheader(f"Channel Name: {channel_data['Channel_Name']}")
                st.text(f"Channel ID: {channel_data['Channel_Id']}")
                st.text(f"Subscription Count: {channel_data['Subscription_Count']}")
                st.text(f"Channel Views: {channel_data['Channel_Views']}")
                st.text(f"Channel Description: {channel_data['Channel_Description']}")
                st.divider()

        except HttpError as e:
           st.error(f"An error occurred: {e.reason}")

    if st.button("Store Data"):
        channel_ids = channel_id.split(",")
        for i in range(len(channel_ids)):
            # Get channel data
            channel_data = get_channel_data(channel_ids[i])
            # Get videos data
            videos = get_video_data(channel_data["Playlist_Id"])
                    
            # Get Comments data
            for video in videos:
                comments = get_comment_data(video["Video_Id"])
                if len(comments) > 0:
                    comments_df = pd.DataFrame(comments)
                    comments_df.to_sql("comments",engine, if_exists="append", index=False)

            # Store data in SQL database
            channels_df = pd.DataFrame([channel_data])
            videos_df = pd.DataFrame(videos)
            channels_df.to_sql("channels",engine, if_exists="append", index=False)
            videos_df.to_sql("videos",engine, if_exists="append", index=False)
        st.write("Data is Stored in the Database")
        st.divider()

    # Query SQL database
    query = st.text_area("Enter SQL query")
    if st.button("Execute Query"):
        try:
            result = pd.read_sql_query(query, engine)
            st.dataframe(result)
        except Exception as e:
           st.error(f"An error occurred: {e}")

if __name__ == "__main__":
   main()