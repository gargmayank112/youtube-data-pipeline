import os
import requests
import pandas as pd
import snowflake.connector

from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
api_key = os.getenv("YOUTUBE_API_KEY")



channel_handle="MrBeast"
base_url = "https://youtube.googleapis.com/youtube/v3"

def get_playlist_id():
    
    try:
        
        url = f"{base_url}/channels"
        
        params = {
            "part": "contentDetails",
            "forHandle": channel_handle,
            "key": api_key
            }

        response = requests.get(url,params=params,timeout=30)

        # print("Status code:", response.status_code)
        # print("API response:", response.text)
        response.raise_for_status()

        data=response.json()

        channel_playlistId=data["items"][0]["contentDetails"]['relatedPlaylists']["uploads"]

        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e



def get_video_ids(play_list_id,api_key, max_results=50):
    video_ids=[]
    page_token=None
    url = f"{base_url}/playlistItems"
   
    try:
        
        while True:
            params = {
                "part": "contentDetails",
                "maxResults": max_results,
                "playlistId":play_list_id,
                "key": api_key
                }
            
      
            if page_token:
                params["pageToken"] = page_token
            
            response=requests.get(url,params=params, timeout=30)
            response.raise_for_status()
            
            data=response.json()
            
            for item in data.get("items",[]):
                video_id=item["contentDetails"]["videoId"]
                video_ids.append(video_id)
                
            page_token =data.get("nextPageToken")
        
            if not page_token :
                break
                
        return video_ids

    except requests.exceptions.RequestException as e:
        raise e
        

def extract_video_data(video_ids):
    extracted_data=[]
    
    def batch_list(video_id_list,batch_size):
        for video_id in range(0,len(video_id_list),batch_size):
            yield video_id_list [video_id : video_id + batch_size]
            
    try:
        for batch in batch_list(video_ids,batch_size=50):
            videos_ids_str = ",".join(batch)
            
            url = f"{base_url}/videos"

            params = {
                "part": "contentDetails,snippet,status,statistics",
                "id": videos_ids_str,
                "key": api_key
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data=response.json()
            
            
            for item in data.get('items',[]):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data={
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount",None),
                    "likeCount": statistics.get("likeCount",None),
                    "commentCount": statistics.get("commentCount",None)
                }
        
                extracted_data.append(video_data)
        return extracted_data
            
            
    except requests.exceptions.RequestException as e:
        raise e





def save_data(extracted_data):
    df = pd.DataFrame (extracted_data)
    df.to_csv(r"C:\Users\gargm\OneDrive\Desktop\Python\Youtube\test.csv",index=False)

def read_data():
    csv_path = r"C:\Users\gargm\OneDrive\Desktop\Python\Youtube\test.csv"
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.upper()
    return df

# ------------------------------connect to snowflake------------------------------------------------------------------------------

def write_to_snowflake(df):

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    try:
        cursor.execute("""
        create table if not exists videos(
        video_id varchar,
        title varchar,
        publishedAT varchar,
        duration varchar,
        viewCount varchar,
        likeCount varchar,
        commentCount varchar)
        """)
        
        print ("Table created sucessfully!")


        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name="VIDEOS",
            database="YOUTUBE_DATA",
            schema="PUBLIC"
        )

        print("Success:", success)
        print("Chunks loaded:", num_chunks)
        print("Rows loaded:", num_rows)

    
    finally:
        cursor.close()
        conn.close()

        
if __name__== "__main__":
    play_list_id = get_playlist_id()
    video_ids = get_video_ids(play_list_id,api_key, max_results=50)
    extracted_data = extract_video_data(video_ids)
    save_data(extracted_data)
    df = read_data()
    write_to_snowflake(df)


