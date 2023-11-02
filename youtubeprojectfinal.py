import pandas as pd
import streamlit as st
import mysql.connector
import pymongo
import re
from googleapiclient.discovery import build
from streamlit_option_menu import option_menu
#first set the page layout in streamlit
st.set_page_config(layout='wide')
st.title(':red[YouTube Data Harvesting and Warehousing using MongoDB,SQL and Streamlit]')
api_key = 'AIzaSyAoTSNwL1c-Sg9LtDvkluY7U4g-B6gQWLk' #created using  google developers console to connect with youtube data API
youtube = build('youtube', 'v3', developerKey=api_key)
with st.sidebar:
    selected = option_menu(None, ["Extract and Transform","Store in MongoDB","Migrate to SQL","Data Analysis"])
#data extraction from youtube
def channel_details(channel_id):
#function to fetch channel details
 #request format from youtube data API page reference section and using in all functions
  request = youtube.channels().list(
    id=channel_id,
    part='snippet,statistics,contentDetails'
  )
  response = request.execute()['items'][0]

  channel_data=[{'titleofchannel':response['snippet']['title'],
                'channeldescription':response['snippet']['description'],
                'published_date':response['snippet']['publishedAt'],
                'subscriber_count':response['statistics']['subscriberCount'],
                'video_count':response['statistics']['videoCount'],
                'view_count':response['statistics']['viewCount'],
                'playlist_ids':response['contentDetails']['relatedPlaylists']['uploads']}]
  return channel_data


def playlist(c_id):
#function to get playlist details of a channel using channel id
    playlist_details=[]
    next_page_token = None
    while True:
        request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=c_id,
        maxResults=50
         )
        response = request.execute()

        for i in range (len(response['items'])):
            playlist_info={'title_playlist':response['items'][i]['snippet']['title'],
               'Id':response['items'][i]['id'],
               'itemcount':response['items'][i]['contentDetails']['itemCount']
                }

            playlist_details.append(playlist_info)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_details

def get_video_ids(channel_playlist_id):
#function to get the video_ids of a particular playlist
  video_id = []
  next_page_token = None
  while True:
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=channel_playlist_id,
        maxResults=50,
        pageToken=next_page_token)
    response = request.execute()
    for item in response['items']:
      video_id.append(item['contentDetails']['videoId'])
    next_page_token = response.get('nextPageToken')
    if response.get('nextPageToken') is None:
      break

  return video_id
def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return 0
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def get_video_details(video_ids):
    # Function to get video_details of multiple video_ids
    video_stats = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for i in range(len(response['items'])):
            video_info = {
                "Video_id": response['items'][i]['id'],
                "Video_Name": response['items'][i]['snippet']['title'] if 'title' in response['items'][i]['snippet'] else "Not Available",
                "titleofchannel": response['items'][i]['snippet']['channelTitle'],
                "Video_Description": response['items'][i]['snippet']['description'],
                "Published_date": response['items'][i]['snippet']['publishedAt'],
                "view_count": response['items'][i]['statistics']['viewCount'],
                "like_count": response['items'][i]['statistics']['likeCount'] if 'likeCount' in response['items'][i]['statistics'] else "Not Available",
                "thumbnail": response['items'][i]['snippet']['thumbnails'],
                "commentCount": response['items'][i]['statistics']['commentCount'] if 'commentCount' in response['items'][i]['statistics'] else "Not Available",
                "favouritecount": response['items'][i]['statistics']['favoriteCount'],
                "duration": convert_duration(response['items'][i]['contentDetails']['duration']) #we are converting duration to seconds by calling function
            }

            video_stats.append(video_info)


    return video_stats


def comment_details(vids):
    comments= []
    for i in vids:
        try:
            response =youtube.commentThreads().list(
            part="snippet,replies",
            videoId=i,
            maxResults=100).execute()

            for cmt in response['items']:
                comment_info = dict(Comment_id = cmt['id'],
                        Video_id = cmt['snippet']['videoId'],
                        Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_publisheddate = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                        Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                        Reply_count = cmt['snippet']['totalReplyCount']
                        )
                comments.append(comment_info)
        except:
            pass
    return comments


#streamlit to extract and display
if selected == "Extract and Transform":
    st.header("EXTRACT AND TRANSFORM DATA")

    channel_id= st.text_input("Enter Channel id here")


    if st.button("Extract Data"):
        st.markdown("## Channel Details ")
        ch_info=(channel_details(channel_id)) #calling function passing value
        pl_data=playlist(channel_id) #calling function passing value
        df1=pd.DataFrame(ch_info)
        df2=pd.DataFrame(pl_data)
        print(df1)
        print(df2)
        print("Channel Details Executed.")
        print(" Playlist Details Executed.")
        st.table(ch_info)
        st.markdown("## Playlist Details (Sample Output)")
        st.table(pl_data)

        pl_id=ch_info[0]['playlist_ids']
        video_ids=get_video_ids(pl_id)
        print(video_ids)
        print("video_ids of a chosen playlist executed")
        video_details = get_video_details(video_ids)
        df3=pd.DataFrame(video_details)
        print(df3)
        print("Video Details Executed.")
        st.markdown("## Video Details (Sample Output) ")
        max_rows = 10
        st.table(df3.head(max_rows))
        comment_data=comment_details(video_ids)
        df4 =pd.DataFrame(comment_data)
        print(df4)
        print("Completed comment Details Executed.")
        st.markdown("## Comment Details (Sample Output)")
        max_rows = 10  # Change this value to limit the number of rows displayed
        st.table(df4.head(max_rows))


client=pymongo.MongoClient("mongodb://127.0.0.1:27017")
mydb = client['Youtube_DB']
collection = mydb['Youtube_data']

#function to make every detail to a single dictionary to migrate data to mongodb


def main(channel_id):
    c=channel_details(channel_id)
    p=playlist(channel_id)
    v_id=get_video_ids(c[0]['playlist_ids'])
    v=get_video_details(v_id)
    cm=comment_details(v_id)

    data={'channel details':c,
         'playlist details':p,
         'video_ids':v_id,
         'video details':v,
         'comment details':cm}
    return data
data_stored = False
if selected == "Store in MongoDB":
    st.header("STORE DATA IN MONGODB")

    channel_id= st.text_input("Enter Channel id here")
    stored_channel_ids = set()

  # Set to track stored channel IDs

    if not data_stored:
        if st.button("STORE DATA"):
            # Check if the channel ID is already stored
            if channel_id in stored_channel_ids:
                st.warning("Data for this channel is already stored!")
            else:
                mongo_dic = main(channel_id)
                result = collection.insert_one(mongo_dic)
                if result.acknowledged:
                    data_stored = True
                    stored_channel_ids.add(channel_id)  # Add the channel ID to the set
                    st.success("Data stored successfully!")
                else:
                    st.error(f"An error occurred")
    else:
        st.warning("Data is already stored!")

mydb = mysql.connector.connect(host="localhost", #setting connection with sql using python
                   user="root",
                   password="12345",
                  database="youtube_project")
mycursor = mydb.cursor()


def insert_channel_details_from_mongodb(mycursor):

    mycursor.execute('''CREATE TABLE if not exists channel_details
                 (titleofchannel VARCHAR(100),
                 channeldescription VARCHAR(1000),
                 subscriber_count INT,
                 video_count INT,
                 view_count INT,
                 playlist_id VARCHAR(100)
                 )''')

    query ='''insert into channel_details(titleofchannel,channeldescription,subscriber_count,video_count,view_count,playlist_id) 
                values (%s,%s,%s,%s,%s,%s)'''
    cursor = collection.find().sort([('_id', -1)]).limit(1)
    ch_values = []
    # Iterate over the cursor to access each document
    for item in cursor:
        channel_details = item.get('channel details', []) # Extracting data of channel details from MongoDB

        if channel_details:
            for i in range(min(len(channel_details), 50)): # Iterate over the first 50 channel details or less if there are fewer channels
                channel = channel_details[i]
            # Extract relevant fields from the playlist object
                titleofchannel= channel.get('titleofchannel', None)
                channeldescription= channel.get('channeldescription', None)
                subscriber_count= int(channel.get('subscriber_count', 0))
                video_count= int(channel.get('video_count',0))
                view_count= int(channel.get('view_count',0))
                playlist_ids= channel.get('playlist_ids',None)

                # Create a tuple with the extracted data
                c = (titleofchannel,channeldescription,subscriber_count,video_count,view_count,playlist_ids)
                ch_values.append(c)

                print(c)
        else:
            print("No channel details found in this document")

    mycursor.executemany(query,ch_values)

    mydb.commit()
def insert_playlist_details_from_mongodb(mycursor):

    mycursor.execute('''CREATE TABLE if not exists playlist_details 
                 (title_playlist VARCHAR(100),
                 Id VARCHAR(500),
                 itemcount INT
                  )''')

    query ='''insert into playlist_details(title_playlist,Id,itemcount) values (%s,%s,%s)'''
    cursor = collection.find().sort([('_id', -1)]).limit(1)
    p_values = []
    # Iterate over the cursor to access each document
    for item in cursor:

        # Extracting data of playlist details from MongoDB
        playlist_details = item.get('playlist details', [])

        if playlist_details:
            # Iterate over the first 50 playlist details or less if there are fewer playlist
            for i in range(min(len(playlist_details), 50)):
                playlist = playlist_details[i]

                # Extract relevant fields from the playlist object
                title_playlist = playlist.get('title_playlist', None)
                Id = playlist.get('Id', None)
                itemcount = int(playlist.get('itemcount', 0))


                # Create a tuple with the extracted data
                p = (title_playlist,Id,itemcount)
                p_values.append(p)

                print(p)
    else:
        print("No playlist details found in this document")
    mycursor.executemany(query,p_values)
    mydb.commit()
def insert_video_details_from_mongodb(mycursor):

    mycursor.execute('''CREATE TABLE if not exists video_details
                  (Video_id VARCHAR(500),
                  Video_Name VARCHAR(500),
                  titleofchannel VARCHAR(100),
                   Published_date VARCHAR(500),
                   view_count INT,
                   like_count INT,
                   commentCount INT,
                   favouritecount INT,
                   duration INT)
                   ''')

    query='''insert into video_details(Video_id,Video_Name,titleofchannel,Published_date,view_count,like_count,commentCount,
                                        favouritecount,duration) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    cursor = collection.find().sort([('_id', -1)]).limit(1)
    v_values = []
    # Iterate over the cursor to access each document
    for item in cursor:
        # Extracting data of video details from MongoDB
        video_details = item.get('video details', [])

        if video_details:
            # Iterate over the first 50 video details or less if there are fewer comments
            for i in range(min(len(video_details), 50)):
                video = video_details[i]

                # Extract relevant fields from the video object
                Video_id =video.get('Video_id',None)
                Video_Name= video.get('Video_Name', None)
                titleofchannel=video.get('titleofchannel', None)
                Published_date= video.get('Published_date', None)
                view_count = int(video.get('view_count', 0))if video.get('view_count', 0) != 'Not Available' else 0
                like_count = int(video.get('like_count', 0))if video.get('like_count', 0) != 'Not Available' else 0
                commentCount = int(video.get('commentCount', 0)) if video.get('commentCount', 0) != 'Not Available' else 0
                favouritecount = int(video.get('favouritecount',0))if video.get('favouritecount', 0) != 'Not Available' else 0
                duration = int(video.get('duration', 0))if video.get('duration', 0) != 'Not Available' else 0
                # Create a tuple with the extracted data
                v = (Video_id,Video_Name,titleofchannel,Published_date,view_count,like_count,commentCount,favouritecount,duration)
                v_values.append(v)
                # Print the 'v' tuple for the current video detail
                print(v)
        else:
            print("No video details found in this document")


    mycursor.executemany(query,v_values)
    mydb.commit()


def insert_comment_details_from_mongodb(mycursor):

    mycursor.execute('''CREATE TABLE if not exists comment_details 
                   (Comment_id VARCHAR(200),
                   Video_id VARCHAR(500),
                   Comment_author VARCHAR(200),
                   Comment_publisheddate VARCHAR(100),
                   Like_count INT,
                   Reply_count INT
                   )
                   ''')

    query='''insert into comment_details(Comment_id,Video_id,Comment_author,Comment_publisheddate,like_count,Reply_count)
             values(%s,%s,%s,%s,%s,%s)'''
    cursor = collection.find().sort([('_id', -1)]).limit(1)
    c_values = []
    # Iterate over the cursor to access each document
    for item in cursor:
        # Extracting data of comment details from MongoDB
        comment_details = item.get('comment details', [])

        if comment_details:
            # Iterate over the first 50 comment details or less if there are fewer comments
            for i in range(min(len(comment_details), 50)):
                comment = comment_details[i]

                # Extract relevant fields from the comment object
                comment_id = comment.get('Comment_id', None)
                video_id = comment.get('Video_id', None)
                comment_author = comment.get('Comment_author', None)
                comment_published_date = comment.get('Comment_publisheddate', None)
                like_count = int(comment.get('Like_count', 0))
                reply_count = int(comment.get('Reply_count', 0))

                # Create a tuple with the extracted data
                c = (comment_id, video_id, comment_author, comment_published_date, like_count, reply_count)
                c_values.append(c)

                print(c)
        else:
            print("No comment details found in this document")
    mycursor.executemany(query,c_values)
    mydb.commit()

if selected == "Migrate to SQL":
    st.header("MIGRATE DATA FROM MONGODB TO SQL")
    if st.button("Insert data"):
        data_inserted = True

    # Initialize a flag to track if data insertion was successful
     # Assume success initially

        try:
            # Call your data insertion functions
            insert_channel_details_from_mongodb(mycursor)
            insert_playlist_details_from_mongodb(mycursor)
            insert_video_details_from_mongodb(mycursor)
            insert_comment_details_from_mongodb(mycursor)
        except Exception as e:
            # If an exception is raised during data insertion, set data_inserted to False
            data_inserted = False
            st.error(f"Error during data insertion: {str(e)}")

        if data_inserted:
            st.write("Data successfully migrated")


if selected == "Data Analysis":
    st.header("DATA ANALYSIS USING SQL")
    questions= st.selectbox('Select your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2023?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))
    if questions == "1. What are the names of all the videos and their corresponding channels?":
        mycursor.execute("select Video_Name,titleofchannel from video_details order by titleofchannel;")
        result_1 = mycursor.fetchall()
        result_1=pd.DataFrame(result_1, columns=["Video_Name","titleofchannel"])
        st.dataframe(result_1)
    elif questions =="2. Which channels have the most number of videos, and how many videos do they have?":
        mycursor.execute("select video_count,titleofchannel from channel_details order by video_count desc;")
        result_2 = mycursor.fetchall()
        result_2 = pd.DataFrame(result_2, columns=["video_count","titleofchannel"])
        st.table(result_2)
    elif questions  =="3. What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute("select Video_Name,titleofchannel,view_count from video_details order by view_count desc limit 10;")
        result_3 =mycursor.fetchall()
        result_3 = pd.DataFrame(result_3, columns=["Video_Name","titleofchannel","view_count"])
        st.table(result_3)
    elif questions =="4. How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("select Video_id,Video_name,titleofchannel,commentCount from video_details order by commentCount;")
        result_4 = mycursor.fetchall()
        result_4 = pd.DataFrame(result_4, columns=["Video_id","Video_Name","titleofchannel","commentCount"])
        st.table(result_4)

    elif questions =="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute("select Video_Name,like_count,titleofchannel from video_details order by like_count desc  limit 50;")
        result_5 = mycursor.fetchall()
        result_5 = pd.DataFrame(result_5, columns=["Video_Name","like_count","titleofchannel"])
        st.table(result_5)
    elif questions =="6. What is the total number of likes for each video, and what are their corresponding video names?":
        mycursor.execute("select Video_Name,like_count,Video_id from video_details order by like_count desc;")
        result_6 =mycursor.fetchall()
        result_6 = pd.DataFrame(result_6, columns=["Video_Name","like_count","Video_id"])
        st.table(result_6)
    elif questions =='7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("select titleofchannel,view_count from channel_details order by view_count;")
        result_7 =mycursor.fetchall()
        result_7 = pd.DataFrame(result_7, columns=["titleofchannel","view_count"])
        st.table(result_7)
    elif questions =='8. What are the names of all the channels that have published videos in the year 2023?':
        mycursor.execute("select titleofchannel,max(Published_date) as Publisheddate from video_details where Published_date like '%2023%' group by titleofchannel;")
        result_8 =mycursor.fetchall()
        result_8 = pd.DataFrame(result_8, columns=["titleofchannel","Publisheddate"])
        st.table(result_8)
    elif questions =="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute("select titleofchannel, avg(duration) as avg_duration from video_details group by titleofchannel order by avg_duration desc;")
        result_9 = mycursor.fetchall()
        result_9 = pd.DataFrame(result_9, columns=["titleofchannel","avg_duration(seconds)"])
        st.table(result_9)
    elif questions =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("select Video_Name,commentCount,titleofchannel from video_details order by commentCount desc limit 50;")
        result_10 = mycursor.fetchall()
        result_10 = pd.DataFrame(result_10, columns=["Video_Name","commentCount","titleofchannel"])
        st.table(result_10)

    mycursor.close()
