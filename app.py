import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from streamlit_option_menu import option_menu
from models import Channel, Video, Comment, insert_data_into_db, youtube_project

import mysql.connector
mydb = mysql.connector.connect(
 host="localhost",
 user="root",
 password="",
 )

print(mydb)
mycursor = mydb.cursor(buffered=True)

# Connect to the MySQL database
engine = create_engine('mysql+pymysql://root:@localhost:3306/youtube')
Session = sessionmaker(bind=engine)
session = Session()

# Streamlit app
st.title('YouTube Data Viewer')


with st.sidebar:
    selected = option_menu(None, ["Home","Extract and view data","SQL Query"])

if selected == "Home":
    
    st.title("YouTube Data Harvesting and Warehousing")
    st.markdown("### Tools used in the project")
    st.markdown("- Python")
    st.markdown("- SQL")
    st.markdown("- Streamlit")
    st.markdown("- SQLAlchemy")
    

if selected == "Extract and view data" :
 # Data retrieval from YouTube API
 channel_id = st.text_input('Enter Channel ID')

 if st.button("submit"):
        data = youtube_project(channel_id)
        insert_data_into_db(data)
        st.success("Data uploaded successfully")

 # Display channels
 channel= st.text_input('Enter channel details')
 if st.button("Fetch Channel Details"):
     st.header('Channels')
     channel = session.query(Channel).filter_by(channelid=channel_id).first()
     if channel:
        st.subheader(channel.name)
        st.write(f"Channel ID: {channel.channelid}")
        st.write(f"Subscriber Count: {channel.subscriber_count}")
        st.write(f"Video Count: {channel.video_count}")
        st.write(f"Playlist ID: {channel.playlist_id}")
  # Display videos
        st.header('Videos')
        videos = session.query(Video).filter_by(channel_id=channel_id).all()
        if videos:
          for video in videos:
                st.subheader(video.title)
                st.write(f"Video ID: {video.video_id}")
                st.write(f"channel Title: {video.channel_name}")
                st.write(f"View Count: {video.view_count}")
                st.write(f"Title name:{video.title}")
                st.write(f"Like Count: {video.like_count}")
                st.write(f"Comment Count: {video.comment_count}")
                st.write(f"Caption Status: {video.caption_status}")
                st.write(f"Description: {video.description}")

 # Display comments
        st.header('Comments')
        comments = session.query(Comment).filter(Comment.video_id.in_([video.video_id for video in videos])).all()
        if comments:
          for comment in comments:
                st.subheader(f"Comment ID: {comment.comment_id}")
                st.write(f"Video ID: {comment.video_id}")
                st.write(f"Comment Text: {comment.comment_text}")
                st.write(f"Comment Author: {comment.comment_author}")
                st.write(f"Comment Published: {comment.comment_published}")

engine = create_engine('mysql+pymysql://root:@localhost:3306/youtube')
Session = sessionmaker(bind=engine)
session = Session()

if selected == "SQL Query":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
          videos_data = session.query(Video.title, Video.channel_name).all()
          for title, video_id in videos_data:
              df = pd.DataFrame(videos_data)
          st.write(df)
    if questions == '2. Which channels have the most number of videos, and how many videos do they have?':
          result = session.query(Video.channel_name, func.count(Video.id).label('video_count')).group_by(Video.channel_name).order_by(func.count(Video.id).desc()).first()
          if result:
            channel_name, video_count = result
            df = pd.DataFrame([{'Channel Name': channel_name, 'Video Count': video_count}])
          st.write(df)
    if questions == '3. What are the top 10 most viewed videos and their respective channels?':
           top_videos = session.query(Video, Channel).join(Channel).order_by(Video.view_count.desc()).limit(10).all()
           session.close()
           data = []
           for video, channel in top_videos:
                data.append({
                        'Video Title': video.title,
                        'Channel Name': channel.name,
                        'View Count': video.view_count
                })
                df = pd.DataFrame(data)
           st.write(df)
    if questions == '4. How many comments were made on each video, and what are their corresponding video names?':
           comments_count = session.query(Video.title, Video.video_id, func.count(Comment.id)).join(Comment).group_by(Video.title, Video.video_id).all()
           session.close()
           df = pd.DataFrame(comments_count, columns=['Video Name', 'Video ID', 'Comments Count'])
           st.write(df)
    if questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
           videos_likes = session.query(Video.title, Video.video_id, Video.like_count, Channel.name)\
                    .join(Channel).order_by(Video.like_count.desc()).limit(10).all()
           session.close()
           df = pd.DataFrame(videos_likes, columns=['Video Name', 'Video ID', 'Likes Count', 'Channel Name'])
           st.write(df)
    if questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
           total_likes= session.query(Video.title, Video.video_id, func.sum(Video.like_count).label('total_likes'))\
                        .group_by(Video.video_id).all()
           session.close()
           df = pd.DataFrame(total_likes, columns=['Video Name', 'Video ID', 'Total Likes'])
           st.write(df)
    if questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
           views_per_channel = session.query(Channel.name, func.sum(Video.view_count).label('total_views'))\
                           .join(Video, Channel.channelid == Video.channel_id)\
                           .group_by(Channel.name).all()
           session.close()
           df = pd.DataFrame(views_per_channel, columns=['Channel Name', 'Total Views'])
           st.write(df)
    if questions == '8. What are the names of all the channels that have published videos in the year 2022?':
           channels_2022 = session.query(Channel.name)\
                       .join(Video, Channel.channelid == Video.channel_id)\
                       .filter(Video.published_at.between('2022-01-01', '2022-12-31'))\
                       .distinct().all()
           session.close()
           df =pd.DataFrame(channels_2022, columns=['Channel Name'])
           st.write (df)
    if questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
           avg_duration_per_channel = session.query(Channel.name, func.avg(Video.duration).label('avg_duration'))\
                                   .join(Video, Channel.channelid == Video.channel_id)\
                                   .group_by(Channel.name).all()
           session.close()
           df = pd.DataFrame(avg_duration_per_channel, columns=['Channel Name', 'Average Duration'])
           st.write (df)
    if questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
           videos_with_most_comments = session.query(Video.title, Channel.name, func.count(Comment.id).label('num_comments'))\
                                   .join(Channel, Video.channel_id == Channel.channelid)\
                                   .join(Comment, Video.video_id == Comment.video_id)\
                                   .group_by(Video.title, Channel.name)\
                                   .order_by(func.count(Comment.id).desc())\
                                   .limit(10).all()
           session.close()
           df = pd.DataFrame(videos_with_most_comments, columns=['Video Title', 'Channel Name', 'Number of Comments'])
           st.write (df)