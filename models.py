import googleapiclient.discovery
import pandas as pd
import mysql.connector
mydb = mysql.connector.connect(
 host="localhost",
 user="root",
 password="",
 )

print(mydb)
mycursor = mydb.cursor(buffered=True)


api_service_name = "youtube"
api_version = "v3"
api_key = 'AIzaSyB4Kb0eG7cbxl0Kjei_p7QeEpHS2oaxCeU'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = api_key)

channel_ids=['UCj22tfcQrWG7EMEKS0qLeEg','UC0sqAUdSWwXTAiiomjVpLlw','UCU1VNvcwKDmsHqbTtUOaZ-w','UCEBpSZhI1X8WaP-kY_2LLcg',
            'UCwYdZhlLQngd0lWgEUDACIA','UCh4xsM0PaDFcPiDpSbtV28w','UCoieqrLbDthrDBY1KNTnQUA','UCVXHYmFar7yArWvkcjxWXuQ',
            'UC68KSmHePPePCjW4v57VPQg','UC7N24rONpE-lAX3Busp_umg']

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = 'AIzaSyB4Kb0eG7cbxl0Kjei_p7QeEpHS2oaxCeU'
youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey= DEVELOPER_KEY)

def get_channel_details(channel_ids):
    channel_data = []

    request = youtube.channels().list(
           part="snippet,statistics,contentDetails",
           id=channel_ids
    )
    response = request.execute()

    for item in response['items']:
        data = {
            'Channelid': item['id'],
            'Channel_name': item['snippet']['title'],
            'description': item['snippet']['description'],
            'subscriber': item['statistics']['subscriberCount'],
            'viewcount': item['statistics']['viewCount'],
            'videocount': item['statistics']['videoCount'],
            'playlistid': item['contentDetails']['relatedPlaylists']['uploads']
        }
        channel_data.append(data)

    return channel_data

def get_videoid (channel_ids):
     video_ids=[]
     response = youtube.channels().list(id=channel_ids,part='contentDetails').execute()
     for i in range(len(response['items'])):
      playlist_Id = response ['items'][i]['contentDetails']['relatedPlaylists']['uploads']     
     next_page = None

     while True:
          response1 = youtube.playlistItems().list(
             part='snippet',
             playlistId = playlist_Id,
             maxResults = 50,
             pageToken = next_page
             ).execute()

          for i in range(len(response1['items'])):
               video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
          next_page = response1.get('nextPageToken')

          if next_page is None:
               break
     return video_ids

def get_video_details(youtube,video_ids):
    video_info = []
    for video_id in video_ids:
        try:
            request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id= video_id)
            response = request.execute()

            for item in response['items']:
                duration_str = item['contentDetails']['duration']
                minutes_str, seconds_str = duration_str.split('M')
                minutes = int(minutes_str[2:]) if 'M' in duration_str else 0
                seconds = int(seconds_str[:-1]) if 'S' in duration_str else 0
               
                total_minutes = minutes + (seconds / 60)

                data = {'channel_name':item['snippet']['channelTitle'],
                    'channel_id': item['snippet']['channelId'],
                    'video_id': item['id'],
                    'video_name': item['snippet']['title'],
                    'tags': item['snippet'].get('tags', []),
                    'published_at': item['snippet']['publishedAt'],
                    'thumbnail': item['snippet']['thumbnails'],
                    'view_count': item['statistics'].get('viewCount', 0),
                    'like_count': item['statistics'].get('likeCount', 0),
                    'favorite_count': item['statistics'].get('favoriteCount', 0),
                    'comment_count': item['statistics'].get('commentCount', 0),
                    'duration': total_minutes,
                    'caption_status': item['contentDetails'].get('caption', ''),
                    'definition': item['contentDetails'].get('definition', '')
                }
                video_info.append(data)
        except:
            print('You have exceeded your YouTube API quota.')
            
    return video_info
            
    return video_info
def get_video_comments(video_ids):
  comment_data=[]
  try:
      for video_id in video_ids:
        request = youtube.commentThreads().list(
            part='snippet',
            videoId= video_id, maxResults=100
        )
        response = request.execute()
          

        for item in response['items']:
          comment = dict(Comment_Id= item['snippet']['topLevelComment']['id'],
                        Video_Id= item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
          comment_data.append(comment)
  except:
      pass
  return comment_data


def youtube_project (channel_ids):
    ch_details =get_channel_details(channel_ids)
    video_id = get_videoid(channel_ids)
    video_details =get_video_details(youtube,video_id)
    comment_details =get_video_comments(video_id)
 
    data = {"Channel_information": ch_details,
            "Video_identification": video_id,
            "Video_details": video_details,
            "Comment_information": comment_details}
    final_output = data
    
    return final_output

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

engine = create_engine('mysql+pymysql://root:@localhost:3306/youtube')

Base = declarative_base()

class Channel(Base):
    __tablename__ ='channels'

    id = Column(Integer, primary_key=True, autoincrement=True)
    channelid =Column(String(50),unique=True)
    name = Column(String(50))
    subscriber_count = Column(Integer)
    video_count = Column(Integer)
    playlist_id = Column(String(50))
    videos = relationship('Video', back_populates='channel')

class Video(Base):
    __tablename__ ='videos'

    id = Column(Integer, primary_key=True, autoincrement=True)

    video_id = Column(String(50), unique=True)
    channel_name=Column(String(50))
    title = Column(String(100))
    description = Column(String(500))
    published_at = Column(DateTime)
    duration = Column(Integer)
    view_count =Column(Integer)
    like_count =Column(Integer)
    comment_count =Column(Integer)
    caption_status =Column(String(200))
    channel_id = Column(String(50), ForeignKey('channels.channelid'))
    channel = relationship('Channel', back_populates='videos')
    comments = relationship('Comment', back_populates='video')

class Comment(Base):
   __tablename__ ='Comments'

   id = Column(Integer, primary_key=True, autoincrement=True)
   comment_id = Column(String(100), primary_key=True)
   video_id = Column(String(50), ForeignKey('videos.video_id')) # Reference the primary key of the Video model
   comment_text = Column(String(500))
   comment_author = Column(String(50))
   comment_published = Column(DateTime)
   video = relationship('Video', back_populates='comments')
Base.metadata.create_all(engine)

def insert_data_into_db(data):
    Session = sessionmaker(bind=engine)
    session = Session()

    for channel_info in data['Channel_information']:
        channel = Channel(channelid = channel_info['Channelid'],
                          name= channel_info['Channel_name'],
                          subscriber_count=channel_info['subscriber'],
                          video_count=channel_info['videocount'],
                          playlist_id=channel_info['playlistid'])
        session.add(channel)
        session.commit()  # Commit after adding each channel to ensure channel id is generated
    
    for video_info in data['Video_details']:
            video = Video(
                    video_id=video_info['video_id'],
                    channel_name= video_info['channel_name'],
                    title= video_info['video_name'],
                    published_at=video_info['published_at'],
                    duration = video_info['duration'],
                    view_count=video_info['view_count'],
                    like_count=video_info['like_count'],
                    comment_count=video_info['comment_count'],
                    caption_status=video_info['caption_status'],
                    channel_id=channel.channelid)
            
            session.add(video)
            session.commit()

    for comment_info in data['Comment_information']:
        video_id= comment_info['Video_Id']
        video = session.query(Video).filter_by(video_id=video_id).first()
        if video:
            comment = Comment(
                        comment_id=comment_info['Comment_Id'],
                        video_id= video_id,
                        comment_text=comment_info['Comment_Text'],
                        comment_author=comment_info['Comment_Author'],
                        comment_published=comment_info['Comment_published'])
        
            session.add(comment)
            session.commit()
    
   
    session.close()

