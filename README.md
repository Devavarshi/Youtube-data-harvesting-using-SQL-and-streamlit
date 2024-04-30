# Youtube-data-harvesting-using-SQL-and-streamlit

Project Overview:
The YouTube Data Harvesting and Warehousing project is designed to extract specific data from YouTube using the Google API. The extracted data is stored in a SQL database and made accessible for analysis and exploration within a Streamlit app.

Technology Stack:
1.	Python
2.	MySQL
3.	SQLAlchemy
4.	Streamlit
   
Project Plan:
1.	Established a connection to the YouTube API to retrieve channel and video data using the Google API client library for Python.
2.	Developed Python functions to fetch various information such as channel statistics, playlist data, video IDs, video details, and comment details using the YouTube API.
3.	Defined Python functions to extract necessary YouTube details using channel IDs.
4.	Set up a MySQL server and established a connection with Python using XAMPP control panel V3.
5.	Utilized SQLAlchemy to create an engine connecting with MySQL and created base tables and wrote functions to insert data into these tables.
6.	Transferred collected data (channel information, video details, and comment details) from multiple sources to a SQL data warehouse.
7.	Installed and configured the Streamlit application in Python. Designed the application to allow users to enter a channel ID, which is then stored in the SQL database. Users can view the fetched information through the app.
8.	Integrated 10 questions into the application, allowing users to select the desired option to obtain answers based on the data stored in the database.
