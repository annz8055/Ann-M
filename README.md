# Ann-M
YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit
Problem Statement

The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
Option to store the data in a MongoDB database as a data lake.
Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

Introduction

YouTube, the online video-sharing platform serves as a hub for entertainment, education, and community engagement. With its vast user base and diverse content library, YouTube has become a powerful tool for individuals, creators, and businesses to share their stories, express themselves, and connect with audiences worldwide.
This project extracts the particular youtube channel data by using the youtube channel id, processes the data, and stores it in the MongoDB database. It has the option to migrate the data to MySQL from MongoDB then analyse the data and give the results depending on the customer questions.

Approach: 

Set up a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.
Connect to the YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.
Store data in a MongoDB data lake: Once you retrieve the data from the YouTube API, you can store it in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.
Query the SQL data warehouse: You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. You can use a Python SQL library such as SQLAlchemy to interact with the SQL database.
Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. You can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.
Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.

Technologies Used

Python: The programming language used for building the application and scripting tasks.
Streamlit: A Python library used for creating interactive web applications and data visualizations.
YouTube API: Google API is used to retrieve channel and video data from YouTube.
MongoDB: A NoSQL database used as a data lake for storing retrieved YouTube data.
SQL (MySQL): A relational database used as a data warehouse for storing migrated YouTube data.
Pandas: A data manipulation library used for data processing and analysis.

Conclusion

The YouTube Data Harvesting and Warehousing project provides a powerful tool for retrieving, storing, and analyzing YouTube channel and video data. By leveraging SQL, MongoDB, and Streamlit, users can easily access and manipulate YouTube data in a user-friendly interface. The project allows users to gain insights from the vast amount of YouTube data available.
