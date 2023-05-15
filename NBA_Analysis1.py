#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
import streamlit as st


# In[2]:


spark = (SparkSession.builder.config("spark.driver.memory","4g").config("spark.driver.maxResultSize", "4g").getOrCreate())


# In[3]:


df1= spark.read.csv(path=r'C:\Users\manodnyagaikwad\Downloads\shotdetail.csv',inferSchema=True,header=True)


# In[ ]:


df= pd.read_csv('C:/Users/manodnyagaikwad/Downloads/shotdetail.csv')


# In[ ]:


st.title('NBA Data Explorer')
st.text('This is a web app to allow exploration of NBA player Data')


# In[ ]:


#st.write(df1.toPandas().head())


# In[5]:


df1.createOrReplaceTempView("nba_shots")


# In[18]:


made_missed_shots = spark.sql("SELECT EVENT_TYPE, COUNT(*) AS num_shots FROM nba_shots GROUP BY EVENT_TYPE").toPandas()
#made_missed_shots.show()
st.code('''"SELECT EVENT_TYPE, COUNT(*) AS num_shots FROM nba_shots GROUP BY EVENT_TYPE").toPandas()''')


# In[19]:


st.set_option('deprecation.showPyplotGlobalUse', False)
fig = plt.bar(made_missed_shots['EVENT_TYPE'],made_missed_shots['num_shots'])
plt.title('SHOT MADE ')
plt.xlabel('SHOT TYPE')
plt.ylabel('NO OF MADE AND MISSED SHOTS ')
st.pyplot()


# In[21]:


#AVERAGE SHOT DISTANCES BY SHOT TYPE
avg_distance = spark.sql("SELECT SHOT_TYPE, AVG(SHOT_DISTANCE) AS avg_distance FROM nba_shots GROUP BY SHOT_TYPE").toPandas()
#avg_distance.show()


# In[ ]:


st.code('''"#AVERAGE SHOT DISTANCES BY SHOT TYPE
avg_distance = spark.sql("SELECT SHOT_TYPE, AVG(SHOT_DISTANCE) AS avg_distance FROM nba_shots GROUP BY SHOT_TYPE").toPandas()
#avg_distance.show()"''')


# In[22]:


plt.pie(avg_distance['avg_distance'], labels=avg_distance['SHOT_TYPE'], autopct='%1.1f%%', explode=[0,0,0], shadow=True, startangle=90)
plt.title('AVERAGE DISTANCE VS SHOT TYPE')
plt.axis('equal')
st.pyplot()


# In[ ]:


top_players = spark.sql("SELECT PLAYER_NAME, COUNT(*) AS num_shots FROM nba_shots GROUP BY PLAYER_NAME ORDER BY num_shots DESC LIMIT 10").toPandas()
#top_players.show()


# In[ ]:


st.code('''"top_players = spark.sql("SELECT PLAYER_NAME, COUNT(*) AS num_shots FROM nba_shots GROUP BY PLAYER_NAME ORDER BY num_shots DESC LIMIT 10").toPandas()
#top_players.show()"''')


# In[ ]:


st.write(top_players)


# In[ ]:


plt.pie(top_players['num_shots'], labels=top_players['PLAYER_NAME'], autopct='%1.1f%%', explode=[0,0.1,0,0,0,0,0,0,0,0], shadow=True, startangle=90)
plt.title('top players based of num of shots')
plt.axis('equal')
st.pyplot()


# In[6]:


#shooting percentage for each player 
shootingpercentage=spark.sql("SELECT PLAYER_NAME, (COUNT(CASE WHEN shot_made_flag = 1 THEN 1 ELSE NULL END) / COUNT(*)) * 100 as shooting_percentage FROM nba_shots GROUP BY player_name ORDER BY shooting_percentage DESC").toPandas()
st.write(shootingpercentage.head(10))


# In[9]:


# total number of missed and made shots 
query = ("""
SELECT team_name, 
    COUNT(CASE WHEN shot_made_flag = 1 THEN 1 ELSE NULL END) as made_shots,
    COUNT(CASE WHEN shot_made_flag = 0 THEN 1 ELSE NULL END) as missed_shots
FROM nba_shots
GROUP BY team_name
""")
result = spark.sql(query).toPandas()
st.write(result.head())


# In[11]:




# Set the team_name column as the index
result.set_index('team_name', inplace=True)

# Create a stacked bar chart
result.plot(kind='bar', stacked=True)

# Set labels and title
plt.xlabel('Team')
plt.ylabel('Number of Shots')
plt.title('Number of Made and Missed Shots for Each Team')

# Show the plot
st.pyplot()


# In[12]:


#average shot distance for each player 
query = """
SELECT `player_name`, AVG(`shot_distance`) as `avg_shot_distance`
FROM `nba_shots`
GROUP BY `player_name`
ORDER BY `avg_shot_distance` DESC
"""

result = spark.sql(query).toPandas()
st.write(result.head())


# In[13]:


# Convert Spark dataframe to Pandas dataframe

# Create histogram
plt.hist(result['avg_shot_distance'], bins=20)

# Set labels and title
plt.xlabel('Average Shot Distance')
plt.ylabel('Frequency')
plt.title('Distribution of Average Shot Distances')

# Show the plot
st.pyplot()


# In[ ]:




