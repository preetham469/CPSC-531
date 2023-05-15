#!/usr/bin/env python
# coding: utf-8

# In[2]:


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


# In[3]:


spark = (SparkSession.builder.config("spark.driver.memory","4g").config("spark.driver.maxResultSize", "4g").getOrCreate())


# In[4]:


df1= spark.read.csv(path=r'C:\Users\manodnyagaikwad\Downloads\shotdetail.csv',inferSchema=True,header=True)


# In[5]:


df= pd.read_csv('C:/Users/manodnyagaikwad/Downloads/shotdetail.csv')


# In[6]:


df.columns


# In[ ]:


import streamlit as st


# In[ ]:


st.title('NBA Data Explorer')
st.text('This is a web app to allow exploration of NBA player Data')


# In[ ]:


#st.write(df1.toPandas().head())


# In[ ]:


df1.createOrReplaceTempView("nba_shots")
df1.registerTempTable("charts")


# In[ ]:


made_missed_shots = spark.sql("SELECT EVENT_TYPE, COUNT(*) AS num_shots FROM nba_shots GROUP BY EVENT_TYPE").toPandas()
#made_missed_shots.show()
st.code('''"SELECT EVENT_TYPE, COUNT(*) AS num_shots FROM nba_shots GROUP BY EVENT_TYPE").toPandas()''')


# In[ ]:


st.set_option('deprecation.showPyplotGlobalUse', False)
fig = plt.bar(made_missed_shots['EVENT_TYPE'],made_missed_shots['num_shots'])
plt.title('SHOT MADE ')
plt.xlabel('SHOT TYPE')
plt.ylabel('NO OF MADE AND MISSED SHOTS ')
st.pyplot()


# In[ ]:


#AVERAGE SHOT DISTANCES BY SHOT TYPE
avg_distance = spark.sql("SELECT SHOT_TYPE, AVG(SHOT_DISTANCE) AS avg_distance FROM nba_shots GROUP BY SHOT_TYPE").toPandas()
#avg_distance.show()


# In[ ]:


st.code('''"#AVERAGE SHOT DISTANCES BY SHOT TYPE
avg_distance = spark.sql("SELECT SHOT_TYPE, AVG(SHOT_DISTANCE) AS avg_distance FROM nba_shots GROUP BY SHOT_TYPE").toPandas()
#avg_distance.show()"''')


# In[ ]:


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


# In[ ]:


#shooting percentage for each player 
shootingpercentage=spark.sql("SELECT PLAYER_NAME, (COUNT(CASE WHEN shot_made_flag = 1 THEN 1 ELSE NULL END) / COUNT(*)) * 100 as shooting_percentage FROM nba_shots GROUP BY player_name ORDER BY shooting_percentage DESC").toPandas()
shootingpercentage.head(10)


# In[ ]:


# total number of missed and made shots 
query = """
SELECT team_name, 
    COUNT(CASE WHEN shot_made_flag = 1 THEN 1 ELSE NULL END) as made_shots,
    COUNT(CASE WHEN shot_made_flag = 0 THEN 1 ELSE NULL END) as missed_shots
FROM nba_shots
GROUP BY team_name
"""

result = spark.sql(query)
st.write(result.toPandas())


# In[ ]:


result_pd = result.toPandas()

# Set the team_name column as the index
result_pd.set_index('team_name', inplace=True)

# Create a stacked bar chart
result_pd.plot(kind='bar', stacked=True)

# Set labels and title
plt.xlabel('Team')
plt.ylabel('Number of Shots')
plt.title('Number of Made and Missed Shots for Each Team')

# Show the plot
st.pyplot()


# In[ ]:


#average shot distance for each player 
query = """
SELECT `player_name`, AVG(`shot_distance`) as `avg_shot_distance`
FROM `nba_shots`
GROUP BY `player_name`
ORDER BY `avg_shot_distance` DESC
"""

result = spark.sql(query)
st.write(result.toPandas())


# In[ ]:


# Convert Spark dataframe to Pandas dataframe
result_pd = result.toPandas()

# Create histogram
plt.hist(result_pd['avg_shot_distance'], bins=20)

# Set labels and title
plt.xlabel('Average Shot Distance')
plt.ylabel('Frequency')
plt.title('Distribution of Average Shot Distances')

# Show the plot
st.pyplot()

