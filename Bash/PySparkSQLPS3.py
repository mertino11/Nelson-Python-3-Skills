#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Automation Python Script for PySpark SQL Problem Statement 3.
#  Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

# Imports packages.
import subprocess
import os
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext

# Creates SparkContext session.
sc = SparkContext("local","master")

# SQL Context
sqlCtx = SQLContext(sc)

# Imports HDFS-Data to RDD and converts it directly to a Dataframe in PySpark.
fcdf = sqlCtx.load(source = 'com.databricks.spark.csv', header = 'false', path= 'hdfs://localhost/training/fc/*')
# Add manually column names, because while importing, it gives Column names like C0, C1 etc.
fcdf = fcdf.selectExpr('C0 as fc_id', 'C1 as name')

gamedf = sqlCtx.load(source = 'com.databricks.spark.csv', header = 'false', path= 'hdfs://localhost/training/game/*')
gamedf = gamedf.selectExpr('C0 as game_id', 'C1 as fc_id1', 'C2 as fc_id2', 'C3 as official_start', 'C4 as official_end', 'C5 as halftime_start', 'C6 as halftime_end')

game_goalsdf = sqlCtx.load(source = 'com.databricks.spark.csv', header = 'false', path= 'hdfs://localhost/training/game_goals/*')
game_goalsdf = game_goalsdf.selectExpr('C0 as game_id', 'C1 as time', 'C2 as fc')

tweetdf = sqlCtx.load(source = 'com.databricks.spark.csv', header = 'false', path= 'hdfs://localhost/training/tweet/*')
tweetdf = tweetdf.selectExpr('C0 as tweet_id', 'C1 as created_time', 'C2 as user_id')

tweet_hashtagdf = sqlCtx.load(source = 'com.databricks.spark.csv', header = 'false', path= 'hdfs://localhost/training/tweet_hashtag/*')
tweet_hashtagdf = tweet_hashtagdf.selectExpr('C0 as tweet_id', 'C1 as hashtag_id')

hashtagdf = sqlCtx.load(source = 'com.databricks.spark.csv', header = 'false', path= 'hdfs://localhost/training/hashtag/*')
hashtagdf = hashtagdf.selectExpr('C0 as hashtag_id', 'C1 as message')

hashtag_fcdf = sqlCtx.load(source = 'com.databricks.spark.csv', header = 'false', path= 'hdfs://localhost/training/hashtag_fc/*')
hashtag_fcdf = hashtag_fcdf.selectExpr('C0 as hashtag_id', 'C1 as fc_id')

# Dataframes converting to Tables
fcdf.registerAsTable('fc')
gamedf.registerAsTable('game')
game_goalsdf.registerAsTable('game_goals')
tweetdf.registerAsTable('tweet')
tweet_hashtagdf.registerAsTable('tweet_hashtag')
hashtagdf.registerAsTable('hashtag')
hashtag_fcdf.registerAsTable('hashtag_fc')

# PROBLEM STATEMENT 3 ANSWER

# Only shows how many teams have played (games) during the league.
q3p1 = sqlCtx.sql('SELECT name, COUNT(game_id) AS numb_of_games FROM fc f JOIN game g WHERE (f.fc_id = g.fc_id1 OR f.fc_id = g.fc_id2) GROUP BY name ORDER BY numb_of_games DESC')

q3p1.registerAsTable('q3p1')
q3p1.show()





