#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Automation Python Script for PySpark SQL Problem Statement 2.
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

# PROBLEM STATEMENT 2 ANSWER

# Select all columns from other tables that I need and put it into one table.
q2p1 = sqlCtx.sql('SELECT hashtag_id ,fc_id1 ,fc_id2 ,game_id ,official_start,official_end FROM hashtag_fc INNER JOIN game ON (hashtag_fc.fc_id = game.fc_id1) OR (hashtag_fc.fc_id = game.fc_id2)')
q2p1.registerTempTable('q2p1')
q2p2 = sqlCtx.sql('SELECT q2p1.hashtag_id,q2p1.fc_id1,q2p1.fc_id2,q2p1.game_id,q2p1.official_start,q2p1.official_end,tweet_hashtag.tweet_id FROM q2p1 INNER JOIN tweet_hashtag ON q2p1.hashtag_id = tweet_hashtag.hashtag_id')
q2p2.registerTempTable('q2p2')
q2p3 = sqlCtx.sql('SELECT q2p2.hashtag_id,q2p2.game_id,q2p2.official_start,q2p2.official_end,q2p2.tweet_id,tweet.created_time,tweet.user_id FROM q2p2 INNER JOIN tweet ON q2p2.tweet_id = tweet.tweet_id')
q2p3.registerTempTable('q2p3')
q2p4 = sqlCtx.sql('SELECT * FROM q2p3 WHERE created_time BETWEEN official_start AND official_end')
q2p4.registerTempTable('q2p4')

# Use the query that I needed to answer Problem Statement 2.
q2p5 = sqlCtx.sql('SELECT COUNT(DISTINCT user_id) AS UniqueUsers,game_id FROM q2p4 GROUP BY game_id ORDER BY UniqueUsers DESC')

q2p5.show()
print ("Saving result in a CSV-file...")
q2p5.toPandas().to_csv('PySpark Problem Statement2.csv')
print ("CSV-file has been created!")
