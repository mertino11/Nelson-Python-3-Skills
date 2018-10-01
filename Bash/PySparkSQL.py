#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Automation Python Script PySparkSQL.py
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
PySpark_Problem_Statement1 = sqlCtx.sql('SELECT game_id, sum_tweet / min_played as tweetsPerMinutes FROM ( SELECT vq1p2.game_id AS game_id, (UNIX_TIMESTAMP(official_end) - UNIX_TIMESTAMP(official_start)) / 60 AS min_played, vq1p2.sum_tweet AS sum_tweet FROM virtualq1p2 vq1p2 JOIN game g ON g.game_id = vq1p2.game_id ) AS t ORDER BY tweetsPerMinutes DESC LIMIT 10')

# PROBLEM STATEMENT 2 ANSWER - DONE
PySpark_Problem_Statement2 = sqlCtx.sql('SELECT COUNT(distinct user_id) AS users, game_id FROM tweet t JOIN tweet_hashtag th ON th.tweet_id = t.tweet_id JOIN hashtag_fc hfc ON hfc.hashtag_id = th.hashtag_id JOIN game g WHERE ((hfc.fc_id = g.fc_id1) OR (hfc.fc_id = g.fc_id2)) AND ((t.created_time > g.official_start) AND (t.created_time < g.official_end)) GROUP BY game_id ORDER BY users DESC LIMIT 10')
