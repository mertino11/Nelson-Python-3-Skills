#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Automation Python Script PySpark2.0+.py
#  Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

# Packages needed to run this script.
import pyspark
from datetime import datetime
import time
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf
from pyspark.sql.types import *

# Creates a SparkContext.
sc = pyspark.SparkContext('local[*]')

# Doing a check to see if the RDD is working.
rdd = sc.parallelize(range(1000))
rdd.takeSample(False, 5)

# Loading data from HDFS to PySpark --> Create an RDD and split (lambda) the RDD with commas.
fcrdd = sc.textFile("/training/fc/*")
fc = fcrdd.map(lambda x: x.split(","))

gamerdd = sc.textFile("/training/game/*")
game = gamerdd.map(lambda x: x.split(","))

gamegoalsrdd = sc.textFile("/training/game_goals/*")
gamegoals = gamegoalsrdd.map(lambda x: x.split(","))

hashtagrdd = sc.textFile("/training/hashtag/*")
hashtag = hashtagrdd.map(lambda x: x.split(","))

hashtagfcrdd = sc.textFile("/training/hashtag_fc/*")
hashtagfc = hashtagfcrdd.map(lambda x: x.split(","))

tweetrdd = sc.textFile("/training/tweet/*")
tweet = tweetrdd.map(lambda x: x.split(","))

tweethashtagrdd = sc.textFile("/training/tweet_hashtag/*")
tweethashtag = tweethashtagrdd.map(lambda x: x.split(","))

# Creates a Spark SQL Session to work with Dataframes.
spark = SQLContext(sc)

# Creates Dataframes with names for the columns.
df_games = spark.createDataFrame(game,
                                 schema=['game_id', 'fc1', 'fc2', 'start_time_string', 'end_time_string', 'hts', 'hte'])
df_tweet = spark.createDataFrame(tweet, schema=['id', 'created_string', 'uid'])
df_hashtag = spark.createDataFrame(hashtag, schema=['hashtag_id', 'hashtag'])
df_hashtagfc = spark.createDataFrame(hashtagfc, schema=['hashtag_id', 'fc_id'])
df_tweethashtag = spark.createDataFrame(tweethashtag, schema=['tweet_id', 'hashtag_id'])

for col in ['fc1', 'fc2', 'game_id']:
    df_games = df_games.withColumn(col, df_games[col].cast(LongType()))

for col in ['id', 'uid']:
    df_tweet = df_tweet.withColumn(col, df_tweet[col].cast(LongType()))

for col in ['hashtag_id']:
    df_hashtag = df_hashtag.withColumn(col, df_hashtag[col].cast(LongType()))

for col in ['hashtag_id', 'fc_id']:
    df_hashtagfc = df_hashtagfc.withColumn(col, df_hashtagfc[col].cast(LongType()))

for col in ['hashtag_id', 'tweet_id']:
    df_tweethashtag = df_tweethashtag.withColumn(col, df_tweethashtag[col].cast(LongType()))

# Creating a function to convert the time.
unix_timestamp = udf(lambda s: int(datetime.strptime(s, '%Y-%m-%d %H:%M:%S').timestamp()), LongType())

print(df_games.count())

# Creating a function to convert the time in seconds.
unix_timestamp = udf(
    lambda s: int((datetime.strptime(s, '%Y-%m-%d %H:%M:%S.0') - datetime(1970, 1, 1)).total_seconds()), LongType())

print(df_games.count())

# Converting time string to UNIX TIMESTAMP.
df_games = df_games.withColumn('end_time', unix_timestamp(df_games['end_time_string']))
df_games = df_games.withColumn('start_time', unix_timestamp(df_games['start_time_string']))
df_games = df_games.withColumn('diff', df_games['end_time'] - df_games['start_time'])

# Takes the difference in start time and end time in seconds and divides it by 60 to get minutes.
df_games = df_games.withColumn('minutes', df_games['diff'] / 60)

# Converting Time String to UNIX TIMESTAMP.
df_tweet = df_tweet.withColumn('created', unix_timestamp(df_tweet['created_string']))
df_tweet.head()

# Joining Hashtag IDs for both teams to the Games table.
# For Team 1:
df_games = df_games.join(df_hashtagfc, df_hashtagfc['fc_id'] == df_games['fc1'])
df_games = df_games.withColumnRenamed('hashtag_id', 'hashtag_id1')
df_games = df_games.drop('fc_id')

# For Team 2:
df_games = df_games.join(df_hashtagfc, df_hashtagfc['fc_id'] == df_games['fc2'])
df_games = df_games.withColumnRenamed('hashtag_id', 'hashtag_id2')
df_games = df_games.drop('fc_id')

print(df_games.count())
df_games.head(2)

# Joining hashtags to tweets.
df_tweet = df_tweet.join(df_tweethashtag, df_tweethashtag['tweet_id'] == df_tweet['id'])

# Only tweets that happened during the game.
df_tweet = df_tweet.join(df_games, (df_games['start_time'] < df_tweet['created']) &
                         (df_games['end_time'] > df_tweet['created']) &
                         ((df_games['hashtag_id1'] == df_tweet['hashtag_id']) |
                          (df_games['hashtag_id2'] == df_tweet['hashtag_id'])))

# Cache for the speedup
df_tweet.cache()

print(df_tweet.head())

print "Starting with Problem Statement 1."
time.sleep(3)
# PROBLEM STATEMENT 1

# Count the amount of tweets per game.
grouped = df_tweet.groupBy('game_id').agg({'*': 'count', 'minutes': 'first'})

# Creating a new column called per_minute to save the tweets per minute.
grouped = grouped.withColumn('per_minute', grouped['count(1)'] / grouped['first(minutes)']).orderBy('per_minute',
                                                                                                    ascending=False)
grouped.head(10)


print "Starting with Problem Statement 2."
time.sleep(3)
# PROBLEM STATEMENT 2

# Grouping per user id and game id.
# This allows to create a table with a row that shows the unique users that tweeted during the game.
grouped = df_tweet.groupBy(['uid', 'game_id']).agg({'*': 'count'})

# Grouping per game to see and count the amount of users.
grouped = grouped.groupBy('game_id').agg({'*': 'count'}).orderBy('count(1)', ascending=False)

grouped.head(10)


print "Starting with Problem Statement 3."
time.sleep(3)
# PROBLEM STATEMENT 3

# Counting amount of games at 'home'.
games_grouped1 = df_games.groupBy('fc1').count()
games_grouped1 = games_grouped1.withColumnRenamed('count', 'count1')

# Counting amount of games at 'away'.
games_grouped2 = df_games.groupBy('fc2').count()
games_grouped2 = games_grouped2.withColumnRenamed('count', 'count2')

# Joining 'home' and 'away'.
games_grouped = games_grouped1.join(games_grouped2, games_grouped1['fc1'] == games_grouped2['fc2'])

# Calculate the total games played from 'home' and 'away'.
games_grouped = games_grouped.withColumn('total', games_grouped['count1'] + games_grouped['count2'])
games_grouped = games_grouped.orderBy(['total', 'fc1'], ascending=False)
games_grouped = games_grouped.drop('fc2').withColumnRenamed('fc1', 'team_id')
games_grouped.head(3)
games_grouped.count()

tweet_counts_per_team = df_tweet.join(games_grouped, (df_tweet['fc1'] == games_grouped['team_id']) | (
        df_tweet['fc2'] == games_grouped['team_id'])).groupBy('team_id').count()

tweet_counts_per_team.head(5)

local_data = games_grouped.collect()

# For loop for the first three teams.
for i in range(3):
    fc_id = local_data[i]['fc1']
    print(fc_id)
    # het aantal tweets per game
    grouped = df_tweet.groupBy('game_id').agg({'*': 'count', 'fc1': 'first', 'fc2': 'first'})
    grouped = grouped.withColumnRenamed('first(fc1)', 'fc1')
    grouped = grouped.withColumnRenamed('first(fc2)', 'fc2')

    # Selecting only the games where teams with an FC_id have played.
    grouped = grouped.filter((grouped.fc1 == fc_id) | (grouped.fc2 == fc_id)).orderBy('count(1)', ascending=False)
    first_5 = grouped.head(5)
    for row in first_5:
        print(row)

print "Starting with Problem Statement 4."
time.sleep(3)
# PROBLEM STATEMENT 4

'''
Filtering on tweets in the last 10 minutes of the game.
Then creating an extra column with the minutes of the tweets.
After that grouping it by game and time.
Counting the amount of tweets per minute.
Grouping by on game and takes the maximum (spike).
'''
grouped = df_tweet.filter(df_tweet['created'] > df_tweet['end_time'] - 600).withColumn('created_minute',
                                                                                       (df_tweet['created'] / 60).cast(
                                                                                           LongType())).groupBy(
    ['game_id', 'created_minute']).count().groupBy('game_id').max().orderBy('max(count)', ascending=False)

grouped.head(10)

print "Starting with Problem Statement 5."
time.sleep(3)
# PROBLEM STATEMENT 5

# Re-using tweets table but throwing columns away that are not needed at all.
# And then joining the hashtags on tweets.
all_hashtags = df_tweet.drop('tweet_id').drop('hashtag_id').join(df_tweethashtag,
                                                                 df_tweet['id'] == df_tweethashtag["tweet_id"])

# Cache for the speedup
all_hashtags.cache()

# Filtering to only keep and show the un-official hashtags.
hashtag_count = all_hashtags.filter((all_hashtags['hashtag_id'] != all_hashtags['hashtag_id1']) & (
        all_hashtags['hashtag_id'] != all_hashtags['hashtag_id2'])).groupBy('hashtag_id').count()

# Ordering it by hashtag id.
hashtag_count.orderBy('count', ascending=False).head(10)

print "Starting with Problem Statement 6."
time.sleep(3)
# PROBLEM STATEMENT 6

# Filtering game_id by 47 (because why not).
grouped = df_tweet.filter(df_tweet['game_id'] == 47).withColumn('created_minute',
                                                                (df_tweet['created'] / 60).cast(LongType())).groupBy(
    'created_minute').count().orderBy('created_minute')

grouped.show()

# Converting Problem Statement 6 solution to a CSV-file.
print "Converting into a CSV-file..."
grouped.save('PySpark_Problem_Statement_6.csv', 'com.databricks.spark.csv')
print "Done!"
print "Python Script has been done."
