#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Load MySQL File --> HDFS --> Impala.
#  Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

echo "
Name:			Mert Cakir
Student Number: 	@00518279
Course: 		MSc Big Data Tools and Techniques
File:			Load MySQL File --> HDFS --> Impala.
Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

_______________________________________________________________________________________

!!! MAKE SURE TO HAVE DATA.SQL LOCATED IN YOUR DESKTOP OR CANCEL NOW WITH CTRL + C !!!

             !!! MAKE SURE TO RUN THE SCRIPT AS ROOT TO RUN SERVICES !!!
_______________________________________________________________________________________

Script is starting in 5..."
sleep 1

echo "4..."
sleep 1

echo "3..."
sleep 1

echo "2..."
sleep 1

echo "1..."
sleep 1

printf "\nScript is starting NOW! \n\n"

echo "Logging into MySQL... Creating and using database 'training'... And loading SQL data in the database 'training'..."

# Creates and use the Database training.
mysql -utraining -ptraining -e "CREATE DATABASE training;
USE training;

-- Importing data.sql
source /home/training/Desktop/data.sql;"

echo "SQL.data has been imported to the database 'training'."

echo "Creating a directory in HDFS called training..."
# Creates a directory in HDFS.
hdfs dfs -mkdir training

# Sqoop grabs MySQL data (via connection) and save it in HDFS under the directory training.
sqoop import-all-tables --connect jdbc:mysql://localhost/training --username training --password training --warehouse-dir /training

echo "Export MySQL to Hadoop is a success!"

echo "Starting services to use Hadoop/Impala..."

# Starts the services that are required to use Impala, Hive or Hue.
sudo service zookeeper-server start
sudo service hive-server2 start 
sudo service oozie start

echo "Services has been started successfully!"

echo "Creating database in Hue (Impala)..."

# Creating database in Impala, use database in Impala and import all the tables from HDFS.
impala-shell -q "CREATE DATABASE training;
use training;

-- Create External Tables in Impala...
CREATE EXTERNAL TABLE fc
(fc_id SMALLINT ,
name STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/training/fc';

CREATE EXTERNAL TABLE game
(game_id SMALLINT,
fc_id1 SMALLINT ,
fc_id2 SMALLINT ,
official_start TIMESTAMP ,
official_end TIMESTAMP ,
halftime_start TIMESTAMP ,
halftime_end TIMESTAMP)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/training/game';

CREATE EXTERNAL TABLE game_goals
(game_id SMALLINT ,
time TIMESTAMP ,
fc SMALLINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/training/game_goals';

CREATE EXTERNAL TABLE hashtag
(hashtag_id BIGINT ,
message STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/training/hashtag';

CREATE EXTERNAL TABLE hashtag_fc
(hashtag_id BIGINT ,
fc_id SMALLINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/training/hashtag_fc';

CREATE EXTERNAL TABLE tweet
(tweet_id BIGINT ,
created_time TIMESTAMP ,
user_id BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/training/tweet';

CREATE EXTERNAL TABLE tweet_hashtag
(tweet_id BIGINT ,
hashtag_id BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/training/tweet_hashtag';
"
printf "\nScript is done!"
printf "\nGoing back to Run.sh..."
/home/training/Desktop/Run.sh
