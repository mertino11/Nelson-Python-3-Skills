#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Answers the Problem Statements of Impala + bonus marks.
#  Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

echo "
Name:			Mert Cakir
Student Number: 	@00518279
Course: 		MSc Big Data Tools and Techniques
File:			Answers the Problem Statements of Impala + bonus marks.
Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

_______________________________________________________________________________________

!!! MAKE SURE THAT YOU RUNNED THE SCRIPT PREPARATION.SH OR THIS SCRIPT WON'T WORK !!!
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

# Runs queries to solve Problem Statement 1.
impala-shell -q "use training;
-- Problem Statement 1: Extract and present the average number of tweets per during game minute for the top 10 (i.e. most tweeted about during the event) games.

-- CREATES A VIEW TO COUNT ALL TWEETS DURING GAME.
CREATE VIEW virtualq1p1 AS 
SELECT COUNT(t.tweet_id) AS sum_tweet, game_id
FROM tweet t
  JOIN tweet_hashtag th
  ON th.tweet_id = t.tweet_id
  JOIN hashtag_fc hfc
  ON hfc.hashtag_id = th.hashtag_id
  JOIN game g
WHERE ((hfc.fc_id = g.fc_id1) OR (hfc.fc_id = g.fc_id2)) AND ((t.created_time >= g.official_start) AND (t.created_time <= g.official_end))
GROUP BY game_id;

-- PROBLEM STATEMENT 1 - ANSWER FOR PROBLEM STATEMENT 1!
SELECT game_id, sum_tweet / min_played as tweetsPerMinutes
FROM
(
SELECT vq1p1.game_id AS game_id, (UNIX_TIMESTAMP(official_end) - UNIX_TIMESTAMP(official_start)) / 60 AS min_played, vq1p1.sum_tweet AS sum_tweet
FROM virtualq1p1 vq1p1
JOIN game g
  ON g.game_id = vq1p1.game_id
) AS t
ORDER BY tweetsPerMinutes DESC
LIMIT 10;
"

# Runs queries to solve  Problem Statement 2.
impala-shell -q "use training;
-- Problem Statement 2: Rank the games according to number of distinct users tweeting during-game and present the information for the top 10 games, including the number of distinct users for each.
-- PROBLEM STATEMENT 2 - ANSWER FOR PROBLEM STATEMENT 2!
SELECT COUNT(distinct user_id) AS users, game_id 
FROM tweet t
 JOIN tweet_hashtag th
 ON th.tweet_id = t.tweet_id
 JOIN hashtag_fc hfc
 ON hfc.hashtag_id = th.hashtag_id
 JOIN game g
WHERE ((hfc.fc_id = g.fc_id1) 
       OR (hfc.fc_id = g.fc_id2)) 
       AND ((t.created_time >= g.official_start) 
            AND (t.created_time <= g.official_end))
GROUP BY game_id
ORDER BY users DESC 
LIMIT 10;
"

# Runs queries to solve  Problem Statement 3.
impala-shell -q "use training;
-- Problem Statement 3: Find the top 3 teams that played in the most games. Rank their games in order of highest number of during-game tweets (include the frequency in your output).
-- PROBLEM STATEMENT 3 - CREATING VIEW TO SAVE RESULTS OF GAMES THAT HAVE BEEN PLAYED.
CREATE VIEW virtualq3p1 AS
SELECT name, COUNT(game_id) AS numb_of_games
FROM fc f
JOIN game g
WHERE (f.fc_id = g.fc_id1 OR f.fc_id = g.fc_id2)
GROUP BY name;

-- PROBLEM STATEMENT 3 - ANSWER FOR PROBLEM STATEMENT 3!
SELECT vq3p1.name AS name, vq3p1.numb_of_games AS numb_of_games, COUNT(t.tweet_id) AS numb_of_tweets
FROM virtualq3p1 vq3p1
JOIN tweet t
JOIN tweet_hashtag th
ON th.tweet_id = t.tweet_id
JOIN hashtag_fc hfc
ON hfc.hashtag_id = th.hashtag_id
JOIN game g
JOIN fc f
WHERE ((hfc.fc_id = g.fc_id1) 
     OR (hfc.fc_id = g.fc_id2)) 
     AND (hfc.fc_id = f.fc_id) 
     AND (vq3p1.name = f.name) 
     AND ((t.created_time >= g.official_start) 
     AND (t.created_time <= g.official_end))
GROUP BY name, numb_of_games
ORDER BY numb_of_tweets DESC 
LIMIT 3;

-- Rank their games in order of highest number of 'during game' tweets
-- This one is for Manchester United.
SELECT COUNT (tweet_id) AS TweetsPerMatch, game_id
FROM tweet, game, fc
WHERE (created_time >= official_start
       AND created_time <= official_end
       AND (fc_id = fc_id1
       OR fc_id = fc_id2))
       AND name = 'Manchester United'
GROUP BY game_id
ORDER BY TweetsPerMatch DESC;

-- Rank their games in order of highest number of 'during game' tweets
-- This one is for Liverpool.
SELECT COUNT (tweet_id) AS TweetsPerMatch, game_id
FROM tweet, game, fc
WHERE (created_time >= official_start
       AND created_time <= official_end
       AND (fc_id = fc_id1
       OR fc_id = fc_id2))
       AND name = 'Liverpool'
GROUP BY game_id
ORDER BY TweetsPerMatch DESC;

-- Rank their games in order of highest number of 'during game' tweets
-- This one is for Newcastle.
SELECT COUNT (tweet_id) AS TweetsPerMatch, game_id
FROM tweet, game, fc
WHERE (created_time >= official_start
       AND created_time <= official_end
       AND (fc_id = fc_id1
       OR fc_id = fc_id2))
       AND name = 'Newcastle'
GROUP BY game_id
ORDER BY TweetsPerMatch DESC;
"

# Runs queries to solve  Problem Statement 4.
impala-shell -q "use training;
-- Problem Statement 4: Find the top 10 (ordered by number of tweets) games which have the highest during-game tweeting spike in the last 10 minutes of the game.
-- PROBLEM STATEMENT 4 - CREATING VIEW TO SAVE SPIKES AND TWEETS RESULT PER GAME.
CREATE VIEW virtualq4p1 AS
SELECT COUNT(t.tweet_id) AS tweets_per_minute ,(HOUR(t.created_time) - HOUR(g.official_start)) * 60 + (MINUTE(t.created_time) - MINUTE(g.official_start)) AS spike ,game_id
FROM tweet t 
 JOIN tweet_hashtag th
 ON th.tweet_id = t.tweet_id
 JOIN hashtag_fc hfc
 ON hfc.hashtag_id = th.hashtag_id
 JOIN game g
WHERE ((hfc.hashtag_id = g.fc_id1) OR (hfc.hashtag_id = g.fc_id2)) 
  AND (t.created_time >= DATE_ADD(g.official_end, INTERVAL -10 MINUTE) 
  AND t.created_time <= g.official_end)
GROUP BY game_id, spike
ORDER BY tweets_per_minute DESC;

-- PROBLEM STATEMENT 4 - CREATING VIEW TO SAVE MAX SPIKE RESULT PER GAME.
CREATE VIEW virtualq4p2 AS
SELECT MAX(tweets_per_minute), spike, game_id
FROM virtualq4p1
GROUP BY tweets_per_minute ,spike, game_id
ORDER BY tweets_per_minute DESC;

-- PROBLEM STATEMENT 4 - ANSWER FOR PROBLEM STATEMENT 4!
SELECT game_id, spike, _c0 AS tweets
FROM 
(
SELECT v.*, ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY _c0 DESC) AS seqnum
FROM virtualq4p2 v
) v
WHERE seqnum = 1
ORDER BY tweets DESC
LIMIT 10;
"

# Runs a query + saves the query results in a CSV-file (Problem Statement 5).
impala-shell -q "use training;
-- Problem Statement 5: As well as the official hashtags, each tweet may be labelled with other hashtags. Restricting the data to during-game tweets, list the top 10 most common non-official hashtags over the whole dataset with their frequencies.
-- PROBLEM STATEMENT 5 - CREATES A VIEW THAT HAVE OFFICIAL_HASHTAGS.
CREATE VIEW virtualq5p1 AS
SELECT th.tweet_id , th.hashtag_id
  FROM tweet_hashtag th
 WHERE exists (SELECT 1
                FROM hashtag_fc hfc
                WHERE th.hashtag_id = hfc.hashtag_id);

-- PROBLEM STATEMENT 5 - ANSWER FOR PROBLEM STATEMENT 5!
-- SELECTS ALL TWEETS THAT HAS AN UNOFFICIAL_HASHTAG BUT ALSO HAVE AN OFFICIAL_HASHTAG.
-- THE OFFICIAL HASHTAG DOES NOT COUNT.
-- IT COUNTS ALL HASHTAGS FROM THESE TWEETS AND SUMS THEM TOGETHER AND GROUP THEM PER HASHTAG (ONLY DURING GAME).
CREATE VIEW virtualq5p2 AS 
SELECT th.hashtag_id,
       COUNT(th.hashtag_id) AS count_hashtags
  FROM tweet_hashtag th
  JOIN tweet t
    ON t.tweet_id = th.tweet_id
  JOIN virtualq5p1 vq5p1
    ON vq5p1.tweet_id = th.tweet_id
  JOIN hashtag_fc fc
    ON fc.hashtag_id = vq5p1.hashtag_id
  JOIN game g
    ON g.fc_id1 = fc.fc_id
    OR g.fc_id2 = fc.fc_id 
  WHERE NOT EXISTS (SELECT 1
                     FROM virtualq5p1 vq5p3
                    WHERE vq5p3.hashtag_id = th.hashtag_id
                      AND vq5p3.tweet_id = th.tweet_id)
   AND t.created_time >= g.official_start
   AND t.created_time <= g.official_end
GROUP BY th.hashtag_id
ORDER BY COUNT(th.hashtag_id) DESC
LIMIT 10;

SELECT vq5p2.hashtag_id, vq5p2.count_hashtags, ht.message
FROM hashtag ht
JOIN virtualq5p2 vq5p2
ON vq5p2.hashtag_id = ht.hashtag_id;
"

# Runs a query + saves the query results in a CSV-file (Problem Statement 6).
impala-shell -B -o SQL_Problem_Statement_6.csv --output_delimiter=',' -q "use training;
-- Problem Statement 6: Draw the graph of the progress of one of the games (the game you choose should have a complete set of tweets for the entire duration of the game). It may be useful to summarize the tweet frequencies in 1-minute intervals.
-- PROBLEM STATEMENT 6 - ANSWER FOR PROBLEM STATEMENT 6!
SELECT COUNT(t.tweet_id), (hour(created_time) - hour(g.official_start)) * 60 + (MINUTE(created_time) - MINUTE(g.official_start)) AS minutes
FROM tweet t
  JOIN tweet_hashtag th
    ON th.tweet_id = t.tweet_id
  JOIN hashtag_fc hfc
    ON hfc.hashtag_id = th.hashtag_id
  JOIN game g
    ON (hfc.fc_id = g.fc_id1
    OR hfc.fc_id = g.fc_id2)
WHERE g.game_id = 47
  AND t.created_time <= g.official_end
  AND t.created_time >= g.official_start
GROUP BY minutes
ORDER BY minutes ASC;
"
printf "\n"
printf "Done!\n\n" 
printf "Check your desktop for the SQL CSV-file.\n"

printf "Going further with Extra marks...\n"

echo "Starting in 5..."
sleep 1

echo "4..."
sleep 1

echo "3..."
sleep 1

echo "2..."
sleep 1

echo "1..."
sleep 1

printf "NOW STARTING WITH EXTRA MARKS! \n"
printf "Extra marks 1: Who Won the League? \n"
sleep 1

# Creates a view that shows how many goals there were made per team during the whole league:
impala-shell -q "use training;
CREATE VIEW bonus1 AS
SELECT fc, COUNT(fc) as goals 
FROM game_goals 
GROUP BY fc
ORDER BY goals DESC;

-- Adds an extra column to show which team scored the most during the league.
SELECT name, fc.fc_id, goals
FROM fc
JOIN bonus1 x
ON x.fc = fc.fc_id 
GROUP BY fc.fc_id, name, goals
ORDER BY goals DESC;

-- Shows last games that have been played (final / loser-final).
SELECT * FROM game
ORDER BY official_end DESC; 

-- Shows the goals in the final.
SELECT *
FROM game_goals
WHERE game_id = 48;
"

printf "\n"
echo " PREMIER LEAGUE RANK
1ST - MANCHESTER CITY
2ND - MANCHESTER UNITED
3RD - ???

GOING FURTHER WHO ENDED AS THIRD...
"

impala-shell -q "use training;
SELECT *
FROM game_goals
WHERE game_id = 47
"
printf "It's a tie, so in my opinion Liverpool won since Liverpool has 13 goals during the league and Everton only 10."

printf "\nScript is done!"
printf "\nGoing back to Run.sh..."
/home/training/Desktop/Run.sh
