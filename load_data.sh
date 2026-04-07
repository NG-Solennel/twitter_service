#!/bin/bash
# load_data.sh - Run on the EC2 instance after downloading ETL output
# Usage: bash load_data.sh

set -e

MYSQL_CMD="sudo mysql --local-infile=1 twitter"

echo "Enabling local infile..."
sudo mysql -e "SET GLOBAL local_infile=1;"

echo "Creating schema..."
sudo mysql < /tmp/schema.sql

echo "Loading users..."
for f in /tmp/etl-data/users/part-*; do
  $MYSQL_CMD -e "LOAD DATA LOCAL INFILE '$f'
    INTO TABLE users
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    (user_id, screen_name, description);"
done
echo "Users loaded."

echo "Loading contacts..."
for f in /tmp/etl-data/contacts/part-*; do
  $MYSQL_CMD -e "LOAD DATA LOCAL INFILE '$f'
    INTO TABLE contacts
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    (user_id, contacted_user_id, reply_count, retweet_count);"
done
echo "Contacts loaded."

echo "Loading contact_tweets..."
for f in /tmp/etl-data/contact_tweets/part-*; do
  $MYSQL_CMD -e "LOAD DATA LOCAL INFILE '$f'
    INTO TABLE contact_tweets
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    (user_id, contacted_user_id, tweet_type, tweet_text, all_tweet_texts);"
done
echo "Contact tweets loaded."

echo "Loading user_hashtags..."
for f in /tmp/etl-data/user_hashtags/part-*; do
  $MYSQL_CMD -e "LOAD DATA LOCAL INFILE '$f'
    INTO TABLE user_hashtags
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    (user_id, hashtag, count);"
done
echo "User hashtags loaded."

echo "Verifying row counts..."
sudo mysql twitter -e "
  SELECT 'users' as tbl, COUNT(*) as cnt FROM users
  UNION ALL
  SELECT 'contacts', COUNT(*) FROM contacts
  UNION ALL
  SELECT 'contact_tweets', COUNT(*) FROM contact_tweets
  UNION ALL
  SELECT 'user_hashtags', COUNT(*) FROM user_hashtags;
"

echo "All done!"
