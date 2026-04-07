-- Twitter Service MySQL Schema
-- Run this on your MySQL instance before loading data

CREATE DATABASE IF NOT EXISTS twitter;
USE twitter;

-- Drop existing tables if re-running
DROP TABLE IF EXISTS contact_tweets;
DROP TABLE IF EXISTS contacts;
DROP TABLE IF EXISTS user_hashtags;
DROP TABLE IF EXISTS users;

-- Latest user info
CREATE TABLE users (
  user_id     BIGINT PRIMARY KEY,
  screen_name VARCHAR(255),
  description TEXT,
  INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Interaction counts per user pair
CREATE TABLE contacts (
  user_id            BIGINT,
  contacted_user_id  BIGINT,
  reply_count        INT DEFAULT 0,
  retweet_count      INT DEFAULT 0,
  PRIMARY KEY (user_id, contacted_user_id),
  INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Contact tweets: latest for display + all texts for keyword scoring
CREATE TABLE contact_tweets (
  user_id            BIGINT,
  contacted_user_id  BIGINT,
  tweet_type         ENUM('reply','retweet'),
  tweet_text         MEDIUMTEXT,      -- latest tweet (for display)
  all_tweet_texts    MEDIUMTEXT,      -- all tweets concatenated with \x01 (for keyword scoring)
  PRIMARY KEY (user_id, contacted_user_id, tweet_type),
  INDEX idx_user_type (user_id, tweet_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Hashtag counts per user (excluding popular hashtags)
CREATE TABLE user_hashtags (
  user_id  BIGINT,
  hashtag  VARCHAR(255),
  count    INT DEFAULT 0,
  PRIMARY KEY (user_id, hashtag),
  INDEX idx_user_id (user_id),
  INDEX idx_hashtag (hashtag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
