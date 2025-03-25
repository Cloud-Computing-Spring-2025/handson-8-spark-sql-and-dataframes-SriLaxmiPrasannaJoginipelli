# Social Media Sentiment & Engagement Analysis with Spark SQL

## Overview
In order to gather information about user engagement, sentiment, and influence, this project uses **Apache Spark SQL and DataFrames** to analyze social media data. The study makes use of Spark's distributed computing capabilities and is carried out on datasets that contain **social media posts and user information**.

## Dataset
Two CSV files are used:
1. `posts.csv` - Contains post details (content, likes, retweets, hashtags, sentiment)
2. `users.csv` - Contains user demographics (age group, country, verification status)

## Tasks Performed

### 1. Hashtag Trends Analysis
- Extracts and counts individual hashtags from posts
- Identifies top 10 most used hashtags
- **Output:** `outputs/hashtag_trends.csv`

### 2. Engagement by Age Group
- Joins posts with user data
- Calculates average likes/retweets by age group
- **Output:** `outputs/engagement_by_age.csv`

### 3. Sentiment vs Engagement
- Categorizes posts by sentiment (Positive/Neutral/Negative)
- Measures engagement metrics for each sentiment category
- **Output:** `outputs/sentiment_engagement.csv`

### 4. Top Verified Users
- Filters verified accounts
- Ranks by total reach (likes + retweets)
- **Output:** `outputs/top_verified_users.csv`

## How to Run
1. Ensure Spark is installed
2. Run each task with:
```bash
spark-submit src/task1_hashtag_trends.py
spark-submit src/task2_engagement_by_age.py
spark-submit src/task3_sentiment_vs_engagement.py
spark-submit src/task4_top_verified_users.py
