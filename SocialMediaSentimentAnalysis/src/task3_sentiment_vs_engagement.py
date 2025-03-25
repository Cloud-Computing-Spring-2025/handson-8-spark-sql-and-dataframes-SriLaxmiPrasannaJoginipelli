

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Step 1: Categorize Sentiment
posts_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Step 2: Compute average likes and retweets per Sentiment category
sentiment_stats = (
    posts_df.groupBy("Sentiment")
    .agg(
        avg("Likes").alias("Avg Likes"),
        avg("Retweets").alias("Avg Retweets")
    )
    .orderBy(col("Avg Likes").desc())  # Rank by engagement
)

# Step 3: Save the result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)

print("✅ Sentiment analysis completed. Results saved in 'outputs/sentiment_engagement.csv'")

