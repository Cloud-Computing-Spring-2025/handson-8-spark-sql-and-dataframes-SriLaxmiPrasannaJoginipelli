
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Step 1: Extract and explode hashtags
hashtags_df = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))

# Step 2: Count frequency of each hashtag
hashtag_counts = hashtags_df.groupBy("Hashtag").agg(count("*").alias("Count"))

# Step 3: Sort in descending order and select top 10 hashtags
top_hashtags = hashtag_counts.orderBy(col("Count").desc()).limit(10)

# Step 4: Save result
top_hashtags.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)

print("✅ Hashtag trends analysis completed. Results saved in 'outputs/hashtag_trends.csv'")

