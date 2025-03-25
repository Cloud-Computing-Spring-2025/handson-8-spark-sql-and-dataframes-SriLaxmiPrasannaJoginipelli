# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, col

# spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# # Load datasets
# posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
# users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# # TODO: Implement the task here


# # Save result
# engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Initialize Spark Session
spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Step 1: Join datasets on UserID
joined_df = posts_df.join(users_df, "UserID")

# Step 2: Compute average likes and retweets per AgeGroup
engagement_df = (
    joined_df.groupBy("AgeGroup")
    .agg(
        avg("Likes").alias("Avg Likes"),
        avg("Retweets").alias("Avg Retweets")
    )
    .orderBy(col("Avg Likes").desc())  # Rank by engagement
)

# Step 3: Save the result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)

print("✅ Engagement analysis completed. Results saved in 'outputs/engagement_by_age.csv'")

