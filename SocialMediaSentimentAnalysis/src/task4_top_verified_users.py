# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum as _sum

# spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# # Load datasets
# posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
# users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# # TODO: Implement the task here

# # Save result
# top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Step 1: Filter only verified users
verified_users_df = users_df.filter(col("Verified") == True)

# Step 2: Join posts with verified users
verified_posts_df = posts_df.join(verified_users_df, "UserID", "inner")

# Step 3: Calculate total reach (Likes + Retweets) per user
reach_df = (
    verified_posts_df.groupBy("Username")
    .agg(_sum(col("Likes") + col("Retweets")).alias("Total Reach"))
    .orderBy(col("Total Reach").desc())  # Sort in descending order
)

# Step 4: Select top 5 verified users
top_verified = reach_df.limit(5)

# Step 5: Save the result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)

print("✅ Top verified users analysis completed. Results saved in 'outputs/top_verified_users.csv'")

