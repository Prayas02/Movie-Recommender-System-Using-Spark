from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2"))
    # pairScores.show()

    # Compute numerator, denominator and numPairs columns
    pairScores.createOrReplaceTempView("pairScores")
    calculateSimilarity = spark.sql("\
    SELECT\
    movie1,\
    movie2,\
    SUM(xy) AS numerator,\
    (SQRT(SUM(xx)) * SQRT(SUM(yy))) AS denominator,\
    COUNT(xy) AS numPairs\
    FROM\
    pairScores\
    GROUP BY\
    movie1,\
    movie2")
    # calculateSimilarity.show()

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result

# Get movie name by given movie id
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])

moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])


# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("paste your u.item path here")

# Load up movie data as dataset
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("paste your u.data path here")


ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs

ratings.createOrReplaceTempView("ratings")

moviePairs=spark.sql("SELECT ratings1.movieId AS movie1,\
                        ratings2.movieId AS movie2,\
                        ratings1.rating AS rating1,\
                        ratings2.rating AS rating2 \
                        FROM ratings AS ratings1 \
                        JOIN ratings AS ratings2 \
                        ON ratings1.userId = ratings2.userId \
                        AND ratings1.movieId < ratings2.movieId")
# moviePairs.show()

moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

# moviePairSimilarities.show()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
            similarMovieID = result.movie2

        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))


