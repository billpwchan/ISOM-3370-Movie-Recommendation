from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd

if __name__ == "__main__":
    global probability
    probability = 0.2
    focalUser = [3, 9, 33, 39, 90]
    # Define Spark Context
    spark = SparkSession.builder.master('local').config(
        "spark.driver.cores", 4).appName("ISOM").getOrCreate()

    # Import CSV Data
    ratings = spark.read.format("csv").option(
        "header", "true").load("ratings.csv")
    ratings = ratings.filter(ratings.rating > 3.0)
    ratings.show(n=5)

    ratings_pandas = ratings.toPandas()

    for selectedUser in focalUser:
        users = ratings.select('userId').distinct().rdd.map(
            lambda r: r[0]).collect()
        userDict = {user: 0.0 for user in users}
        userCount = {user: float(ratings_pandas[(
            ratings_pandas.userId == user)].movieId.count()) for user in users}
        # userCount = {user:float(ratings.filter(ratings.userId == user).count()) for user in users}    # RDD Approach

        movies = ratings.select('movieId').distinct().rdd.map(
            lambda r: r[0]).collect()
        movieDict = {movie: 1.0/9125.0 for movie in movies}
        movieCount = {movie: float(ratings_pandas[(
            ratings_pandas.movieId == movie)].userId.count()) for movie in movies}
        # movieCount = {movie: float(ratings.filter(ratings.movieId == movie).count()) for movie in movies} # RDD Approach

        for iteration in range(10):
            for user, similarity in userDict.iteritems():
                likedMovies = ratings_pandas[ratings_pandas.userId == user].movieId
                similaritySum = 0.0
                for movie in likedMovies:
                    relevance = float(movieDict[movie])
                    userNum = float(movieCount[movie])
                    similaritySum = similaritySum + relevance / userNum
                userDict[user] = (1.0 - probability) * similaritySum + \
                    (probability if user == str(selectedUser) else 0.0)
            for movie, relevance in movieDict.iteritems():
                # rater = ratings.filter(ratings.movieId == movie).rdd.map(lambda r: r[0]).collect()
                rater = ratings_pandas[ratings_pandas.movieId == movie].userId
                relevanceSum = 0.0
                for user in rater:
                    similarity = float(userDict[user])
                    movieNum = float(userCount[user])
                    relevanceSum = relevanceSum + similarity / movieNum
                movieDict[movie] = relevanceSum

        pd.DataFrame.from_dict(data=movieDict, orient='index').to_csv(
            'movieRelevance_' + str(int(selectedUser)) + '_Complete.csv')
        pd.DataFrame.from_records(data=sorted(movieDict.items(), key=lambda kv: kv[1], reverse=True)[:20]).to_csv(
            'movieRelevance_' + str(int(selectedUser)) + '_Selected.csv', header=False)
        # pd.DataFrame.from_records(data=sorted(userDict.items(), key=lambda kv: kv[1], reverse=True)[:20]).to_csv(
        #     'userSimilarity_' + str(int(userNum)) + '.csv', header=False)
