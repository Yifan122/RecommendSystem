import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}

object StatisticAlgorithm {
  val RATING_RANK_COLLECTION_NAME = "RatingNumberMovie"
  val RATING_RANK_BY_MONTH_COLLECTION_NAME = "RankMovieByMonth"
  val GENRES_TOP_MOVIES = "GenresTopMovies"
  val MOVIE_WITH_SCORE = "MovieWithScore"


  /**
   * Get he popularity of movies
   * Calculate the rating number for each movie, and order them in descending order
   *
   * @param spark Spark Session for access to the tmp view
   * @param config
   */
  def rateRank(spark: SparkSession)(implicit config: MongoConfig): Unit = {
    val result = spark.sql("select mid, count(1) as count from ratings group by mid order by count desc")
    result.write
      .option("uri", config.uri)
      .option("collection", RATING_RANK_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
   * Get he popularity of movies by month
   * It will calculate the number of ratings of each movie by every month
   * and then order it in descending order
   *
   * @param spark
   * @param config
   */
  def rateRankByMonth(spark: SparkSession)(implicit config: MongoConfig): Unit = {
    val sdf = new SimpleDateFormat("yyyyMM")
    spark.udf.register("convertTimestamp", (timeStamp: Long) => {
      sdf.format(new Date(timeStamp * 1000L))
    })

    // Covert the timestamp to yyyyMM
    val newRatings = spark.sql("select mid, score, convertTimestamp(timeStamp) as time from ratings")
    // create new view
    newRatings.createOrReplaceTempView("newRatings")
    val result = spark.sql("select mid, count(1) as count, time from newRatings group by mid, time order by time desc, count desc")

    result.write
      .option("uri", config.uri)
      .option("collection", RATING_RANK_BY_MONTH_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
   * Get the top movie in each genres
   *
   * @param spark  Spark Session
   * @param genres Genres List
   * @param movies Movie Datasets
   * @param config MongoDB configuration
   */
  def rankMovieByGenre(spark: SparkSession, genres: List[String])(movies: Dataset[Movie])(implicit config: MongoConfig): Unit = {
    // Calculate the average score for each movie
    val averageMovieScoreDF = spark.sql("select mid, avg(score) as avg from ratings group by mid").cache()

    val moviesWithScoreDF = averageMovieScoreDF.join(movies, Seq("mid", "mid")).select("mid", "avg", "genres").cache()

    val genresRDD = spark.sparkContext.makeRDD(genres)

    // get the genre of each movie
    // fetch the top 10 movies
    import spark.implicits._
    val result = genresRDD.cartesian(moviesWithScoreDF.rdd).filter {
      case (genre, row) => {
        row.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
    }
      .map {
        case (genre, row) => {
          (genre, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
        }
      }
      .groupByKey()
      .map {
        case (genre, items) => {
          GenresRecommendations(genre, items.toList.sortWith(_._2 > _._2).take(10).map(x => Recommendation(x._1, x._2)))
        }
      }.toDF

    result.write
      .option("uri", config.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    moviesWithScoreDF.write
      .option("uri", config.uri)
      .option("collection", MOVIE_WITH_SCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


  }

}
