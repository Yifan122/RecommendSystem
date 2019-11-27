import org.apache.spark.sql.SparkSession

object StatisticAlgorithm {
  val RATING_RANK_COLLECTION_NAME = "RatingNumberMovie"

  /**
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

}
