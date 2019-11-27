import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Load data into MongoDB and ElasticSearch
 */
object Dataloader {
  def main(args: Array[String]): Unit = {
    // TEST file path, change it to your path
    val DATAFILE_MOVIES = "E:\\Users\\Larry\\IdeaProjects\\RecommendSystem\\testfiles\\movies.csv"
    val DATAFILE_RATINGS = "E:\\Users\\Larry\\IdeaProjects\\RecommendSystem\\testfiles\\ratings.csv"
    val DATAFILE_TAGS = "E:\\Users\\Larry\\IdeaProjects\\RecommendSystem\\testfiles\\tags.csv"

    // Parameters
    val params = scala.collection.mutable.Map[String, Any]()
    params += "spark.cores" ->  "local[2]"

    // Spark Configuration
    val config = new SparkConf().setAppName("DataLoader")
      .setMaster(params("spark.cores").asInstanceOf[String])

    val spark = SparkSession.builder().config(config).getOrCreate()

    // load the raw data from the original file
    val movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES)
    val ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)
    val tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)

    // convert the RDD to the DataFrame
    import spark.implicits._
    val movieDF = movieRDD.map(line => {
      val x = line.split("\\^")
      Movie(x(0).trim.toInt, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim, x(7).trim, x(8).trim, x(9).trim)
    }).toDF

    val ratingDF = ratingRDD.map(line => {
      val x = line.split(",")
      Rating(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble, x(3).trim.toInt)
    }).toDF

    val tagDF = tagRDD.map(line => {
      val x = line.split(",")
      Tag(x(0).trim.toInt, x(1).trim.toInt, x(2).trim, x(3).trim.toInt)
    }).toDF

    // show the result
//    movieDF.show(2)
//    ratingDF.show(2)
//    tagDF.show(2)

  }
}
