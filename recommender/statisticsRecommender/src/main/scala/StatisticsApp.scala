import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StatisticsApp extends App {
  // MongoDB collections
  val MOVIES_COLLECTION_NAME = "Movie"
  val RATING_COLLECTION_NAME = "Rating"
  val TAG_COLLECTION_NAME = "Tag"

  // All genres
  val ALL_GENRES = List[String]("War", "Fantasy", "Western", "Musical", "Horror", "IMAX", "Crime", "Animation", "Children", "Sci-Fi",
    "Comedy", "Documentary", "Mystery", "Thriller", "Adventure", "Romance", "Drama", "Film-Noir", "Action")


  // Set the general parameters
  val params = scala.collection.mutable.Map[String, Any]()
  params += "spark.cores" -> "local[2]"
  params += "mongo.uri" -> "mongodb://bigdata112:27017/recom"
  params += "mongo.db" -> "recom"

  // Configure spark
  val conf = new SparkConf().setMaster("SatisticsApp").setMaster(params("spark.cores").asInstanceOf[String])
  val spark = SparkSession.builder().config(conf).getOrCreate()

  // generate MongoConfig
  implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String], params("mongo.db").asInstanceOf[String])

  // read the data from mongoDB

  import spark.implicits._

  val ratings = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", RATING_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating].cache()

  val movies = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MOVIES_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie].cache()

  // Create a view for SQL analysis
  ratings.createOrReplaceTempView("ratings")
  movies.createOrReplaceTempView("movies")

  // Calculate the rating number for each movie, and order them in descending order
  // store the result into mongodb
  StatisticAlgorithm.rateRank(spark)

  // Get the top movies according to the popularity by month
  StatisticAlgorithm.rateRankByMonth(spark)

  // Get the top movies according to the score by genres
  StatisticAlgorithm.rankMovieByGenre(spark, ALL_GENRES)(movies)

  spark.stop()

}
