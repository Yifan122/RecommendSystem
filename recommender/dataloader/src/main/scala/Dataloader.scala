import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Load data into MongoDB and ElasticSearch
 */
object Dataloader {
  // MongoDB collections
  val MOVIES_COLLECTION_NAME = "Movie"
  val RATING_COLLECTION_NAME = "Rating"
  val TAG_COLLECTION_NAME = "Tag"

  def main(args: Array[String]): Unit = {
    // TEST file path, change it to your path
    val DATAFILE_MOVIES = "E:\\Users\\Larry\\IdeaProjects\\RecommendSystem\\testfiles\\movies.csv"
    val DATAFILE_RATINGS = "E:\\Users\\Larry\\IdeaProjects\\RecommendSystem\\testfiles\\ratings.csv"
    val DATAFILE_TAGS = "E:\\Users\\Larry\\IdeaProjects\\RecommendSystem\\testfiles\\tags.csv"

    // Parameters
    val params = scala.collection.mutable.Map[String, Any]()
    params += "spark.cores" ->  "local[2]"
    params += "mongo.uri" -> "mongodb://bigdata112:27017/recom"
    params += "mongo.db" -> "recom"

    // generate MongoConfig
    implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String], params("mongo.db").asInstanceOf[String])

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

    // Store the data into MongoDB
    storeDataintoMango(movieDF, ratingDF, tagDF)
  }


  /**
   * Store the data into Mango
   *
   * @param movieDF
   * @param ratingDF
   * @param tagDF
   */
  def storeDataintoMango(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit config: MongoConfig): Unit = {

    // Create mongoDB connection
    val mongoClient = MongoClient(MongoClientURI(config.uri))

    // Drop the collection if it exists
    mongoClient(config.db)(MOVIES_COLLECTION_NAME).dropCollection()
    mongoClient(config.db)(RATING_COLLECTION_NAME).dropCollection()
    mongoClient(config.db)(TAG_COLLECTION_NAME).dropCollection()

    // Write the data to database
    movieDF.write
      .option("uri", config.uri)
      .option("collection", MOVIES_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", config.uri)
      .option("collection", RATING_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", config.uri)
      .option("collection", TAG_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .save()

    // create index
    // MongoDBObject("mid" -> 1)： "mid" is the field, 1 means order ascending
    mongoClient(config.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(config.db)(RATING_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(config.db)(RATING_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(config.db)(TAG_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(config.db)(RATING_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
  }
}
