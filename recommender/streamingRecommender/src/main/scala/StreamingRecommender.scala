import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingRecommender {
  def main(args: Array[String]): Unit = {
    // Set the general parameters
    val params = scala.collection.mutable.Map[String, Any]()
    params += "spark.cores" -> "local[3]"
    params += "mongo.uri" -> "mongodb://bigdata112:27017/recom"
    params += "mongo.db" -> "recom"
    params += "spark.executor.memory" -> "4g"
    params += "spark.driver.memory" -> "2g"

    // Configure spark
    val conf = new SparkConf().setMaster("SatisticsApp")
      .setMaster(params("spark.cores").asInstanceOf[String])
      .set("spark.executor.memory", params("spark.executor.memory").asInstanceOf[String])
      .set("spark.driver.memory", params("spark.driver.memory").asInstanceOf[String])

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    
  }
}
