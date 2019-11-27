/**
 *
 * @param mid movie ID
 * @param name  movie name
 * @param descri  movie description
 * @param timelong movie duration
 * @param issue issue date
 * @param shoot shoot date
 * @param language language
 * @param genres genres
 * @param actors actors
 * @param directors directors
 */

case class Movie(val mid:Int,val name:String,val descri:String,val timelong:String,
                 val issue:String,val shoot:String,val language: String,
                 val genres:String,val actors:String,val directors:String)

/**
 *
 * @param uid user ID
 * @param mid movie ID
 * @param score score
 * @param timestamp  create time
 */
case class Rating(val uid:Int,val mid:Int,val score:Double,val timestamp:Int)

/**
 *
 * @param uid user ID
 * @param mid movie ID
 * @param tag tags created by the user
 * @param timestamp create time
 */
case class Tag(val uid:Int,val mid:Int,val tag:String,val timestamp:Int)

/**
 * Mongo Configuration
 *
 * @param uri
 * @param db
 */
case class MongoConfig(val uri: String, val db: String)