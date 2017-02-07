import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.SparkSession

object ParseApp {
	def main(args: Array[String]) {
		val spark = SparkSession
      .builder()
      .appName("Amazon Movies")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

	import spark.implicits._

		val file = spark.sparkContext.textFile("movies.txt")

		val rowRDD = file.map(_.split(",")).map(attributes => (attributes(0).toDouble, attributes(1).length.toDouble))

		val avgHelp = rowRDD.map({case(x,y) => x}).mean()
		val avgText = rowRDD.map({case(x,y) => y}).mean()

		val cov = rowRDD.map({case(x,y) => (x - avgHelp) * (y - avgText)}).sum()

		val varX = math.sqrt(rowRDD.map({case(x,y) => (x - avgHelp) * (x - avgHelp)}).sum())
		val varY = math.sqrt(rowRDD.map({case(x,y) => (y - avgText) * (y - avgText)}).sum())

		println(cov / (varX * varY))
  	}
}




