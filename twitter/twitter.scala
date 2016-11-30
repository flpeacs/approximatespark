// This example was copied and adapted from https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/03%20Twitter%20Hashtag%20Count%20-%20Scala.html

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.math.Ordering

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

System.setProperty("twitter4j.oauth.consumerKey", "8mVJWeXj8segROLX308pRqYBI")
System.setProperty("twitter4j.oauth.consumerSecret", "cKXKpm36TBmGl5asIMQeDz8hrPFPDXCc5U0p2wkqfDCBW16PiM")
System.setProperty("twitter4j.oauth.accessToken", "307507444-IrZnQKGiyGqJYi9npNKr4nONfB0pdfjUFzFQSxPO")
System.setProperty("twitter4j.oauth.accessTokenSecret", "C0VyaUpE7SNu9eGQrn455SPACagM8wIXKmcEbKS528pZW")

val outputFolder = "/twitter" // Folder that contains all top hashtags
val interval = new Duration(1000) // Time to recompute the top hashtags
val window = new Duration(10000) // Break stream into equally time-spaced windows 
val timeout = 100000 // Time to wait before stopping the stream

// Remove everything inside outputFolder 
dbutils.fs.rm(outputFolder, true)

var newContextCreated = false
var num = 0

// This is a helper class used for sorting values
object SecondValueOrdering extends Ordering[(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def streamingFunc(): StreamingContext = {   
  // Create a Spark Streaming Context
  val ssc = new StreamingContext(sc, interval)
  
  // Create a Twitter Stream for the input source. 
  val twitter = TwitterUtils.createStream(ssc, Some(new OAuthAuthorization(new ConfigurationBuilder().build())))
    
  // Parse the tweets and gather the hashTags.
  val hashTag = twitter.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
  
  // Compute the counts of each hashtag by window.
  val windowedhashTagCountStream = hashTag.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, window, interval)

  // For each window, calculate the top hashtags for that time period.
  windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
    val topEndpoints = hashTagCountRDD.top(20)(SecondValueOrdering)
    
    dbutils.fs.put(s"${outputFolder}/top_hashtags_${num}", topEndpoints.mkString("\n"), true)
    println(s"TOP HASHTAGS for window ${num}")
    println(topEndpoints.mkString("\n"))
    num = num + 1
  })
  
  newContextCreated = true
  ssc
}

@transient val ssc = StreamingContext.getActiveOrCreate(streamingFunc)

ssc.start()
ssc.awaitTerminationOrTimeout(timeout)

StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }


