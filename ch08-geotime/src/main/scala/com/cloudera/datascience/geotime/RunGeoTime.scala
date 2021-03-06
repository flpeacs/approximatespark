/*
 * Copyright 2015 and onwards Sanford Ryza, Juliet Hougland, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.geotime

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import com.esri.core.geometry.Point
import spray.json._

import com.cloudera.datascience.geotime.GeoJsonProtocol._

class RichRow(row: Row) {
  def getAs[T](field: String): Option[T] =
    if (row.isNullAt(row.fieldIndex(field))) None else Some(row.getAs[T](field))
}

case class Trip(
  license: String,
  pickupTime: Long,
  dropoffTime: Long,
  pickupX: Double,
  pickupY: Double,
  dropoffX: Double,
  dropoffY: Double)

object RunGeoTime extends Serializable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val taxiRaw = spark.read.option("header", "true").csv("taxidata")
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.map(_.left.get)
    taxiGood.cache()	

    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }

    taxiGood.filter(x => x.dropoffX != 0).filter(x => x.dropoffY != 0).filter(x => x.pickupX != 0).filter(x => x.pickupY != 0).filter(x => hours(x.pickupTime, x.dropoffTime) >= 0)

    val geojson = scala.io.Source.fromURL(this.getClass.getResource("/nyc-city_council.geojson")).mkString

    val bFeatures = spark.sparkContext.broadcast(geojson.parseJson.convertTo[FeatureCollection])

    val bLookup = (x: Double, y: Double) => {
      val feature: Option[Feature] = bFeatures.value.find(f => {
        f.geometry.contains(new Point(x, y))
      })
      feature.map(f => {
        f("CounDist").convertTo[String]
      }).getOrElse("NA")
    }
    	    
    val countsByKey = taxiGood.map(p => ((bLookup(p.dropoffX, p.dropoffY).toString, ((p.pickupTime % 86400) / 3600).toString), (p.dropoffTime - p.pickupTime).toDouble)).countByKey

    val boroughDurations = taxiGood.map(p => ((bLookup(p.dropoffX, p.dropoffY).toString, ((p.pickupTime % 86400) / 3600).toString), (p.dropoffTime - p.pickupTime).toDouble)).reduceByKey(_ + _).map({case(a, b) => (a, b / countsByKey(a))}).sortByKey().foreach(println)
	
    taxiGood.unpersist()
  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }

  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat(
       "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)
    optDt.map(dt => formatter.parse(dt).getTime / 60000).getOrElse(0L)
  }

  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }

  def parse(line: Row): Trip = {
    val rr = new RichRow(line)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude")
    )
  }
}
