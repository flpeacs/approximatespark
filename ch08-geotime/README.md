# Spark applied to NYC taxi data

Code was adapted from the book Advanced Analytics with Spark, by Juliet Hougland, Uri Laserson, Sean Owen, Sandy Ryza, and Josh Wills.

NYC regions that are being used are available at [here](https://github.com/dwillis/nyc-maps/blob/master/city_council.geojson).

Spark Version: 2.1.0 with Hadoop 2.7

Java Version: 1.8

**src/main/scala/com/cloudera/datascience/geotime/RunGeoTime.scala** calculates the average duration of taxi rides that depart from a given NYC region in a given hour of the day. Code uses dataframes to perform operations.

Running the application:

Download the dataset at [here](http://www.andresmh.com/nyctaxitrips/) 

Unzip the dataset to ```/ch08-geotime/taxidata/```

```mvn package```

```/YOUR_SPARK_HOME/bin/spark-submit target/ch08-geotime-2.0.0-jar-with-dependencies.jar``` 

