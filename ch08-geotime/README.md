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

Example of output: 

+-------+------+------------------+
|borough|pickup|        avg       |
+-------+------+------------------+
|      1|     0|12.826692045758886|
|      1|     1|12.770052670047395|
|      1|     2|11.595311437372485|
|      1|     3|12.954744036779656|
|      1|     4|12.738297778236447|
|      1|     5|13.145644095788604|
|      1|     6|13.222581079262548|
|      1|    18| 11.70735953927543|
|      1|    19|12.737486864126415|
|      1|    20|11.714717060207454|
|      1|    21|12.910486190734819|
|      1|    22| 13.02640610019444|
|      1|    23|12.977898676806342|
|     10|     0| 18.29158993247391|
|     10|     1|19.242752293577983|
|     10|     2|17.234633319203052|
|     10|     3|18.226008617312964|
|     10|     4| 18.27751677852349|
|     10|     5|18.758107507774323|
|     10|     6|19.057035064358633|
+-------+------+------------------+
