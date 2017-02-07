# Spark applied to Amazon movies review data

Spark Version: 2.1.0 with Hadoop 2.7

Java Version: 1.8

**src/main/scala/com/cloudera/datascience/geotime/RunGeoTime.scala** calculates the Pearson correlation coefficient between the length of Amazon movies reviews and their helpfulness. 

Running the application:

Download the dataset at [here](http://snap.stanford.edu/data/web-Movies.html) 

Unzip the dataset to ```/pearson```

Rename the dataset to ```movies.txt```

```python parser.py > movies.txt```

```sbt package```

```/YOUR_SPARK_HOME/bin/spark-submit target/scala-2.11/amazon-movies_2.11-1.0.jar``` 

