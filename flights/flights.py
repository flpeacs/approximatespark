import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math

# Code to parse the dataset available at https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html
# Set File Paths
tripdelaysFilePath = "/databricks-datasets/flights/departuredelays.csv"
airportsnaFilePath = "/databricks-datasets/flights/airport-codes-na.txt"

# Obtain airports dataset
airportsna = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true', delimiter='\t').load(airportsnaFilePath)
airportsna.registerTempTable("airports_na")

# Obtain departure Delays data
departureDelays = sqlContext.read.format("com.databricks.spark.csv").options(header='true').load(tripdelaysFilePath)
departureDelays.registerTempTable("departureDelays")
departureDelays.cache()

# Available IATA codes from the departuredelays sample dataset
tripIATA = sqlContext.sql("select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
tripIATA.registerTempTable("tripIATA")

# Only include airports with atleast one trip from the departureDelays dataset
airports = sqlContext.sql("select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
airports.registerTempTable("airports")
airports.cache()

# Build `departureDelays_geo` DataFrame
#  Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)  
departureDelays_geo = sqlContext.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination") 

# RegisterTempTable
departureDelays_geo.registerTempTable("departureDelays_geo")

# Cache and Count
departureDelays_geo.cache()
departureDelays_geo.count()

# Create Vertices (airports) and Edges (flights)
tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
tripEdges = departureDelays_geo.select("tripid", "delay", "src", "dst")
tripEdges.registerTempTable("tripEdges")

# Cache Vertices and Edges
tripEdges.cache()
tripVertices.cache()

# Define an airport as example
airport = 'ATL'

# Determine approx quantile errors
errors = [0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.9]
table = [["Error", "Time", "min, max, median"]]

a = spark.sql("SELECT delay FROM tripEdges WHERE src = '" + airport + "'").toDF("delay").cache()

# Calculate max, min and median delays of example airport for various different approx error values
for i in range(len(errors)):
  table.append([])
  
  start = time.time()
  
  b = a.approxQuantile("delay", [0.0, 1.0, 0.5], errors[i])
  
  c = time.time() - start

  table[i + 1].append(errors[i])
  table[i + 1].append(c)
  table[i + 1].append(b)

print
# Print information about approx error, execution time, min, max and median
for i in range(len(table)):
  print table[i]

table = [["Error", "Time", "Number of airports"]]  

# Determine approx count errors
errors = [0.01, 0.02, 0.03, 0.05, 0.1, 0.15, 0.2]
  
a = spark.sql("SELECT src FROM tripEdges").toDF("src").cache()  

# Calculate approx count of different airports available
for i in range(len(errors)):
  table.append([])
  
  start = time.time()  
  
  b = a.select(approxCountDistinct("src", errors[i]))

  c = time.time() - start

  table[i + 1].append(errors[i])
  table[i + 1].append(c)
  table[i + 1].append(b.take(1)[0][0])

print
# Print information about approx error, execution time and approx number of different airports
for i in range(len(table)):
  print table[i]

# Define a maxDelay threshold that is going to be used to calculate the percentage of delayed flights that exceed more than maxDelay minutes to depart
maxDelay = 100

a = spark.sql("SELECT delay FROM tripEdges WHERE src = '" + airport + "'").toDF("delay").cache()

# Determine approx quantile errors
errors = [0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.9]
table = [["Error", "Time", "Percentage of delayed flights that exceed more than maxDelay minutes to depart"]]

for i in range(len(errors)):
  table.append([])
  
  start = time.time()  
  
  l = 0.00
  r = 1.0
  it = 10000

  # Calculate the percentage of delayed flights that exceed more than maxDelay minutes to depart using binary search and approx Quantile
  while l < r and r - l > 0.00001 and it > 0:
    m = (l + r) / 2.0
      
    b = a.approxQuantile("delay", [m], errors[i])[0]
    
    if b < maxDelay:
      l = m
    else:
      r = m
    
    it = it - 1
    
  c = time.time() - start
  
  table[i + 1].append(errors[i])
  table[i + 1].append(c)
  table[i + 1].append(1.0 - l)

# Print information about the percentage of delayed flights that exceed more than maxDelay minutes to depart
print
for i in range(len(table)):
  print table[i]
