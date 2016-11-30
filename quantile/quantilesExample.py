import numpy as np
import time
import math
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Parsing extracted from https://docs.cloud.databricks.com/docs/latest/sample_applications/06%20Sample%20Data%20Pipeline/Python/Stage%201:%20ETL.html
def parseLine(line):
  tokens = zip(line.split("\t"), header)
  parsed_tokens = []
  for token in tokens:
    token_type = token[1][1]
    if token_type == 'double':
      parsed_tokens.append(float(token[0]))
    elif token_type == 'int':
      parsed_tokens.append(-1 if '-' in token[0] else int(token[0])) # Taking care of fields with --
    else:
      parsed_tokens.append(token[0])
  return parsed_tokens


def strToType(str):
  if str == 'int':
    return IntegerType()
  elif str == 'double':
    return DoubleType()
  else:
    return StringType()

header = sc.textFile("/databricks-datasets/songs/data-001/header.txt").map(lambda line: line.split(":")).collect()
dataRDD = sc.textFile("/databricks-datasets/songs/data-001/part-000*")
parsedRDD = dataRDD.map(parseLine)
schema = StructType([StructField(t[0], strToType(t[1]), True) for t in header])
dataset = sqlContext.createDataFrame(parsedRDD, schema)
sqlContext.registerDataFrameAsTable(dataset, "songs")
#end Parsing

# Code based on https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html
# Find the exact average and standard deviation of songs time data
sqlContext.sql("select avg(tempo) from songs").show()
sqlContext.sql("select stddev(tempo) from songs").show()

# Create a dataframe that is used to calculate approximate quantile
review_lengths = sqlContext.sql("select tempo from songs").toDF("tempo").cache()

n = review_lengths.count()

# Define quantiles as 1% (min value), 99% (max value) and 50% (median)
quantiles = [0.01, 0.99, 0.5]

# Run approx quantile
l = review_lengths.approxQuantile("tempo", quantiles, 0.01)
a = l[0] # Approx Min Value
b = l[1] # Approx Max Value
m = l[2] # Approx Median

# Calculate and print approx average
print "AVG: ", float(a + 2 * m + b) / 4.0

# Calculate and print approx variance and standard deviation
p1 = 1.0 / float(n - 1.0)
p2 = a * a + m * m + b * b
p3 = float(n - 3.0) / 2.0
p4 = float((a + m) * (a + m) + (b + m) * (b + m)) / 4.0
p5 = n * (float(a + 2 * m + b) / 4.0 + float(a - 2 * m + b) / float(4.0 * n)) * (float(a + 2 * m + b) / 4.0 + float(a - 2 * m + b) / (4 * n))
variance = p1 * (p2 + p3 * p4 - p5)

print "Variance: ", variance
print "SD: ", math.sqrt(variance)

# Now, define quantiles as 50%, 90%, 99% and 100% for plotting purposes
quantiles = [0.5, 0.9, 0.99, 1.0]
quantile_labels = ["q"+str(int(q * 100)) for q in quantiles]

rsds = np.logspace(-1.0, -5.0, num=30)

res = []
times = []

# Define approx quantile based on time variable and calculate execution time
for rsd in rsds:
  start = time.time()
  qs = review_lengths.approxQuantile("tempo", quantiles, rsd)
  dt = time.time() - start
  res.append(qs)
  times.append(dt)

# Save answers for plotting
p0 = pd.DataFrame(res, columns=quantile_labels)
p1 = pd.DataFrame({"rsds" : rsds, "time" : times})
p = pd.concat([p0, p1], axis = 1)
sqlContext.createDataFrame(p).registerTempTable("p4")


