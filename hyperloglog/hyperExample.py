import numpy as np
import time
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
# Find the count of distinct elements at songs time data
review_lengths = sqlContext.sql("select tempo from songs").toDF("tempo").cache()

n = review_lengths.count()

# Determine approx errors
rsds = np.logspace(-1.0, -2.5, num=20)

# Define approx count based on time variable and calculate execution time
res = []
times = []
for rsd in rsds:
  start = time.time()      
  qs = dataset.select(approxCountDistinct("tempo", rsd)).take(19)[0]
  dt = time.time() - start
  res.append(qs[0])
  times.append(dt)
  
# Save answers for plotting      
p0 = pd.DataFrame(res, columns=["count"])  
p1 = pd.DataFrame({"rsds" : rsds, "time" : times})
p = pd.concat([p0, p1], axis = 1)
sqlContext.createDataFrame(p).registerTempTable("p5")




