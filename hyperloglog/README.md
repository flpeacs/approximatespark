# Spark HyperLogLog Example

HyperLogLog theory: https://en.wikipedia.org/wiki/HyperLogLog

Codes adapted from: https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html

**hyperExample.py** contains an example of counting distinct elements using HyperLogLog applied to a songs dataset. Spark approxCountDistinct() function is applied to the "tempo" (time) column presented at the songs database. Some approximate distinct counting values of "time" column are gathered with their associated execution times, given an error value.

**hyperDataPlot.r** contains code to plot HyperLogLog outputs associated with different error values.

**hyperTimePlot.r** contains code to plot HyperLogLog time execution associated with different error values.
