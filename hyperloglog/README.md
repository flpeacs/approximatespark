# Spark HyperLogLog Example

HyperLogLog theory: https://en.wikipedia.org/wiki/HyperLogLog

Codes adapted from: https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html

**hyperExample.py**  contains an example of counting distinct elements using HyperLogLog applied to a songs dataset. Spark approxCountDistinct() function is applied to the "tempo" (time) column presented at the songs database. First, it is gathered some approximate counting of distinct "time" values and execution time, associated with an error value. Two plots are defined: first constains a comparison of approximate counting associated with different error values, second contains a comparison between execution time and different error values.

**hyperDataPlot.r** contains code to plot of HyperLogLog outputs on different error values.

**hyperTimePlot.r** contains code to plot of HyperLogLog time execution on different error values.
