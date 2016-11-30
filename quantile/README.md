# Spark Approximate Quantiles Example

Greenwald-Khanna algorithm used to calculate approximate quantiles: http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf

Codes adapted from: https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html

**quantilesExample.py** contains an example of approximate quantiles applied to a songs dataset. Spark approxQuantile() function is applied to the "tempo" (time) column presented at the songs database. First, it is calculated the max (100%), min (0%) and median (50%) approximate quantiles with a maximum allowed error of 1%. Results are used to approximate average, variance and standard deviation based on the method available at: https://goo.gl/Ow6TgB. After, some quantile values (corresponding to 50%, 90%, 99%, 100%) and their associated execution times are gathered considering various different errors.

**quantilesDataPlot.r** contains code to plot approximate quantile outputs associated with different error values.

**quantilesTimePlot.r** contains code to plot approximate quantile time execution associated with different error values.
