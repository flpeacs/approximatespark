# Spark Approximate Quantiles Example

Greenwald-Khanna algorithm used to calculate approximate quantiles: http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf

Codes adapted from: https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html

**quantilesExample.py** contains an example of approximate quantiles applied to a songs dataset. Spark approxQuantile() function is applied to the "tempo" (time) column presented at the songs database. First, it is calculated the max (100%), min (0%) and median (50%) approximate quantiles with a maximum allowed error of 1%. Approximate results are used to approximate average, variance and standard deviation based on the method available at: http://bmcmedresmethodol.biomedcentral.com/articles/10.1186/1471-2288-5-13. After, some quantiles values (corresponding to 50%, 90%, 99%, 100%) and execution time are gathered considering various different error values. Two plots are defined: the first one contains quantiles calculation based on the error associated, the second one contains a plot comparing execution time and the error associated. 

**quantilesDataPlot.r** contains code to plot of approximate quantile outputs on different errors associated.

**quantilesTimePlot.r** contains code to plot of approximate quantile time execution on different errors associated.
