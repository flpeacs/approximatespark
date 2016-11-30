# Spark Approximate Quantiles and Approximate Counting applied to a delayed flights dataset

Code was adapted from https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html

**flights.py** contains some examples of approximate quantiles and approximate distinct counting applied to a delayed flights dataset. Spark approxQuantile() and approxCountDistinct() are applied to the "delay" column presented at the delayed flights database. Similar to the other examples, outputs and execution time are gathered and printed out as output associated with different error values. After, a binary search associated with approximate quantiles method is implemented to answer to the question: "what is the percentage of flights that exceeded more than X minutes to depart?" and results are shown based on the answer associated with different error values.

