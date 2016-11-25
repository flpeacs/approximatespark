# Approximate Spark Samples

This repository contains some applications that demonstrate some Spark approximate tools.

**/quantile** contains an example of approximate quantiles applied to a songs dataset. Spark approxQuantile() function is applied to the "tempo" (time) column presented at the songs database. First, it is calculated the max (100%), min (0%) and median (50%) approximate quantiles with a maximum allowed error of 1%. Approximate results are used to approximate average, variance and standard deviation based on the method available at:. After, some quantiles values (corresponding to 50%, 90%, 99%, 100%) and execution time are gathered considering various different error values. Two plots are defined: the first one contains quantiles calculation based on the error associated, the second one contains a plot comparing execution time and the error associated. 

**/hyperloglog** contains an example of counting distinct elements using HyperLogLog applied to a songs dataset. Spark approxCountDistinct() function is applied to the "tempo" (time) column presented at the songs database. First, it is gathered some approximate counting of distinct time values and execution time associated with an error value. Two plots are defined: first constains a comparison of approximate counting associated with different error associated, second contains a comparison between execution time and the errors associated.

**/flights** contains some examples of approximate quantiles and approximate counting applied to a delayed flights dataset. Spark approxQuantile() and approxCountDistinct() are applied to the "delay" column presented at the delay flights database. Similar to above examples, outputs and execution time are gathered and printed as output. After, a binary search is implemented to answer to the question: "what is the percentage of flights that delayed more than X minutes to depart?", results are shown based on a calculation associated with various error values.

**/twitter** contains an example of tweets streaming on Spark. The main goal of this example is to get an ideia how we should proceed to get streaming data that is going to be presented to the approximation methods. Current application calculates the top hashtags on Twitter given a time window. 

To run above examples, go to [www.databricks.com], sign up for a community account, paste the codes on Databricks notebooks and run them on a Spark 2.0+ cluster (for the first 3 applications) or Spark 1.6.2 (for Twitter application).
