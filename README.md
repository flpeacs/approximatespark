# Approximate Spark Samples

This repository contains some applications that demonstrate two Spark approximation tools.

**/quantile** contains an example of approximate quantiles applied to a songs dataset. Spark approxQuantile() function is applied to the "tempo" (time) column presented at the songs database. First, it is calculated the max (100%), min (0%) and median (50%) approximate quantiles with a maximum allowed error of 1%. Results are used to approximate average, variance and standard deviation based on the method available at: https://goo.gl/Ow6TgB. After, some quantile values (corresponding to 50%, 90%, 99%, 100%) and their associated execution times are gathered considering various different errors. Two plots are defined: first contains quantiles calculation based on the error associated, second contains a plot comparing execution time and the error associated. 

**/hyperloglog** contains an example of counting distinct elements using HyperLogLog applied to a songs dataset. Spark approxCountDistinct() function is applied to the "tempo" (time) column presented at the songs database. First, some approximate distinct counting values of "time" column are gathered with their associated execution times, given an error value. Two plots are defined: first contains a comparison between approximate counting and different error values, second contains a comparison between execution time and different error values.

**/flights** contains some examples of approximate quantiles and approximate distinct counting applied to a delayed flights dataset. Spark approxQuantile() and approxCountDistinct() are applied to the "delay" column presented at the delayed flights database. Similar to the other examples, outputs and execution time are gathered and printed out as output associated with different error target values. After, a binary search associated with approximate quantiles method is implemented to answer to the question: "what is the percentage of flights that exceeded more than X minutes to depart?" and results are shown based on the answer associated with different error values.

**/twitter** contains an example of tweets streaming on Spark. The main goal of this example is to get an ideia how we should proceed to get streaming data that may be associated with approximation methods. Current application calculates the top hashtags on Twitter given a time window. 

To run above examples, go to [www.databricks.com], sign up for a community account, paste the codes on Databricks notebooks and run them on a Spark 2.0+ cluster (for the first 3 applications) or Spark 1.6.2 with Hadoop 1 (for Twitter application).

**/ch08-geotime** contains an application that calculates the average duration of taxi rides that depart from a given NYC region in a given hour of the day.

**/pearson** contains an application that calculates the Pearson correlation coefficient between the length of Amazon movies reviews and their helpfulness. 
