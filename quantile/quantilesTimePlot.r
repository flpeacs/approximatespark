# Code based on https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html
# Plot approx quantile execution time for each target residual value 
library(magrittr)
library(ggplot2)
library(reshape2)

rd <- sqlContext %>% sql("select * from p4") %>% collect
molten.rd <- melt(rd, id.vars = c("time", "rsds"))

p <- ggplot(molten.rd, aes(rsds, time))
p <- p + geom_line()
p <- p + labs(x = "Target Residual", y = "Time")
p <- p + scale_x_continuous(breaks = c(1e-5, 1e-4, .001, .01, 0.1), trans="log10", minor_breaks=NULL)
p <- p + scale_y_continuous(breaks = c(0, 0.2, 0.4, 0.6, 0.8, 1.0), trans="identity")
p
