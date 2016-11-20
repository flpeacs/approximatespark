# Code based on https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html
# Plot approx count outputs for each target residual value 
library(magrittr)
library(ggplot2)
library(reshape2)

rd <- sqlContext %>% sql("select * from p5") %>% collect
molten.rd <- melt(rd, id.vars = c("time", "rsds"))

p <- ggplot(molten.rd, aes(rsds, value, color = variable))
p <- p + geom_line()
p <- p + labs(x = "Target Residual", y = "Quantile")
p <- p + scale_x_continuous(breaks = c(0.0, 0.02, 0.04, 0.06, 0.08, 0.1), trans="identity", minor_breaks=NULL)
p <- p + scale_y_continuous(breaks = c(20000, 22000, 24000, 26000, 28000, 30000), trans="identity")
p
