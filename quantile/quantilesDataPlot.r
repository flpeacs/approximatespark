# Code based on https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html
# Plot approx quantile outputs for each target residual value 
library(magrittr)
library(ggplot2)
library(reshape2)

rd <- sqlContext %>% sql("select * from p4") %>% collect
molten.rd <- melt(rd, id.vars = c("time", "rsds"))
 
p <- ggplot(molten.rd, aes(rsds, value, color = variable))
p <- p + geom_line()
p <- p + labs(x = "Target Residual", y = "Quantile")
p <- p + scale_x_continuous(breaks = c(1e-5, 1e-4, .001, .01, 0.1), trans="log10", minor_breaks=NULL)
p <- p + scale_y_continuous(breaks = c(1, 10, 100, 1e3, 1e4, 1e5), trans="log10")
p <- p + scale_colour_discrete(name="Estimated\npercentiles",
                         breaks=c("q50", "q90", "q99", "q100"),
                         labels=c("50%", "90%", "99%", "100%"))
p

