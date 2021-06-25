rm(list=ls())

library('xtable')
dir <- "C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data"
setwd(dir)

# Use the dataset where maturity <= 10y because it's the most inclusive.
data_120 <- read.csv("outstanding_regdata_usd_120m_2021-04-19.csv",
                     stringsAsFactors = TRUE, 
                    row.names = 1,
                    header = TRUE)
mu_120 <- mean(data_120$DV, na.rm = TRUE)
std_120 <- sd(data_120$DV, na.rm = TRUE)
data_120['DV2'] <- (data_120['DV'] - mu_120) / std_120
mu_120usd <- mean(data_120$DV_USD, na.rm = TRUE)
std_120usd <- sd(data_120$DV_USD, na.rm = TRUE)
data_120['DV_USD2'] <- (data_120['DV_USD'] - mu_120usd) / std_120usd
# filter for polity score (democracies with regular elections)
data_120 <- data_120[data_120$polity2 >= 5,]
# filter for nonzero amt mat, because zero values are something else entirely
data_120 <- data_120[(data_120['Amt.Mat'] > 0),]

cols <- c('Months.To.Election', 'Amt.Issued', 'Amt.Out', 'Amt.Mat', 'DV',
          'vote_margin', 'exec_only_round', 'exec_first_round',
          'exec_final_round', 'exec_vote_share', 
          'democ', 'autoc', 'polity2', 'parcomp', 'US_FFR', 'cr'
          )
df <- data_120[cols]

# construct a df with descriptive statistics.
stats <- c('Variable', 'Min', 'Max', 'Median', 'Mean', 'SD')
ds <- data.frame(matrix(ncol = length(stats), nrow = length(cols)))
names(ds) <- stats
ds$variable <- cols

meanlist <- c()
medlist <- c()
minlist <- c()
maxlist <- c()
sdlist <- c()
for (col in cols) {
  temp <- df[,col]
  temp <- temp[temp != -999]
  minlist <- append(minlist, min(temp, na.rm = TRUE))
  maxlist <- append(maxlist, max(temp, na.rm = TRUE))
  meanlist <- append(meanlist, mean(temp, na.rm = TRUE))
  medlist <- append(medlist, median(temp, na.rm = TRUE))
  sdlist <- append(sdlist, sd(temp, na.rm = TRUE))
}

ds$mean <- meanlist
ds$median <- medlist
ds$min <- minlist
ds$max <- maxlist
ds$sd <- sdlist
# options( scipen = 0 )
options( digits = 0 )
options( scipen = 0 )
print(ds)

# output a table for the write-up.
print(xtable(ds, digits = -2), include.rownames = FALSE)
# copy and paste the rows here for the numbers that should be in sci notation.
print(xtable(ds, digits = 2), include.rownames = FALSE)
