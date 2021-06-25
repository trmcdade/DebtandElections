rm(list=ls())

# Function to load packages
loadPkg = function(toLoad){
  for(lib in toLoad){
    if(! lib %in% installed.packages()[,1])
    { install.packages(lib, repos='http://cran.rstudio.com/') }
    suppressMessages( library(lib, character.only = TRUE) ) }
}

#load these libraries
packs = c('ggplot2', 'dplyr', 'scales', 'haven', 'readstata13',
          'tictoc', 'RColorBrewer', 'zoo', 'lmtest', 'margins',
          'stargazer', 'bvartools', 'vars', 'urca', 'reshape2', 
          'dynlm', 'tseries', 'car', 'fUnitRoots', 'panelvar',
          'plm', 'gplots', 'xtable', 'lme4', 'ecm', 'MuMIn', 
          'lmerTest', 'dotwhisker', 'margins', 'glue')

loadPkg(packs)
set.seed(619)
dir <- "C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data"
setwd(dir)

var <- 'exec_only_round'

## for the summed at currency level:
data_24 <- read.csv("cmcm_outstanding_regdata_usd_24m_2021-04-19.csv",
# data_24 <- read.csv("cmcm_outstanding_regdata_usd_24m_2021-03-23.csv",
                     stringsAsFactors = TRUE, 
                     row.names = 1,
                     header = TRUE)
mu_24 <- mean(data_24$DV, na.rm = TRUE)
std_24 <- sd(data_24$DV, na.rm = TRUE)
data_24['DV2'] <- (data_24['DV'] - mu_24) / std_24
mu_24usd <- mean(data_24$DV_USD, na.rm = TRUE)
std_24usd <- sd(data_24$DV_USD, na.rm = TRUE)
data_24['DV_USD2'] <- (data_24['DV_USD'] - mu_24usd) / std_24usd

data_60 <- read.csv("cmcm_outstanding_regdata_usd_60m_2021-04-19.csv",
# data_60 <- read.csv("cmcm_outstanding_regdata_usd_60m_2021-03-23.csv",
                    stringsAsFactors = TRUE, 
                    row.names = 1,
                    header = TRUE)
mu_60 <- mean(data_60$DV, na.rm = TRUE)
std_60 <- sd(data_60$DV, na.rm = TRUE)
data_60['DV2'] <- (data_60['DV'] - mu_60) / std_60
mu_60usd <- mean(data_60$DV_USD, na.rm = TRUE)
std_60usd <- sd(data_60$DV_USD, na.rm = TRUE)
data_60['DV_USD2'] <- (data_60['DV_USD'] - mu_60usd) / std_60usd

data_120 <- read.csv("cmcm_outstanding_regdata_usd_120m_2021-04-19.csv",
# data_120 <- read.csv("cmcm_outstanding_regdata_usd_120m_2021-03-23.csv",
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
data_24 <- data_24[data_24$polity2 >= 5,]
data_60 <- data_60[data_60$polity2 >= 5,]
data_120 <- data_120[data_120$polity2 >= 5,]

# filter for nonzero amt mat, because zero values are something else entirely
data_24 <- data_24[(data_24['Amt.Numerator'] > 0),]
data_60 <- data_60[(data_60['Amt.Numerator'] > 0),]
data_120 <- data_120[(data_120['Amt.Numerator'] > 0),]


# hist(data_120[, 'exec_only_round'])
# hist(data_120[(data_120[,'exec_only_round'] < 50), 'exec_only_round'])


# re-scale the DV. relative to the mean, minus mean div by sd. 
## TODO: this shouldn't be z-score, it should be normalized bc it's not
## normally distributed. 

hist(data_24[, 'DV2'], 
     breaks = 120, 
     xlab = 'Share of Outstanding Maturing This Month (Non-Zero Values)', ylab = 'Frequency',
     main = 'Histogram of Share Maturing Today, 2y')

hist(data_60[, 'DV2'], 
     breaks = 120, 
     xlab = 'Share of Outstanding Maturing This Month (Non-Zero Values)', ylab = 'Frequency',
     main = 'Histogram of Share Maturing Today, 5y')

hist(data_120[, 'DV2'], 
     breaks = 120, 
     xlab = 'Share of Outstanding Maturing This Month (Non-Zero Values)', ylab = 'Frequency',
     main = 'Histogram of Share Maturing Today, 10y')


hist(data_24[(data_24['DV'] > 0) & (data_24['DV'] < 0.5), 'DV'], 
     breaks = 120, 
     xlab = 'Share of Outstanding Maturing This Month (Non-Zero Values)', ylab = 'Frequency',
     main = 'Histogram of Share Maturing Today: 2y')
hist(data_60[(data_60['DV'] > 0) & (data_60['DV'] < 0.5), 'DV'], 
     breaks = 120, 
     xlab = 'Share of Outstanding Maturing This Month (Non-Zero Values)', ylab = 'Frequency',
     main = 'Histogram of Share Maturing Today:5y')
hist(data_120[(data_120['DV'] > 0) & (data_120['DV'] < 0.5), 'DV'], 
     breaks = 120, 
     xlab = 'Share of Outstanding Maturing This Month (Non-Zero Values)', ylab = 'Frequency',
     main = 'Histogram of Share Maturing Today:10y')

m24 <- lmer(DV2 ~
             Months.Mat.To.Election * exec_only_round
           + Amt.Denominator # total amount issued that month
           # add original credit rating var. 
           # + inv_grade
           + cr
           # # global liquidity is tight
           + US_FFR
           + (1 | Country)
           + (1 | Curr)
           , data = data_24[(data_24[,'exec_only_round'] != -999),]
)
summary(m24)
r.squaredGLMM(m24)

m60 <- lmer(DV2 ~
              Months.Mat.To.Election * exec_only_round
            + Amt.Denominator # total amount issued that month
            # add original credit rating var. 
            # + inv_grade
            + cr
            # # global liquidity is tight
            + US_FFR
            + (1 | Country)
            + (1 | Curr)
            , data = data_60[(data_60[,'exec_only_round'] != -999),]
)
summary(m60)
r.squaredGLMM(m60)

m120 <- lmer(DV2 ~
              Months.Mat.To.Election * exec_only_round
             + Amt.Denominator # total amount issued that month
             # add original credit rating var. 
            # + inv_grade
            + cr
            # # global liquidity is tight
            + US_FFR
            + (1 | Country)
            + (1 | Curr)
            , data = data_120[(data_120[,'exec_only_round'] != -999),]
)
summary(m120)
r.squaredGLMM(m120)

# try with convert to USD and sum it all that way 

m24_usd <- lmer(DV_USD2 ~
              Months.Mat.To.Election * exec_only_round
              + Amt.Denominator_USD # total amount issued that month
              # add original credit rating var. 
            # + inv_grade
            + cr
            # # global liquidity is tight
            + US_FFR
            + (1 | Country)
            + (1 | Curr)
            , data = data_24[(data_24[,'exec_only_round'] != -999),]
)
summary(m24_usd)
r.squaredGLMM(m24_usd)

m60_usd <- lmer(DV_USD2 ~
              Months.Mat.To.Election * exec_only_round
              + Amt.Denominator_USD # total amount issued that month
              # add original credit rating var. 
            # + inv_grade
            + cr
            # # global liquidity is tight
            + US_FFR
            + (1 | Country)
            + (1 | Curr)
            , data = data_60[(data_60[,'exec_only_round'] != -999),]
)
summary(m60_usd)
r.squaredGLMM(m60_usd)

m120_usd <- lmer(DV_USD2 ~
               Months.Mat.To.Election * exec_only_round
               + Amt.Denominator_USD # total amount issued that month
               # add original credit rating var. 
             # + inv_grade
             + cr
             # # global liquidity is tight
             + US_FFR
             + (1 | Country)
             + (1 | Curr)
             , data = data_120[(data_120[,'exec_only_round'] != -999),]
)
summary(m120_usd)
r.squaredGLMM(m120_usd)

# try neg bin.? to account for zero values, which are something else entirely. 
# I just filtered them out instead. 

##############################
###### coefficient plot ######
##############################

onecoef <- summary(m24_usd)$coef
onecoef <- onecoef[row.names(onecoef),]
var.names <- row.names(onecoef)
coef.vec.one <- unname(onecoef[,'Estimate'])
se.vec.one <- unname(onecoef[,'Std. Error'])
p.vec.one <- unname(onecoef[,'Pr(>|t|)'])

twocoef <- summary(m60_usd)$coef
twocoef <- twocoef[row.names(onecoef),]
var.names <- row.names(twocoef)
coef.vec.two <- unname(twocoef[,'Estimate'])
se.vec.two <- unname(twocoef[,'Std. Error'])
p.vec.two <- unname(twocoef[,'Pr(>|t|)'])

threecoef <- summary(m120_usd)$coef
threecoef <- threecoef[row.names(onecoef),]
var.names <- row.names(threecoef)
coef.vec.three <- unname(threecoef[,'Estimate'])
se.vec.three <- unname(threecoef[,'Std. Error'])
p.vec.three <- unname(threecoef[,'Pr(>|t|)'])

results_df <- data.frame(term = rep(var.names, times = 3),
                         estimate = c(coef.vec.one, coef.vec.two, coef.vec.three),
                         std.error = c(se.vec.one, se.vec.two, se.vec.three),
                         p.value = c(p.vec.one, p.vec.two, p.vec.three),
                         model = c(rep("2y", length(var.names)), 
                                   rep("5y", length(var.names)),
                                   rep("10y", length(var.names))),
                         stringsAsFactors = FALSE)

# keepcols <- c('Months.Mat.To.Election', 'exec_only_round', 'Months.Mat.To.Election:exec_only_round')
results_df
keepcols <- c('Months.Mat.To.Election', glue('{var}'), glue('Months.Mat.To.Election:{var}'))
keepcols
results_df <- results_df[(results_df$term == keepcols[1]) |
                           (results_df$term == keepcols[2]) |
                           (results_df$term == keepcols[3]) ,]
results_df

colors <- list(c(0.16941176470588235, 0.15, 0.5323529411764706),
               c(0.9419607843137255, 0.3950980392156863, 0.06294117647058822),
               c(0.9, 0.8805882352941177, 0.44823529411764707))
new_colors = c(rgb(red = colors[[1]][1], green = colors[[1]][2], blue = colors[[1]][3]), 
               rgb(red = colors[[2]][1], green = colors[[2]][2], blue = colors[[2]][3]), 
               rgb(red = colors[[3]][1], green = colors[[3]][2], blue = colors[[3]][3]))

p <- dwplot(results_df) +
  theme_bw() +
  theme(legend.justification=c(.02, .993), 
        legend.position=c(0.8, .3),
        legend.title = element_blank(), 
        legend.background = element_rect(color="gray90"),
        plot.title = element_text(hjust = 0.5)) + 
  guides(color = guide_legend(override.aes = list(size=3))) + 
  xlab("Coef. Est.") +
  scale_color_manual(labels = c("mat<=2y", "mat<=5y", 'mat<=10y'),
                     values = c(new_colors[3], new_colors[2], new_colors[1])) +
  geom_vline(xintercept = 0, colour = "grey60", linetype = 2) #+
p
ggsave(paste(dir, glue('/cmcm_coefplot_usd_{var}.png'), sep = ''))


##############################
###### marginal effects ######
##############################

# marg <- margins(m24) # default is observed value approach
# marg
# plot(marg)

# m <- margins(m24_usd, at = list(exec_only_round = fivenum(data_24$exec_only_round, na.rm = TRUE)))
m <- margins(m24_usd, at = 
               list(exec_only_round =
                      fivenum(data_24[(data_24$exec_only_round != -999), 'exec_only_round'], 
                              na.rm = TRUE)))
sm <- summary(m)
sm <- data.frame(lapply(sm, function(y) if(is.numeric(y)) round(y, 8) else y)) 
sm <- sm[sm['factor'] =='Months.Mat.To.Election',]

# output a table for the write-up.
print(xtable(sm[,2:dim(sm)[2]], digits = 3), include.rownames = FALSE)

# output a plot for the write-up. 
ggplot(data = sm) + 
  theme_bw() +
  geom_line(aes(x = exec_only_round, y = AME), colour = new_colors[1]) +
  geom_ribbon(aes(x = exec_only_round, y = AME,
                  ymin = lower, ymax = upper),
              linetype = 2, alpha = .15, colour = new_colors[1]) +
  scale_color_manual(values=new_colors) +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = 'bottom',
        legend.title = element_blank()) +
  labs(colour = "Variable:") +
  xlab("Executive Vote Share") +
  ylab('Marginal Effect of Months to Election on Share') #+
# ggtitle("Effect of Variable Change on \n Predicted Probability of Protest")
ggsave(paste(dir, glue('/cmcm_ame_24m_usd_{var}.png'), sep = ''))

# interpretation: 
# as the vote margin increases, months to election has a stronger neg effect. 
# this means that closer elections means a larger more positive effect, 
# which is what we want. This esp when combined with the positive significant
# coefficient for Months.Mat.To.Election supports our hypotheses: countries do this
# in general, and they only do it less when the election isn't close 

##############################
###### regression table ######
##############################

omit_vars <- c('Amt.Denominator','Amt.Denominator_USD', 'cr', 'US_FFR')
class(m24) <- "lmerMod"
class(m24_usd) <- "lmerMod"
class(m60) <- "lmerMod"
class(m60_usd) <- "lmerMod"
class(m120) <- "lmerMod"
class(m120_usd) <- "lmerMod"

# doesn't show up in LC.

stargazer(m24, m60, m120,
          column.labels = c('2y', '5y', '10y'),
          dep.var.labels.include = FALSE,
          dep.var.caption = 'Pct. of Issued Debt Maturing Near an Election', 
          colnames = FALSE,
          df = FALSE,
          digits = 4,
          font.size = "small",
          column.sep.width = "-5pt",
          omit = c('Constant', 'aic', 'bic', 'll', omit_vars),
          # omit = c("Constant", controls),
          no.space = TRUE,
          # add.lines = list(c("Country and Currency FE", "Y", "Y", "Y", "Y", "Y")),
          header = FALSE, 
          type = 'latex'
)

# it shows up in USD. 


stargazer(m24_usd, m60_usd, m120_usd,
          column.labels = c('2y', '5y', '10y'),
          dep.var.labels.include = FALSE,
          dep.var.caption = 'Pct. of Issued Debt Maturing Near an Election', 
          colnames = FALSE,
          df = FALSE,
          digits = 4,
          font.size = "small",
          column.sep.width = "-5pt",
          omit = c('Constant', 'aic', 'bic', 'll', omit_vars),
          # omit = c("Constant", controls),
          no.space = TRUE,
          # add.lines = list(c("Country and Currency FE", "Y", "Y", "Y", "Y", "Y")),
          header = FALSE, 
          type = 'latex'
)

