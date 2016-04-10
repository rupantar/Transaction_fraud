
# ================================== PART 1 ========================================================

library(sqldf)
library(dplyr)
# reading the data into R
rm(list = ls())
data<-read.csv('trans.csv',na.strings = "")

# removing variables that do not add any value to the analysis

data = subset(data,data$TRANSTYPE=="P")
data$fraud. = NULL
data$TRANSTYPE = NULL
data$MERCHDESCRIPTION=NULL
data$MERCHSTATE=NULL
data$MERCHZIP=NULL
data = data[data$MERCHNUM != 0,]
data = data[!is.na(data$MERCHNUM),] 
# converting dates to the POXiX format
library(lubridate)
data$DATE = mdy(data$DATE)

# Converting amount into numeric

data$AMOUNT = gsub('\\$',"",data$AMOUNT)
data$AMOUNT = as.numeric(data$AMOUNT)# this is the cleaned data

colnames(data)[1] = 'Record'

# creating the previous days variables in the test dataframe 

test=data %>%
  mutate(same_day=DATE) %>%
  mutate(previous_2_day=DATE-days(1)) %>%
  mutate(previous_7_day=DATE-days(6))%>%
  mutate(previous_90_day=DATE-days(89))

#creating the numerators for the frequencies of transactions

card_freq_lookup1 = sqldf('select a.Record,a.CARDNUM,a.DATE,count(*) as card_freq_1_day
             from test a join test b on a.CARDNUM = b.CARDNUM
             where b.DATE = a.DATE
             group by a.Record')

card_freq_lookup2 = sqldf('select a.Record,a.CARDNUM,a.DATE,count(*) as card_freq_2_day
              from test a join test b on a.CARDNUM = b.CARDNUM
              where b.DATE >= a.previous_2_day and b.DATE <= a.DATE
              group by a.Record')

card_freq_lookup7 = sqldf('select a.Record,a.CARDNUM,a.DATE,count(*) as card_freq_7_day
                from test a join test b on a.CARDNUM = b.CARDNUM
                where b.DATE >= a.previous_7_day and b.DATE <= a.DATE
                group by a.Record')

# creating the average of frequencies denominators

card_lookup_avg_1_days = sqldf('select a.Record,a.CARDNUM,a.DATE,
                       (cast(count(*) as FLOAT))/ 90 as card_avg_1_day
                       from test a join test b on a.CARDNUM = b.CARDNUM
                       where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                       group by a.Record')

card_lookup_avg_2_days = sqldf('select a.Record,a.CARDNUM,a.DATE,
                          (cast(count(*) as FLOAT))/ (90/2) as card_avg_1_day
                          from test a join test b on a.CARDNUM = b.CARDNUM
                          where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                          group by a.Record')

card_lookup_avg_7_days = sqldf('select a.Record,a.CARDNUM,a.DATE,
                          (cast(count(*) as FLOAT))/ (90/7) as card_avg_1_day
                          from test a join test b on a.CARDNUM = b.CARDNUM
                          where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                          group by a.Record')

# these are the variables we need to use 
card_1_freq = cbind(data,card_freq_lookup1[4] / card_lookup_avg_1_days[4])
card_2_freq = cbind(card_1_freq,card_freq_lookup2[4] / card_lookup_avg_2_days[4])
card_7_freq = cbind(card_2_freq,card_freq_lookup7[4] / card_lookup_avg_7_days[4])

data = card_7_freq

# creating variables for the merchant level 

merch_freq_lookup1 = sqldf('select a.Record,a.MERCHNUM,a.DATE,count(*) as merch_freq_1_day
                          from test a join test b on a.MERCHNUM = b.MERCHNUM
                          where b.DATE = a.DATE
                          group by a.Record')

merch_freq_lookup2 = sqldf('select a.Record,a.MERCHNUM,a.DATE,count(*) as merch_freq_2_day
                          from test a join test b on a.MERCHNUM = b.MERCHNUM
                          where b.DATE >= a.previous_2_day and b.DATE <= a.DATE
                          group by a.Record')

merch_freq_lookup7 = sqldf('select a.Record,a.MERCHNUM,a.DATE,count(*) as merch_freq_7_day
                          from test a join test b on a.MERCHNUM = b.MERCHNUM
                          where b.DATE >= a.previous_7_day and b.DATE <= a.DATE
                          group by a.Record')

# creating the average of frequencies denominators

merch_lookup_avg_1_days = sqldf('select a.Record,a.MERCHNUM,a.DATE,
                               (cast(count(*) as FLOAT))/ 90 as merch_avg_1_day
                               from test a join test b on a.MERCHNUM = b.MERCHNUM
                               where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                               group by a.Record')

merch_lookup_avg_2_days = sqldf('select a.Record,a.MERCHNUM,a.DATE,
                               (cast(count(*) as FLOAT))/ (90/2) as merch_avg_1_day
                               from test a join test b on a.MERCHNUM = b.MERCHNUM
                               where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                               group by a.Record')

merch_lookup_avg_7_days = sqldf('select a.Record,a.MERCHNUM,a.DATE,
                               (cast(count(*) as FLOAT))/ (90/7) as merch_avg_1_day
                               from test a join test b on a.MERCHNUM = b.MERCHNUM
                               where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                               group by a.Record')


merch_1_freq = cbind(data,merch_freq_lookup1[4] / merch_lookup_avg_1_days[4])
merch_2_freq = cbind(merch_1_freq,merch_freq_lookup2[4] / merch_lookup_avg_2_days[4])
merch_7_freq = cbind(merch_2_freq,merch_freq_lookup7[4] / merch_lookup_avg_7_days[4])

data = merch_7_freq

write.csv(data,'data_freq.csv')

#================================ PART 1 END ==========================================================

#================================ PART 2 ==========================================================
# building variables for the amount level

card_amt_lookup1 = sqldf('select a.Record,a.CARDNUM,a.DATE,sum(a.AMOUNT) as card_amt_1_day
                          from test a join test b on a.CARDNUM = b.CARDNUM
                          where b.DATE = a.DATE
                          group by a.Record')

card_amt_lookup2 = sqldf('select a.Record,a.CARDNUM,a.DATE,sum(a.AMOUNT) as card_amt_2_day
                          from test a join test b on a.CARDNUM = b.CARDNUM
                          where b.DATE >= a.previous_2_day and b.DATE <= a.DATE
                          group by a.Record')

card_amt_lookup7 = sqldf('select a.Record,a.CARDNUM,a.DATE,sum(a.AMOUNT) as card_amt_7_day
                          from test a join test b on a.CARDNUM = b.CARDNUM
                          where b.DATE >= a.previous_7_day and b.DATE <= a.DATE
                          group by a.Record')

# creating the average of frequencies denominators

card_amt_avg_1_days = sqldf('select a.Record,a.CARDNUM,a.DATE,
                               (cast(sum(a.AMOUNT) as FLOAT))/ 90 as card_amt_avg_1_day
                               from test a join test b on a.CARDNUM = b.CARDNUM
                               where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                               group by a.Record')

card_amt_avg_2_days = sqldf('select a.Record,a.CARDNUM,a.DATE,
                               (cast(sum(a.AMOUNT) as FLOAT))/ (90/2) as card_amt_avg_1_day
                               from test a join test b on a.CARDNUM = b.CARDNUM
                               where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                               group by a.Record')

card_amt_avg_7_days = sqldf('select a.Record,a.CARDNUM,a.DATE,
                               (cast(sum(a.AMOUNT) as FLOAT))/ (90/7) as card_avg_1_day
                               from test a join test b on a.CARDNUM = b.CARDNUM
                               where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                               group by a.Record')

# these are the variables we need to use 
card_1_amt = cbind(data,card_amt_lookup1[4] / card_amt_avg_1_days[4])
card_2_amt = cbind(card_1_amt,card_amt_lookup2[4] / card_amt_avg_2_days[4])
card_7_amt = cbind(card_2_amt,card_amt_lookup7[4] / card_amt_avg_7_days[4])

data = card_7_amt

# creating variables for the merchant level 

merch_amt_lookup1 = sqldf('select a.Record,a.MERCHNUM,a.DATE,sum(a.Amount) as merch_amt_1_day
                           from test a join test b on a.MERCHNUM = b.MERCHNUM
                           where b.DATE = a.DATE
                           group by a.Record')

merch_amt_lookup2 = sqldf('select a.Record,a.MERCHNUM,a.DATE,sum(a.Amount) as merch_amt_2_day
                           from test a join test b on a.MERCHNUM = b.MERCHNUM
                           where b.DATE >= a.previous_2_day and b.DATE <= a.DATE
                           group by a.Record')

merch_amt_lookup7 = sqldf('select a.Record,a.MERCHNUM,a.DATE,sum(a.Amount) as merch_amt_7_day
                           from test a join test b on a.MERCHNUM = b.MERCHNUM
                           where b.DATE >= a.previous_7_day and b.DATE <= a.DATE
                           group by a.Record')

# creating the average of frequencies denominators

merch_amt_avg_1_days = sqldf('select a.Record,a.MERCHNUM,a.DATE,
                                (cast(count(*) as FLOAT))/ 90 as merch_amt_avg_1_day
                                from test a join test b on a.MERCHNUM = b.MERCHNUM
                                where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                                group by a.Record')

merch_amt_avg_2_days = sqldf('select a.Record,a.MERCHNUM,a.DATE,
                                (cast(count(*) as FLOAT))/ (90/2) as merch_amt_avg_1_day
                                from test a join test b on a.MERCHNUM = b.MERCHNUM
                                where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                                group by a.Record')

merch_amt_avg_7_days = sqldf('select a.Record,a.MERCHNUM,a.DATE,
                                (cast(count(*) as FLOAT))/ (90/7) as merch_amt_avg_1_day
                                from test a join test b on a.MERCHNUM = b.MERCHNUM
                                where b.DATE >= a.previous_90_day and b.DATE <= a.DATE
                                group by a.Record')


merch_1_amt = cbind(data,merch_amt_lookup1[4] / merch_amt_avg_1_days[4])
merch_2_amt = cbind(merch_1_amt,merch_amt_lookup2[4] / merch_amt_avg_2_days[4])
merch_7_amt = cbind(merch_2_amt,merch_amt_lookup7[4] / merch_amt_avg_7_days[4])

data = merch_7_amt

write.csv(data,'final_data.csv')

#================================ PART 2 END ==========================================================

