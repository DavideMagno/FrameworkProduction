## ---------------------------
##
## Script name: Frameworkv2
## Purpose of script:
## Author: Meyrick Chapman
## Date Created: 2021-09-09
## Copyright (c) Hedge Analytics Ltd, 2021
## Email: meyrick@hedge-analytics.com
##
## ---------------------------
## Notes:
##   
## ---------------------------

## set working directory

## project directory = default 

## ---------------------------

options(scipen = 6, digits = 6) # I prefer to view outputs in non-scientific notation

## ---------------------------

## load up the packages we will need:  (uncomment as required)
library(tidyr)
library(plyr)
library(dplyr)
library(lubridate)
library(xts)
library(missMDA)
library(FactoMineR)
library(factoextra)
library(corrplot)
library(fredr)
# source("functions/packages.R")       # loads up all the packages we need

## ---------------------------

add.col<-function(df, new.col) {n.row<-dim(df)[1]
length(new.col)<-n.row
cbind(df, new.col)
}

m_rescfact<-function(x,lbound,ubound){
  
  result<-(ubound-lbound)/( max(x)-min(x))
  
  return(result)
}

m_rescale<-function(x,lbound,ubound){
  
  fact<-(ubound-lbound)/( max(x)-min(x))
  result<-fact*(x-max(x))+ubound
  return(result)
}

m_descale<-function(x,ubound,max_orig,scalefactor){
  result<-((x-ubound)/scalefactor)+max_orig
  return(result)
}

## ---------------------------

LAG<-6
RESCALELB<-5
RESCALEUB<-50

df<-read.csv("Data/All_data.csv")

#scaleddf<-data.frame(lapply(df[,2:ncol(df)],m_rescale,RESCALELB,RESCALEUB))

data_used<-xts(df[,2:ncol(df)], order.by = as.Date(ymd(df$period)))
period<-df$period[(LAG):nrow(df)]

suppressWarnings(data_used <- diff(log(data_used), lag = LAG))
data_used[is.infinite(data_used) | is.nan(data_used)] <- NA

#data_used <- diff(log(data_used), lag = LAG)
data_used<-data_used[(LAG):nrow(data_used)]
data_used<-as.data.frame(coredata(data_used))

lastdata<-data_used[nrow(data_used),]
# #calculate any missing values using PCA, 4 dimensions
res.comp = imputePCA(data_used,ncp=4)

#perform PCA on resulting complete observation set
# question: is it better to perform PCA on a moving window or the entire dataset? This version uses whole dataset.
pc <- prcomp(coredata(res.comp$completeObs), scale = T, center = T)
pc1<-PCA(coredata(res.comp$completeObs))

data_used<-as.data.frame(res.comp$completeObs)
#build a fitted series
fitted<-as.data.frame(res.comp$fittedX)

data_used$period <- as.Date(period)
fitted$period<-as.Date(period)
names(fitted)<-names(data_used)
  
Ref_data<-df[LAG:nrow(df),-1]

for(i in 1:length(unique(names(data_used[1:(length(data_used)-1)])))) {
  png(file=paste0("Output/Fitted",names(data_used[i]),".png"),
      width=600, height=350)
  plot(
    as.Date(data_used$period),
    data_used[, i],
    type = 'l',
    xlab = "Date",
    ylab = "6m log chg %"
  )
  lines(as.Date(data_used$period), fitted[,i], col = 'red')
  title(main = paste0(names(data_used[i]), " 6m log chg"))
  dev.off()
}

tabsummary<-data_used[,1:(length(data_used)-1)]
tabsummary<-tabsummary[FALSE,]

for(i in 1:length(unique(names(data_used[1:(length(data_used)-1)])))) {
  png(file=paste0("Output/",names(data_used[i]),".png"),
      width=600, height=350)
  plot(
    data_used$period,
    (data_used[, i]-fitted[, i])/pc$scale[i],
    type = 'l',
    xlab = "Date",
    ylab = "Std Dev fm Mean"
  )
  abline(a=0.75,b=0, col="red")
  abline(a=-0.75,b=0, col="red")
  abline(a=0,b=0, col="green")
  title(main = paste0(names(data_used[i]), " Actual less Fitted"))
  dev.off()
  tabsummary[1,i]<-round(Ref_data[nrow(Ref_data),i],4)
  tabsummary[2,i]<-round((lastdata[1,i]-fitted[nrow(fitted),i])*Ref_data[nrow(Ref_data),i],4)
  tabsummary[3,i]<-tabsummary[1,i]-tabsummary[2,i]
  
}

tabsummary[1,31]<-round(1/tabsummary[1,31],4)
tabsummary[3,31]<-round(1/tabsummary[3,31],4)
tabsummary[1,35:36]<-round(1/tabsummary[1,35:36],4)
tabsummary[3,35:36]<-round(1/tabsummary[3,35:36],4)
tabsummary[1:3,10:12]<-round(tabsummary[1:3,10:12],2)
tabsummary[4,]<-  round((((tabsummary[1,]/tabsummary[3,])-1)*100),2)
row.names(tabsummary)<-c("Actual", "Diff_from_fitted","Fitted_level","Diff_pct")
write.csv(t(tabsummary),"Output/summarytable.csv",row.names = TRUE)
View(t(tabsummary))


fviz_contrib(pc, choice = "var", axes = 1, top = 57)
fviz_contrib(pc, choice = "var", axes = 2, top = 57)
fviz_contrib(pc, choice = "var", axes = 1:4, top=57,fill = "#E7B800")

fviz_pca_var(pc, col.var = "cos2",
             gradient.cols = c("#00AFBB", "#E7B800", "#FC4E07"), 
             repel = TRUE, # Avoid text overlapping,
             title="Correlation of markets to PC1&2, 2005-2021"
)

var <- get_pca_var(pc1)

corrplot(var$cos2, method="circle", is.corr=F)

eig.val<-pc1$eig

barplot(eig.val[, 2], 
        names.arg = 1:nrow(eig.val), 
        main = "Variances Explained by PCs (%)",
        xlab = "Principal Components",
        ylab = "Percentage of variances",
        col ="steelblue")

mydir <- getwd()
rmdfiles <- list.files(path=mydir, pattern="*.Rmd", full.names=FALSE)

for(mdown in rmdfiles){
  print(mdown)
  rmarkdown::render(mdown, output_dir = 'Output')
}
  
