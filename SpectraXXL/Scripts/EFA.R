## ---------------------------
##
## Script name: EFA
## Purpose of script: Exploratory Factor Analysis on Financial/monetary regime 
## Author: Meyrick Chapman
## Date Created: 12 May 2021
## Copyright (c) Hedge Analytics Ltd
## Email: meyrick@hedge-analytics.com
##
## ---------------------------
## Notes: "a decomposition of the variance of a test score, Vx, into four parts: 
##          1/that due to a general factor, g, 
##          2/ that due to a set of group factors, f, (factors common to some but not all of the items), 
##          3/ specific factors, s unique to each item, 
##      and 4/ e, random error

##        "the highest virtue in science is a passion for generality, unity, and consistency in theory, 
##         and the highest art is to ﬁnd a strategy that simpliﬁes a complex problem." 
##
##      To me, economics was indeed the "dismal science,"
##      and I tend to see it as good mathematics applied to 
##      naïve models for really terrible data. 
##      With Markov chain Monte Carlo(MCMC) methods 
##      it is getting easier each year to ﬁt a totally wrong model
##      and get plausible parameters.
##      It remains clear to me that all factor models and item response models 
##      are about generalizing to more variables or items than those measured, 
##      and I stand by my conclusion(McDonald, 1977): "If a factor in the domain 
##      is deﬁned by the marker variables, it is deﬁned uniquely" 
##      Roderick P. McDonald: Interview (2007) by Howard Wainer and Daniel H. Robinson
##   
## ---------------------------

## set working directory


#setwd("~/Library/Mobile Documents/com~apple~CloudDocs/Meyrick/R/Scripts/CCF/")      # 

## ---------------------------

options(scipen = 6, digits = 4) # I prefer to view outputs in non-scientific notation

## ---------------------------

## load up the packages we will need:  (uncomment as required)
library(psych)

# source("functions/packages.R")       # loads up all the packages we need

## ---------------------------



## ---------------------------

datadelta<-read.csv(paste0("Data/All_data_delta.csv"),stringsAsFactors=FALSE)

data1<-datadelta[,2:ncol(datadelta)]
fa.parallel(data1,fm = 'minres', fa = 'fa')

fivefactor<-fa(r=data1, nfactors=5, rotate = "varimax", fm="minres")
varlabels5<-c('Stocks','Currencies','Liquidity/Growth', 'Duration', 'Carry')
colnames(fivefactor$loadings) <- varlabels5
fa.diagram(fivefactor,main='Financial/monetary factors')

mydata.cov <- cov(data1, use="pairwise.complete.obs")
ICLUST(mydata.cov)

a <- omega(data1,nfactors=5, title="Financial/monetary factor analysis", sl=FALSE)

omega(data1,nfactors=5, title="Financial/monetary factor analysis", sl=TRUE)

