library(shiny)
library(openxlsx)
library(PerformanceAnalytics)
library(shinycssloaders)

source(here::here("Scripts/SVD.R"))
source(here::here("Scripts/RelativeTrade.R"))
source(here::here("Scripts/InvarianceTest.R"))

FacilitateReport <- function(index, returns, prices, type, dates, 
                             rebalancing.frequency, K, 
                             LAG, delay, ROLLING.WINDOW) {
  message(glue::glue("Analysing {index}"))
  AnalysisForRecommendation(index, returns, prices, rebalancing.frequency, K, 
                            type, LAG, delay, dates, ROLLING.WINDOW)
}

LAG <- 1
K <- 3
rebalancing.frequency <- 1
ROLLING.WINDOW <- 12
delay <- 0

df <- read.csv(here::here("Data/All_dataIncSkewFlex.csv"))

N <- ncol(df)

type <- df |> 
  dplyr::slice(1) |> 
  dplyr::select(-period) |> 
  dplyr::mutate_all(~dplyr::if_else(grepl("Log", .x), TRUE, FALSE)) |> 
  purrr::flatten_lgl() 

prices <- df |> 
  dplyr::slice(-1:-2) |> 
  dplyr::mutate(dates = lubridate::as_date(period, format = "%d/%m/%Y"),
                dplyr::across(2:N, as.numeric)) |> 
  dplyr::select(-period)

dates <- prices$dates[seq(from = LAG + 1, to = nrow(prices), by = LAG)]

returns <- prices |> 
  dplyr::select(-dates) |> 
  purrr::map2_dfc(type, ~CalculateInvariant(.x, .y, LAG)) |> 
  dplyr::mutate(dates = as.Date(dates)) |> 
  dplyr::select(dates, dplyr::everything())


report <- purrr::map_dfr(colnames(returns)[-1],
                         ~FacilitateReport(.x, returns, prices, type, dates,
                                           rebalancing.frequency, K, 
                                           LAG, delay, ROLLING.WINDOW))