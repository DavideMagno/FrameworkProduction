source(here::here("Scripts/InvarianceTest.R"))
source(here::here("Scripts/SVD.R"))
source(here::here("Scripts/Portfolios.R"))
source(here::here("Scripts/myDatepickerInput.R"))
source(here::here("Scripts/RelativeTrade.R"))
source(here::here("Scripts/Hedging.R"))
library(PerformanceAnalytics)
library(doParallel)

run <- "Monthly"
K <- 3
index.strategy <- "S&P500"
center.flag <- FALSE

if (grepl("Daily", run)) {
  REBALANCE <- 1
  LAG <- 1
  ROLLING.WINDOW <- 252
  index.strategy <- "S&P500"
  input.file <- readr::read_csv("Data/dailyFWorkHistData.csv") |> 
    dplyr::select(-`GB12 Govt`)
} else {
  REBALANCE <- 1
  LAG <- 1
  ROLLING.WINDOW <- 12
  index.strategy <- "S.P500"
  input.file <- readr::read_csv("Data/All_dataIncSkewFlex.csv") 
}

type <- input.file |> 
  dplyr::slice(1) |> 
  dplyr::select(-period) |> 
  dplyr::mutate_all(~dplyr::if_else(grepl("Log", .x), TRUE, FALSE)) |> 
  purrr::flatten_lgl() 

N <- ncol(input.file)

prices <- input.file |> 
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

tictoc::tic()
tictoc::tic("Parallel computation")
init.algo.PURRR <- SimulateConePURRR(REBALANCE, 
                                     returns,
                                     prices,
                                     index.strategy,
                                     K, type, center.flag, LAG, ROLLING.WINDOW, FALSE)
tictoc::toc(log = TRUE)

tictoc::tic()
tictoc::tic("Current implementation")
init.algo <- SimulateCone(REBALANCE,
                          returns,
                          prices,
                          index.strategy,
                          K, type, center.flag, LAG, ROLLING.WINDOW, FALSE)
tictoc::toc(log = TRUE)

