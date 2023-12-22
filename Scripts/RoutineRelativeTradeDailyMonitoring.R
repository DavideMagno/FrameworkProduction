source(here::here("Scripts/InvarianceTest.R"))
source(here::here("Scripts/SVD.R"))
source(here::here("Scripts/Portfolios.R"))
source(here::here("Scripts/myDatepickerInput.R"))
source(here::here("Scripts/RelativeTrade.R"))
source(here::here("Scripts/Hedging.R"))
library(PerformanceAnalytics)
library(plotly)

`%not_in%` <- Negate("%in%")

run <- "Daily"
K <- 3
center.flag <- FALSE

REBALANCE <- 1
LAG <- 1
ROLLING.WINDOW <- 12

if (grepl("Daily", run)) {
  index.strategy <- "S&P500"
  input.file <- readr::read_csv("Data/dailyFWorkHistData.csv") |> 
    dplyr::select(-`GB12 Govt`)
} else {
  index.strategy <- "S.P500"
  input.file <- readr::read_csv("Data/All_dataIncSkewFlex.csv") 
}

type <- input.file |> 
  dplyr::slice(1) |> 
  dplyr::select(-period) |> 
  dplyr::mutate_all(~dplyr::if_else(grepl("Log", .x), TRUE, FALSE)) |> 
  purrr::flatten_lgl() 

N <- ncol(input.file)


# Prices ------------------------------------------------------------------

prices <- input.file |> 
  dplyr::slice(-1:-2) |> 
  dplyr::mutate(dates = lubridate::as_date(period, format = "%d/%m/%Y"),
                dplyr::across(2:N, as.numeric)) |> 
  dplyr::select(-period)

if (grepl("Daily", run)) {
  prices.monthly <- prices |> 
    dplyr::mutate(month = lubridate::month(dates),
                  day = lubridate::day(dates),
                  year = lubridate::year(dates)) |> 
    dplyr::group_by(year, month) |> 
    dplyr::filter(day == max(day)) |>
    dplyr::ungroup() |> 
    dplyr::select(-year, -month, - day)
  
  if (lubridate::day(tail(prices.monthly$dates, 1)) %not_in% 28:31) {
    prices.monthly <- head(prices.monthly, -1)
  } 
} else {
  prices.monthly <- prices
}


# Dates -------------------------------------------------------------------


dates.monthly <- prices.monthly$dates[seq(from = LAG + 1, 
                                          to = nrow(prices.monthly), 
                                          by = LAG)]
dates <- prices$dates[2:nrow(prices)]

if (grepl("Daily", run)) {
  dates.main <- dates
} else {
  dates.main <- dates.monthly
} 

# Returns -----------------------------------------------------------------

returns <- prices |> 
  dplyr::select(-dates) |> 
  purrr::map2_dfc(type, ~CalculateInvariant(.x, .y, LAG)) |> 
  dplyr::mutate(dates = as.Date(dates.main)) |> 
  dplyr::select(dates, dplyr::everything())

if (grepl("Daily", run)) {
  returns.monthly <- prices.monthly |> 
    dplyr::select(-dates) |> 
    purrr::map2_dfc(type, ~CalculateInvariant(.x, .y, LAG)) |> 
    dplyr::mutate(dates = as.Date(dates.monthly)) |> 
    dplyr::select(dates, dplyr::everything())
} else {
  returns.monthly <- returns
}

# Monthly Cone ------------------------------------------------------------

tictoc::tic("PCA rolling analysis")
init.algo <- SimulateCone(REBALANCE,
                          returns.monthly,
                          prices.monthly,
                          index.strategy,
                          K, type, center.flag, LAG, ROLLING.WINDOW, FALSE)
tictoc::toc(log = TRUE)

# Trading -----------------------------------------------------------------
tictoc::tic("Trade Analysis")
calculation.dates <- prices.monthly$dates |> 
  tail(-ROLLING.WINDOW) |> 
  head(-1)

if (grepl("Daily", run)) {
  returns <- returns |> 
    dplyr::select(dates, index.strategy = index.strategy)
  
  prices <- prices |> 
    dplyr::select(dates, index.strategy = index.strategy)
  
  
  trading <- RelativeTradingCone(init.algo, calculation.dates,
                                 daily.returns = returns, daily.prices = prices)
} else {
  trading <- RelativeTradingCone(init.algo, calculation.dates)
}
tictoc::toc(log = TRUE)
p <- TradeDistancePlot(trading, FALSE)
print(p)

charts.PerformanceSummary(trading$portfolio.return[,2], ylog = TRUE, 
                          main = glue::glue("Relative Value Performance for \\
                          {index.strategy} with {run} input data"))


