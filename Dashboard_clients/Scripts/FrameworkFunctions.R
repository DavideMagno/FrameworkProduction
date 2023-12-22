source("/home/threshold/FrameworkProduction/Scripts/InvarianceTest.R")
source("/home/threshold/FrameworkProduction/Scripts/SVD.R")
source("/home/threshold/FrameworkProduction/Scripts/RelativeTrade.R")
library(PerformanceAnalytics)

`%not_in%` <- Negate("%in%")

CalculatePrices <- function(LAG, dir, start_date = as.Date("2000-01-03"),
                            end_date = Sys.Date() - lubridate::days(1)) {
  
  prices <- arrow::open_dataset(dir) |> 
    dplyr::collect() |> 
    dplyr::select(-YearMonth) |> 
    dplyr::rename(dates = "date") |> 
    dplyr::arrange(dates) |> 
    dplyr::filter(dates >= start_date) |> 
    dplyr::mutate(month = lubridate::month(dates),
                  year = lubridate::year(dates))
  
  if (LAG == 52) {
    prices.monthly <- prices |> 
      dplyr::mutate(day = lubridate::wday(dates)) |> 
      dplyr::filter(day == 6) |> 
      dplyr::select(-day)
  } else {
    if (LAG == 12 | LAG == 4) {
      prices.monthly <- prices |> 
        dplyr::mutate(day = lubridate::day(dates)) |> 
        dplyr::group_by(year, month) |> 
        dplyr::filter(day == max(day)) |>
        dplyr::ungroup() |> 
        dplyr::select(-day) 
      
      if (lubridate::day(tail(prices.monthly$dates, 1)) %not_in% 28:31) {
        prices.monthly <- head(prices.monthly, -1)
      }
    } else {
      prices.monthly <- prices
    }
  }
  
  prices <- prices |> 
    dplyr::select(-month, -year)
  
  prices.monthly <- prices.monthly |> 
    dplyr::select(-month, -year)
  
  if (LAG == 4) {
    current.month <- lubridate::month(tail(prices.monthly$dates, 1))
    shift.back <- (current.month %% 3)
    factor <- 3
  } else {
    shift.back  <- 0
    factor <- 1
  }
  
  N <- nrow(prices.monthly)
  
  prices.cut <- prices |> 
    dplyr::filter(dates >= start_date,
                  dates <= end_date)
  
  prices.monthly.cut <- prices.monthly |> 
    dplyr::filter(dates >= start_date,
                  dates <= end_date)
  
  return(list(prices = prices,
              prices.monthly = prices.monthly,
              prices.cut = prices.cut,
              prices.monthly.cut = prices.monthly.cut))
}

FromPriceToReturns <- function(prices, object, type) {
  
  dates <- prices |>
    purrr::pluck(object) |> 
    dplyr::pull(dates) |> 
    tail(-1)
  
  prices |>
    purrr::pluck(object) |> 
    dplyr::select(-dates) |> 
    purrr::map2_dfc(type, ~CalculateInvariant(.x, .y)) |> 
    dplyr::mutate(dates = as.Date(dates)) |> 
    dplyr::select(dates, dplyr::everything())
}


CalculateReturns <- function(prices, type) {
  
  returns <- purrr::map(as.list(names(prices)), 
                        ~FromPriceToReturns(prices, .x, as.list(type))) |> 
    purrr::set_names(c("returns", "returns.monthly", "returns.cut", "returns.monthly.cut"))
  
  return(list(returns = returns$returns,
              returns.monthly = returns$returns.monthly,
              returns.cut = returns$returns.cut,
              returns.monthly.cut = returns$returns.monthly.cut))
  
}

CalculateTypeLocal <- function(prices, asset_manager, dir = NULL) {
  
  if (is.null(dir)) {
    load(here::here(glue::glue("Data/IndexMapping{asset_manager}.RData")))
  } else {
    load(dir)
  }
  
  type <- tibble::tibble(Index = tail(colnames(prices$prices),-1)) |> 
    dplyr::left_join(FrameworkIndices, by = "Index") |> 
    dplyr::mutate(type_flag = dplyr::if_else(grepl("Log", invariant), TRUE, FALSE)) |> 
    dplyr::pull(type_flag) 
  
  return(type)
}

InitAlgo <- function(index, prices, returns, type, K, ROLLING.WINDOW, LAG) {
  n.max <- seq(ROLLING.WINDOW*LAG + 1, 
               nrow(prices$prices.monthly.cut), 
               by = 1) |> 
    c(nrow(prices$prices.monthly.cut)) |> 
    unique() |> 
    diff() |> 
    length()
  
  cone <- SimulateCone(1,returns$returns.monthly.cut,prices$prices.monthly.cut,
                       index, K, type, LAG, ROLLING.WINDOW)
  
  return(cone)
}


Framework <- function(index, prices, returns, type, K, ROLLING.WINDOW, LAG, am) {
  message(index)
  
  corridor_data <- InitAlgo(index, prices, returns, type, K, ROLLING.WINDOW, LAG)

  calculation.dates <- corridor_data$eingendata$eigenvectors$Dates
  
  n.max <- nrow(corridor_data$prices)

  returns <- returns$returns.cut[c("dates", index)] |> 
    dplyr::rename_all(~c("dates", "index.strategy")) |> 
    na.omit()
  
  daily.prices <- prices$prices.cut[c("dates", index)] |> 
    dplyr::rename_all(~c("dates", "index.strategy")) |> 
    na.omit()
  
  position <- which(tail(colnames(prices$prices), -1)  %in% index)
  type <- type[position]

  trading <- RelativeTradingCone(corridor_data, calculation.dates,
                                 daily.returns = returns, 
                                 daily.prices = daily.prices,
                                 type = type)

  #Fixing data for automatic parquet storage
  trading$indices <- trading$indices |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$raw.returns <- trading$raw.returns |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$trades <- trading$trades |> 
    dplyr::rename("Dates" = time) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$eigendata <- corridor_data$eingendata 
  
  trading$eigendata$eigenvectors <- trading$eigendata$eigenvectors |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$eigendata$eigenvalues <- trading$eigendata$eigenvalues |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$eigendata$eigenvalues.perc <- trading$eigendata$eigenvalues.perc |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))

  trading$range$range <- trading$range$range |> 
    dplyr::select(-Dates) |> 
    dplyr::rename("Dates" = t) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m")) |> 
    tibble::as_tibble()
  
  trading$range$range.daily <- trading$range$range.daily |>
    dplyr::rename("Dates" = dates) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$range.graph$range <- trading$range.graph$range |> 
    dplyr::select(-Dates) |> 
    dplyr::rename("Dates" = t) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m")) |> 
    tibble::as_tibble()
  
  trading$range.graph$range.daily <- trading$range.graph$range.daily |> 
    dplyr::rename("Dates" = dates) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$trades.graph <- trading$trades.graph |> 
    dplyr::rename("Dates" = time) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  interim <- as.data.frame(trading$portfolio.return) 
  interim$Dates <- rownames(interim)
  rownames(interim) <- NULL
  trading$portfolio.return <- interim |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(Dates = as.Date(Dates),
                  YearMonth = format(Dates, "%Y-%m"))

  WriteFullIndexOutput(trading, index, K, LAG, am)
  # return(trading)
}

AppendData <- function(data_to_store, dir_item) {
  stored_data <- arrow::open_dataset(dir_item) |> 
    dplyr::collect() |> 
    dplyr::arrange(Dates) 
  
  stored_data_dates <- stored_data$Dates
  new_data_dates <- data_to_store$Dates
  delta_dates <- setdiff(new_data_dates, stored_data_dates)
  
  new_data <- data_to_store |> 
    dplyr::filter(Dates %in% delta_dates)
  
  keys <- unique(new_data$YearMonth)
  
  for (key in keys) {
    
    new_data_for_file <- new_data |> 
      dplyr::filter(YearMonth %in% key) |> 
      dplyr::select(-YearMonth)
    
    dir <- here::here(glue::glue("{dir_item}/YearMonth={key}"))
    
    if (!dir.exists(dir)) {
      dir.create(dir)
      
      data_to_store <- new_data_for_file
    } else {
      old_data <- arrow::open_dataset(glue::glue("{dir}/part-0.parquet")) |> 
        dplyr::collect() 
      
      data_to_store <- old_data |> 
        dplyr::bind_rows(new_data_for_file)
      
      unlink(glue::glue("{dir}/part-0.parquet"))
    }
    
    arrow::write_dataset(data_to_store,
                         dir,
                         format = "parquet")
    
  }
}

WriteFullIndexOutput <- function(framework.output, index, K, LAG, am) {
  message(glue::glue("***{index}***"))
  dir <- here::here(glue::glue("Data/output{am}/{index}/K={K}/LAG={LAG}"))
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
  framework.output <- purrr::list_flatten(framework.output)
  items <- names(framework.output) 
  items <- items[-which(items %in% "type")]
  for (item in items) {
    message(item)
    data_to_store <- framework.output[[item]]
    dir_item <- here::here(glue::glue("{dir}/{item}"))
    if (!dir.exists(dir_item)) {
      dir.create(dir_item)
      
      arrow::write_dataset(data_to_store,
                           dir_item,
                           format = "parquet",
                           partitioning = "YearMonth")
    } else {
      AppendData(data_to_store, dir_item)
    }
  }
}

AtomicParquetExtraction <- function(variable, date_variable, K, LAG, index, 
                                    start_date, end_date, dir) {
  
  data <- glue::glue("{dir}/{index}/K={K}/LAG={LAG}/{variable}") |> 
    arrow::open_dataset() |> 
    dplyr::collect() |> 
    dplyr::arrange(Dates) |> 
    dplyr::filter(Dates >= start_date,
                  Dates <= end_date) |> 
    dplyr::select(-YearMonth) |> 
    dplyr::rename(!!date_variable := Dates)
  
  if (variable %in% "portfolio.return") {
    dates <- data$Dates
    performance <- dplyr::select(data, -Dates)
    data <- xts::xts(performance,
                     order.by = dates)
  }
  return(data)
}


GetDataFromParquet <- function(K, LAG, index, start_date, end_date, dir) {
  data_names <- c("eigendata_eigenvalues", "eigendata_eigenvalues.perc",
                  "eigendata_eigenvectors", "range_range.daily",
                  "range_range", "range.graph_range.daily", "range.graph_range",
                  "trades", "trades.graph", "portfolio.return")
  
  date_names <- c(rep("Dates", 3), rep(c("dates", "t"), 2), rep("time", 2),
                  "Dates")
  
  data_output <- c("eigenvalues", "eigenvalues.perc",
                   "eigenvectors", "replica.daily",
                   "range", "replica.daily_graph", "range_graph",
                   "trades", "trades_graph", "performance")
  
  algo_results <- purrr::map2(data_names, date_names, 
                              ~AtomicParquetExtraction(.x, .y, K, LAG, index, start_date, 
                                                       end_date, dir)) |> 
    purrr::set_names(data_output)
  
  return(algo_results)
}


