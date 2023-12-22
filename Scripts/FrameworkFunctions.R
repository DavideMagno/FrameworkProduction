
`%not_in%` <- Negate("%in%")

CalculatePrices <- function(LAG, dir, start_date = as.Date("2000-01-03"),
                            end_date = Sys.Date() - lubridate::days(1),
                            indices = NULL) {
  
  prices <- arrow::open_dataset(dir) |> 
    dplyr::collect()
  
  if (!is.null(indices)) {
    prices <- prices |> 
      dplyr::select(date, dplyr::contains(indices), YearMonth)
  }
  
  prices <- prices |> 
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
  
  prices <- prices |>
    purrr::pluck(object) |> 
    dplyr::select(-dates) 
  
  purrr::pmap(list(x = as.list(colnames(prices)),
                   y = prices,
                   z = type), 
              function(x, y, z) CalculateInvariant(index = x, data = y, traded.asset = z, 1)) |> 
    purrr::list_cbind() |> 
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
    load(glue::glue("~/Documents/R/Meyrick/FrameworkData/Data/IndexMapping{asset_manager}.RData"))
  } else {
    load(dir)
  }
  
  type <- tibble::tibble(Index = tail(colnames(prices$prices),-1)) |> 
    dplyr::left_join(FrameworkIndices, by = "Index") |> 
    dplyr::mutate(type_flag = dplyr::if_else(grepl("Log", invariant), TRUE, FALSE)) |> 
    dplyr::pull(type_flag) 
  
  return(type)
}

CalculateTypeLocalIndex <- function(prices, asset_manager, index, dir = NULL) {
  if (is.null(dir)) {
    load(glue::glue("~/Documents/R/Meyrick/FrameworkData/Data/IndexMapping{asset_manager}.RData"))
  } else {
    load(dir)
  }
  
  type <- tibble::tibble(Index = tail(colnames(prices$prices),-1)) |> 
    dplyr::left_join(FrameworkIndices, by = "Index") |> 
    dplyr::mutate(type_flag = dplyr::if_else(grepl("Log", invariant), TRUE, FALSE)) |> 
    dplyr::filter(Index %in% index) |> 
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
  
  cone <- SimulateConeParallel(1,returns$returns.monthly.cut,prices$prices.monthly.cut,
                               index, K, type, LAG, ROLLING.WINDOW)
  
  return(cone)
}

FrameworkOptimised <- function(indices, prices, returns, types, K, ROLLING.WINDOW, 
                               LAG, am, time_limit_list, dir_output) {
  # message(glue::glue("Calculating SVD for K = {K}"))
  
  svd <- RunSVDAnalysis(1,returns$returns.monthly.cut,prices$prices.monthly.cut, 
                        K, LAG, ROLLING.WINDOW, parallel = TRUE)
  
  cones <- purrr::map2(as.list(indices)[649], as.list(types)[649],
                       ~CalculateConeAndTrades(svd, prices, returns,
                                               .x, .y, K, time_limit_list, LAG, 
                                               am, dir_output))
  
  return(cones)
}

CalculateConeAndTrades <- function(svd, prices, returns, index_chosen, type, K,
                                   time_limit_list, LAG, ROLLING.WINDOW,
                                   am, dir_output) {
  
  cone <- CalculateConeVector(svd, prices$prices.monthly.cut, index_chosen, type,
                              K, LAG, ROLLING.WINDOW)
  
  calculation.dates <- cone$eigendata$eigenvectors$Dates
  
  n.max <- nrow(cone$prices)
  
  returns <- returns$returns.cut[c("dates", index_chosen)] |> 
    dplyr::rename_all(~c("dates", "index.strategy")) |> 
    na.omit()
  
  daily.prices <- prices$prices.cut[c("dates", index_chosen)] |> 
    dplyr::rename_all(~c("dates", "index.strategy")) |> 
    na.omit()
  
  purrr::walk(time_limit_list,
              ~CalculateTradingStrategy(cone, calculation.dates,
                                        returns, daily.prices, type, .x,
                                        index_chosen, K, LAG, am, dir_output))
}


Framework <- function(index, prices, returns, type, K, ROLLING.WINDOW, LAG, am,
                      time_limit_list) {
  # message(glue::glue("* Index: {index} *"))
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
  
  tictoc::tic()
  purrr::walk(time_limit_list,
              ~CalculateTradingStrategy(corridor_data, calculation.dates,
                                        returns, daily.prices, type, .x,
                                        index, K, LAG, am))
  tictoc::toc()
}

CalculateTradingStrategy <- function(corridor_data, calculation.dates,
                                     returns, daily.prices, type, time_limit,
                                     index, K, LAG, am, dir_output) {
  message(glue::glue("* Time Limit: {time_limit} *"))
  if (!is.infinite(time_limit)) {
    LAG <- time_limit
  } 
  
  dir <- glue::glue("{dir_output}/{index}/K={K}/LAG={LAG}")
  
  trading <- RelativeTradingCone(corridor_data, calculation.dates,
                                 daily.returns = returns, 
                                 daily.prices = daily.prices,
                                 type = type, time_limit = time_limit, dir = dir)
  
  #Fixing data for automatic parquet storage
  trading$indices <- trading$indices |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$raw.returns <- NULL
  
  # trading$raw.returns <- trading$raw.returns |> 
  #   dplyr::select(Dates, dplyr::everything()) |> 
  #   dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$trades <- trading$trades |> 
    dplyr::rename("Dates" = time) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$eigendata <- corridor_data$eigendata 
  
  trading$eigendata$eigenvectors <- trading$eigendata$eigenvectors |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$eigendata$eigenvalues <- trading$eigendata$eigenvalues |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$eigendata$eigenvalues.perc <- trading$eigendata$eigenvalues.perc |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$range$range <- trading$range$range |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m")) |> 
    tibble::as_tibble()
  
  trading$range$range.daily <- trading$range$range.daily |>
    dplyr::rename("Dates" = dates) |> 
    dplyr::select(Dates, dplyr::everything()) |> 
    dplyr::mutate(YearMonth = format(Dates, "%Y-%m"))
  
  trading$range.graph$range <- trading$range.graph$range |> 
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
  
  WriteFullIndexOutput(trading, index, K, LAG, time_limit, dir_output)
}

AppendData <- function(data_to_store, dir_item) {
  
  stored_data <- arrow::open_dataset(dir_item) |> 
    dplyr::collect() |> 
    dplyr::arrange(Dates) 
  
  stored_data_dates <- stored_data$Dates
  new_data_dates <- data_to_store$Dates
  delta_dates <- setdiff(new_data_dates, stored_data_dates)
  
  new_data <- data_to_store |> 
    dplyr::filter(Dates %in% as.Date(delta_dates))
  
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

CheckAppend <- function(data_to_store, dir_item) {
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


WriteFullIndexOutput <- function(framework.output, index, K, LAG, TIME, dir_output) {
  message(glue::glue("Storing time limit: {TIME}"))

  if (!is.infinite(TIME)) {
    LAG <- TIME
  }
  dir <- glue::glue("{dir_output}/{index}/K={K}/LAG={LAG}")
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
  framework.output <- purrr::list_flatten(framework.output)
  items <- names(framework.output) 
  items <- items[-which(items %in% "type")]
  for (item in items) {
    data_to_store <- framework.output[[item]]
    dir_item <- glue::glue("{dir}/{item}")
    CheckAppend(data_to_store, dir_item)
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
  
  if (variable %in% c("portfolio.return", "portfolio.return_weighted")) {
    dates <- data$Dates
    performance <- dplyr::select(data, -Dates)
    data <- xts::xts(performance,
                     order.by = dates)
  }
  return(data)
}


GetDataFromParquet <- function(K, LAG, index, start_date, end_date, dir,
                               flag_weighted = FALSE) {
  data_names <- c("eigendata_eigenvalues", "eigendata_eigenvalues.perc",
                  "eigendata_eigenvectors", "range_range.daily",
                  "range_range", "range.graph_range.daily", "range.graph_range",
                  "trades", "trades.graph", "portfolio.return")
  
  date_names <- c(rep("Dates", 3), rep(c("dates", "t"), 2), rep("time", 2),
                  rep("Dates", 1))         
  
  data_output <- c("eigenvalues", "eigenvalues.perc",
                   "eigenvectors", "replica.daily",
                   "range", "replica.daily_graph", "range_graph",
                   "trades", "trades_graph", "performance")
  
  if (flag_weighted) {
    data_names <- c(data_names, "portfolio.return_weighted", "range_range_weighted",
                    "range.graph_range_weighted", "trades_weighted",
                    "trades.graph_weighted")
    
    date_names <- c(date_names, rep("Dates", 1), rep("t", 2), rep("time", 2))
    
    data_output <- c(data_output, "performance_weighted",
                     "range_weighted", "range_graph_weighted", "trades_weighted", 
                     "trades_graph_weighted")
  }
  
  algo_results <- purrr::map2(data_names, date_names, 
                              ~AtomicParquetExtraction(.x, .y, K, LAG, index, start_date, 
                                                       end_date, dir)) |> 
    purrr::set_names(data_output)
  
  return(algo_results)
}

# Weighting for information ratio -----------------------------------------

calculate_average_info_ratio <- function(timeseries, average_flag) {
  info_ratio <- timeseries$`Information Index`
  
  mean_info_ratio <- cumsum(info_ratio[1:252])/1:252
  
  for (i in 253:length(info_ratio)) {
    mean_info_ratio <- c(mean_info_ratio, 
                         mean(info_ratio[(i - 252):i]))
  }
  
  timeseries <- timeseries |> 
    dplyr::mutate(`Mean Information Index` = mean_info_ratio)
  
  return(timeseries)
}

weighted_corridors_calculation <- function(output_distance, prices, returns, 
                                           index, k, asset.manager, start_date, 
                                           end_date, dir_base) {
  message(glue::glue("{index} - K:{k}"))

  output_distance <- output_distance |> 
    dplyr::select(Dates, `Mean Information Index`)
  
  tables <- c("range_range", "range.graph_range")
  
  projection <- list()
  
  for (table in tables) {
    table_name <- stringr::str_extract(table, ".+(?=_)")
    dir <- glue::glue("{dir_base}/Data/output{asset.manager}/{index}/K={k}/LAG=252/{table}")
    
    new_table <- dir |> 
      arrow::open_dataset() |> 
      dplyr::arrange(Dates) |> 
      dplyr::collect() |> 
      dplyr::filter(Dates >= start_date, Dates <= end_date) |> 
      dplyr::left_join(output_distance, by = "Dates") |> 
      tidyr::replace_na(list(`Mean Information Index` = 1)) |> 
      dplyr::mutate(width = up - down,
                    mean = (up + down)/2,
                    up = mean + width/2*`Mean Information Index`,
                    down = mean - width/2*`Mean Information Index`) |> 
      dplyr::select(-width, -mean, -`Mean Information Index`)
    
    projection[[table_name]] <- new_table
  }
  
  projection$range_daily <- projection$range |> 
    dplyr::select(dates = Dates, index.strategy = index)
  
  dir_indices <- glue::glue("{dir_base}/Data/IndexMapping{asset.manager}.RData")
  
  projection$type = CalculateTypeLocalIndex(prices, asset.manager, index,
                                            dir_indices)
  
  time_limits <- c(Inf, 5:10)
  
  for (time_limit in time_limits) {
    message(glue::glue("TRADE LAG: {time_limit}"))
    trading <- CalculateTradingStrategyRebalanced(projection, returns, index, k, 
                                                  time_limit, asset.manager, dir_base)
    
    trading$trades <- trading$trades |> 
      dplyr::rename("Dates" = time) |> 
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
    
    if (is.infinite(time_limit)) {
      time_limit <- 252
    }
    
    dir <- glue::glue("{dir_base}/Data/output{asset.manager}/{index}/K={k}/LAG={time_limit}/range_range_weighted")
    CheckAppend(projection$range, dir)
    
    dir <- glue::glue("{dir_base}/Data/output{asset.manager}/{index}/K={k}/LAG={time_limit}/range.graph_range_weighted")
    CheckAppend(projection$range.graph, dir)
    
    dir <- glue::glue("{dir_base}/Data/output{asset.manager}/{index}/K={k}/LAG={time_limit}/trades_weighted")
    CheckAppend(trading$trades, dir)
    
    dir <- glue::glue("{dir_base}/Data/output{asset.manager}/{index}/K={k}/LAG={time_limit}/trades.graph_weighted")
    CheckAppend(trading$trades.graph, dir)
    
    dir <- glue::glue("{dir_base}/Data/output{asset.manager}/{index}/K={k}/LAG={time_limit}/portfolio.return_weighted")
    CheckAppend(trading$portfolio.return, dir)
    
  }
}

CalculateTradingStrategyRebalanced <- function(projection, returns, index, K, time_limit, am, dir_base) {
  
  if (is.infinite(time_limit)) {
    LAG <- 252
  } else {
    LAG <- time_limit
  }
  dir <- glue::glue("{dir_base}/Data/output{am}/{index}/K={K}/LAG={LAG}")
  calculation.dates <- glue::glue("{dir}/eigendata_eigenvectors") |> 
    arrow::open_dataset() |> 
    dplyr::collect() |> 
    dplyr::pull(Dates)
  
  returns <- returns$returns.cut[c("dates", index)] |> 
    dplyr::rename_all(~c("dates", "index.strategy")) |> 
    na.omit()
  
  trades <- RelativeTradingCone(projection, calculation.dates, weighted_flag = TRUE,
                                type = projection$type, time_limit = time_limit,
                                daily.returns = returns, dir = dir)
  
  return(trades)
  
}

normalise_corridor <- function(distance, prices, returns, k, asset.manager,
                               start_date, end_date, dir_base, average_flag) {

  trim_date <- min(distance$Dates)
  
  if (average_flag) {
    past_distance <- glue::glue("{dir_base}/Data/output_distance{asset.manager}/K={k}") |> 
      arrow::open_dataset() |> 
      dplyr::arrange(Index, Dates)|> 
      dplyr::collect() |> 
      dplyr::filter(Dates < trim_date | Dates > end_date) 
    
    distance <- past_distance |> 
      dplyr::bind_rows(distance)
  }
  
  output_distance <- distance |> 
    dplyr::group_nest(Index) |> 
    dplyr::mutate(data = purrr::map(data, ~calculate_average_info_ratio(.x, average_flag))) |> 
    tidyr::unnest(data) |> 
    dplyr::filter(Dates >= trim_date)
  
  output_distance |>
    dplyr::group_nest(Index) |>
    purrr::transpose() |>
    purrr::walk(~weighted_corridors_calculation(.x$data, prices, returns, .x$Index, k,
                                                asset.manager, trim_date, end_date,
                                                dir_base),
                .progress = TRUE)
  
  return(output_distance)
}


