
# Algorithm ---------------------------------------------------------------

RelativeTradeAlgorithm <- function(scaled.data, 
                                   threshold, index.flag, delay = 0) {
  over.count <- 0
  under.count <- 0
  flag.enter <- FALSE
  flag.in <- FALSE
  # flag.out <- FALSE
  
  trades <- tibble::tibble(time = seq(from = 1, to = nrow(scaled.data), by = 1), 
                           trade.trigger.in = NA_real_,
                           trade.trigger.out = NA_real_,
                           Long.in = NA_real_,
                           Short.in = NA_real_,
                           Long.out = NA_real_,
                           Short.out = NA_real_,
                           w1 = ifelse(index.flag, 0, 0.5),
                           w2 = ifelse(index.flag, 0, 0.5))
  
  for (i in 1:(nrow(trades))) {
    check <- scaled.data$distance[i] < threshold
    under.count <- ifelse(check, under.count + 1, 0)
    over.count <- ifelse(check, 0, over.count + 1)
    flag.in <- ifelse(!flag.in & !flag.enter, FALSE, 
                      ifelse(!flag.out, TRUE, FALSE))
    flag.out <- flag.in & under.count == (delay + 1)
    flag.enter <- ifelse(!flag.in, over.count == delay + 1, FALSE)
    
    if (flag.enter) {
      trades$trade.trigger.in[i] <- threshold 
      if(scaled.data$asset.1[i] > scaled.data$asset.2[i]) {
        asset.long <- "Asset 2"
        trades$w1[i] <- -1
        trades$Short.in[i] <- scaled.data$asset.1[i]
        if (index.flag) {
          trades$w2[i] <- 0
        } else {
          trades$w2[i] <- 1
          trades$Long.in[i] <- scaled.data$asset.2[i]
        }
      } else {
        asset.long <- "Asset 1"
        trades$w1[i] <- 1
        trades$Long.in[i] <- scaled.data$asset.1[i]
        if (index.flag) {
          trades$w2[i] <- 0
        } else {
          trades$w2[i] <- -1
          trades$Short.in[i] <- scaled.data$asset.2[i]
        }
      }
    }
    
    if (flag.out) {
      trades$trade.trigger.out[i] <- threshold
      if (index.flag) {
        trades$w1[i] <- 0
        trades$w2[i] <- 0
      } else {
        trades$w1[i] <- 0.5
        trades$w2[i] <- 0.5
      }
      if (grepl("Asset 1",asset.long)) {
        trades$Long.out[i] <- scaled.data$asset.1[i]
        if (!index.flag) {
          trades$Short.out[i] <- scaled.data$asset.2[i]
        }
      } else {
        trades$Short.out[i] <- scaled.data$asset.1[i]
        if (!index.flag) {
          trades$Long.out[i] <- scaled.data$asset.2[i]
        }
      }
    }
    
    if (flag.in) {
      trades$w1[i] <- trades$w1[i - 1]
      trades$w2[i] <- trades$w2[i - 1]
    }
  }
  
  return(trades)
}

PrepareRange <- function(projection, dates, normalise = TRUE, 
                         daily.prices = NULL) {
  
  actual <- projection$graph.index
  
  if(normalise) {
    projection$prices$mean <- rowMeans(projection$prices[,c("up", "down")]) 
    
    range <- projection$prices |> 
      dplyr::mutate(up = up - mean,
                    down = down - mean) |> 
      head(nrow(actual)) |> 
      dplyr::left_join(actual, by = "Dates") |> 
      dplyr::mutate(index = index - mean) 
  } else {
    range <- projection$prices |> 
      dplyr::left_join(actual, by = "Dates") 
  }
  
  if (!is.null(daily.prices)) {
    if (normalise) {
      
      projection$prices <- projection$prices |> 
        dplyr::select(-Dates)
      
      range.daily <- daily.prices |> 
        dplyr::left_join(range, by = c("dates" = "Dates")) |> 
        dplyr::select(dates, index.strategy, mean) |> 
        tidyr::fill(mean) |> 
        dplyr::mutate(index.strategy = index.strategy-mean) |> 
        dplyr::select(-mean)
    } else {
      range.daily <- daily.prices
    }
  } else {
    range.daily <- NULL
  }
  
  return(list(range = range, range.daily = range.daily))
}

RelativeTradeAlgorithmCone <- function(range_new, delay = 0, updateProgress = NULL,
                                       resetProgress = FALSE, daily.prices = NULL,
                                       time_limit = Inf, dir_trade = NULL, 
                                       dir_range = NULL) {
  
  over.count <- 0
  under.count <- 0
  in.count <- 0
  flag.in <- FALSE
  flag.long <- FALSE
  flag.short <- FALSE
  
  if (dir.exists(dir_trade)) {
    trades_past_history <- dir_trade |>
      arrow::open_dataset() |>
      dplyr::select(-YearMonth) |>
      dplyr::arrange(Dates) |>
      dplyr::collect()
    
    daily.prices <- NULL
    
    range_past_history <- dir_range |> 
      arrow::open_dataset() |> 
      dplyr::select(-YearMonth) |> 
      dplyr::arrange(Dates) |> 
      dplyr::collect()
    
    dates <- union(trades_past_history$Dates, range_new$Dates) ###CHANGE
    # dates <- setdiff(range_new$Dates, range_past_history$Dates) |> 
    #   as.Date(origin = "1970-01-01")
    
    trades <- tibble::tibble(time = as.Date(dates, origin = "1970-01-01"), 
                             out.count = NA_integer_,
                             Long.in = NA_real_,
                             Short.in = NA_real_,
                             Long.out = NA_real_,
                             Short.out = NA_real_,
                             w1 = 0,
                             w2 = 1,
                             trade = NA_character_)
    
    range <- range_past_history |> 
      # dplyr::filter(Dates < dates[1]) |>  ###CHANGE
      dplyr::bind_rows(range_new) |> 
      # dplyr::bind_rows(dplyr::filter(range_new, Dates %in% dates)) |> 
      dplyr::arrange(Dates)
    
    # trades <- trades_past_history |> 
    #   dplyr::filter(time < dates[1]) |>  ###CHANGE
    #   dplyr::bind_rows(trades_new) |> 
    #   dplyr::arrange(time)
    
    adj_factor <- 0
    start_index <- 1
    
  } else {
    range <- range_new
    trades_past_history <- NULL
    dates <- range$Dates
    adj_factor <- 0
    start_index <- 1
    trades <- tibble::tibble(time = range$Dates, 
                             out.count = NA_integer_,
                             Long.in = NA_real_,
                             Short.in = NA_real_,
                             Long.out = NA_real_,
                             Short.out = NA_real_,
                             w1 = 0,
                             w2 = 1,
                             trade = NA_character_)
  }
  
  if (!is.null(daily.prices)) {
    # trading.time <- which(daily.prices$dates %in% trades$time) + 1
    trading.time <- which(daily.prices$dates %in% trades$time)
    trading.price <- daily.prices[trading.time,] |> 
      dplyr::pull(2)
    
    # trades$time <- daily.prices[trading.time,] |> 
    #   dplyr::pull(1)
  } else {
    trading.price <- range$index
  }
  
  for (i in start_index:(nrow(trades))) {
    if (is.function(updateProgress)) {
      text <- glue::glue("Calculating trades at time {i} of \\
                         {nrow(trades)}")
      updateProgress(value = i, detail = text, reset = resetProgress)
      resetProgress <- FALSE
    }
    # Checks
    if (range$index[i] > 0) {
      check.up <- range$index[i] > range$up[i]
      check.down <- range$index[i] < range$down[i]
    } else {
      check.down <- range$index[i] > range$up[i]
      check.up <- range$index[i] < range$down[i]
    }
    
    if (i > 1) {
      if (range$index[i] > 0) {
        check.up.minus1 <- range$index[i-1] > range$up[i-1]
        check.down.minus1 <- range$index[i-1] < range$down[i-1]
      } else {
        check.down.minus1 <- range$index[i-1] > range$up[i-1]
        check.up.minus1 <- range$index[i-1] < range$down[i-1]
      }
    } else {
      check.up.minus1 <- FALSE
      check.down.minus1 <- FALSE
    }
    check.range <- !(check.up | check.down)
    # Counts
    under.count <- ifelse(check.down, under.count + 1, 0)
    over.count <- ifelse(check.up, over.count + 1, 0)
    trades$out.count[i] <- under.count + over.count
    in.count <- ifelse(check.range, in.count + 1, 0)
    switch.count <- ifelse(i < (delay + 1), NA_integer_, trades$out.count[i-(delay+1)])
    # Flags
    if (i > 1) {
      flag.switch <- ifelse(((check.up.minus1 & check.down) | (check.down.minus1 & check.up)) &
                              (trades$out.count[i-1] <= time_limit), TRUE, FALSE)
    } else {
      flag.switch <- FALSE
    }
    # flag.switch <- ifelse(trades$out.count[i] == (delay + 1) & 
    #                         !is.na(switch.count) &
    #                         switch.count > 0,
    #                       TRUE, FALSE)
    flag.enter <- ifelse((!flag.in | flag.switch) & (trades$out.count[i] == (delay + 1)),
                         TRUE, 
                         FALSE)
    flag.out <- ifelse(((flag.in & (flag.switch | in.count == (delay + 1)) & 
                           !flag.switch) | (trades$out.count[i] == (time_limit + 1))),
                       TRUE, FALSE)
    flag.in <- ifelse((((!flag.in & flag.enter) | (flag.in & !flag.out)) & 
                         (trades$out.count[i] <= time_limit)),
                      TRUE, FALSE)
    # Levels
    
    trades$Long.in[i] <- ifelse((flag.switch & (under.count == (delay + 1))) |
                                  (flag.enter & under.count == (delay + 1)),
                                trading.price[i], NA_real_)
    trades$Short.in[i] <- ifelse((flag.switch & (over.count == (delay + 1))) |
                                   (flag.enter & over.count == (delay + 1)),
                                 trading.price[i], NA_real_)
    trades$Long.out[i] <- ifelse((flag.switch & flag.long) |
                                   (flag.out & flag.long),
                                 trading.price[i], NA_real_)
    trades$Short.out[i] <- ifelse((flag.switch & flag.short) |
                                    (flag.out & flag.short),
                                  trading.price[i], NA_real_)
    
    # Flags post trade
    if (i == 1) {
      flag.long <- ifelse(!is.na(trades$Long.in[i]),
                          TRUE, FALSE)
      flag.short <- ifelse(!is.na(trades$Short.in[i]),
                           TRUE, FALSE)
      
    } else {
      flag.long <- ifelse(!is.na(trades$Long.in[i]),
                          TRUE,
                          ifelse(flag.in, 
                                 ifelse(flag.switch, !flag.long, flag.long),
                                 FALSE))
      flag.short <- ifelse(!is.na(trades$Short.in[i]),
                           TRUE,
                           ifelse(flag.in, 
                                  ifelse(flag.switch, !flag.short, flag.short),
                                  FALSE))
    }
    
    # Weight
    trades$w1[i] <- ifelse(flag.long, 1, ifelse(flag.short, -1, 0))
    trades$w2[i] <- 1 - trades$w1[i]
    trades$trade[i] <- dplyr::case_when(
      trades$w1[i] == 1 ~ "Below the corridor",
      trades$w1[i] == -1 ~ "Above the corridor",
      trades$w1[i] == 0 & check.up ~ "Above the corridor but time limit reached",
      trades$w1[i] == 0 & check.down ~ "Below the corridor but time limit reached",
      TRUE ~ "Within the corridor"
    )
  }
  
  trades <- tail(trades, length(range_new$Dates) + adj_factor)
  # trades <- trades |>
  #   dplyr::select(-out.count)
  
  return(list(trades = trades, adj_factor = adj_factor))
}

CalculateStatisticsRT <- function(relative.trade) {
  table_long <- CalculateStatistics("Long", relative.trade$type, 
                                    relative.trade$trades)
  table_short <- CalculateStatistics("Short", relative.trade$type, 
                                     relative.trade$trades)

  stats <- dplyr::bind_rows(table_long, table_short) |> 
    dplyr::filter(trade.in != trade.out)
  
  return(stats)
}

RelativeTradingCone <- function(projection, 
                                dates, lag.window = 0, type = TRUE, index.flag = TRUE,
                                backtest.return = TRUE, daily.returns = NULL,
                                daily.prices = NULL, updateProgress = NULL,
                                time_limit = Inf, dir = NULL, weighted_flag = FALSE) {
  
  if (weighted_flag) {
    range_range <- projection$range
    range.graph <- projection$range.graph
    daily_prices <- projection$range_daily
    dir_trade <- glue::glue("{dir}/trades_weighted")
    dir_range <- glue::glue("{dir}/range_range_weighted")
  } else {
    range <- PrepareRange(projection, dates, normalise = FALSE,
                          daily.prices = daily.prices)
    range_range <- range$range
    range.graph <- PrepareRange(projection, dates, normalise = TRUE,
                                daily.prices = daily.prices)
    dir_trade <- glue::glue("{dir}/trades")
    dir_range <- glue::glue("{dir}/range_range")
    daily_prices <- range$range.daily
  }
  
  trades <- RelativeTradeAlgorithmCone(range_range, delay = lag.window,
                                       updateProgress = updateProgress,
                                       resetProgress = FALSE, 
                                       daily.prices = daily_prices,
                                       time_limit, dir_trade, dir_range)
  
  adj_factor <- trades$adj_factor
  trades <- trades$trades |> 
    dplyr::arrange(time)
  
  trades.graph <- trades |> 
    dplyr::left_join(range_range, by = c("time" = "Dates")) 
  
  trades.graph$mean <- rowMeans(trades.graph[,c("down", "up")])
  
  trades.graph <- trades.graph |> 
    dplyr::mutate(dplyr::across(.cols = c(dplyr::contains("Short"),
                                          dplyr::contains("Long")),
                                .fns = function(x) {x - mean})) |> 
    dplyr::select(-down, -up, -index, -pcx_replica, -mean)
  
  if (length(dates) == 1) {
    dates <- Sys.Date() + lubridate::days(1:length(t))
  }

  if (backtest.return) {
    if (!is.null(daily.returns)) {
      
      trade.returns <- daily.returns |> 
        dplyr::left_join(trades, by = c("dates" = "time")) |> 
        dplyr::filter(dates >= trades$time[1]) |> 
        tidyr::fill(w1, w2, trade) |> 
        dplyr::select(-dplyr::contains("Long"),-dplyr::contains("Short"))
      trade.dates <- trade.returns$dates
      returns.xts <- xts::xts(cbind(dplyr::pull(trade.returns, 2),
                                    rep(0, nrow(trade.returns)),
                                    rep(0, nrow(trade.returns))), 
                              order.by = trade.dates)
      weights <- xts::xts(cbind(trade.returns$w1, trade.returns$w2, 
                                1-trade.returns$w1-trade.returns$w2), 
                          order.by = trade.dates)
      long.only <- xts::xts(dplyr::pull(trade.returns, 2), 
                            order.by = trade.dates)
    } else {
      
      N <- nrow(projection$prices)
      trade.dates <- as.Date(tail(dates, N))
      trade.returns <- tail(projection$returns, N) 
      returns.xts <- xts::xts(cbind(dplyr::pull(trade.returns, index),
                                    rep(0, nrow(trade.returns)),
                                    rep(0, nrow(trade.returns))), 
                              order.by = trade.dates)
      weights <- xts::xts(cbind(trades$w1, trades$w2, 
                                1-trades$w1-trades$w2), 
                          order.by = trade.dates)
      long.only <- xts::xts(dplyr::pull(trade.returns, index), 
                            order.by = trade.dates)
    }
    
    weights.long.only <- xts::xts(rep(1, length(trade.dates)), 
                                  order.by = trade.dates)
    
    if (nrow(returns.xts) == 1) {
      r.strategy <- returns.xts*weights
      r.strategy <- r.strategy[,1]
      r.long.only <- long.only*weights.long.only
    } else {
      r.strategy <- PerformanceAnalytics::Return.portfolio(returns.xts, 
                                                           weights = weights,
                                                           rebalance_on = "days", 
                                                           geometric = type)
      
      r.long.only <- PerformanceAnalytics::Return.portfolio(long.only, weights = weights.long.only,
                                                            rebalance_on = "days", geometric = type)
    }
    
    
    if (!index.flag) {
      r.equal <- Return.portfolio(returns.xts, weights = c(rep(0.5,2),0),
                                  rebalance_on = "days", geometric = type)
      
      r  <- cbind(r.strategy, r.equal) 
      
      dimnames(r)[[2]] <- c("Relative Value Strategy", "Equal Weights")
    } else {
      r <- cbind(r.long.only, r.strategy)
      colnames(r)[[1]] <- "Long Only Strategy"
      colnames(r)[[2]] <- "Relative Value Strategy"
    }
  } else {
    r <- xts::xts(0, trade.dates)
  }
  
  if (adj_factor > 0) {
    trades <- tail(trades, -1)
    trades.graph <- tail(trades.graph, -1)
  }
  
  return(list(trades = trades, portfolio.return = r, 
              indices = projection$prices,
              raw.returns = projection$returns,
              range = range, range.graph = range.graph, 
              trades.graph = trades.graph, type = projection$type))
}

RelativeTrading <- function(prices, returns,threshold, delay,
                            principal.components, dates, index.flag = TRUE) {
  
  if (length(dates) == 1) {
    dates <- Sys.Date() + lubridate::days(1:length(t))
  }
  
  trade.returns <- tail(returns, nrow(prices))
  trade.dates <- as.Date(tail(dates, nrow(prices)))
  returns.xts <- xts::xts(cbind(trade.returns$asset.1,
                                trade.returns$asset.2,
                                rep(0, nrow(trade.returns))), 
                          order.by = trade.dates)
  
  weights <- xts::xts(cbind(trades$w1, trades$w2, 
                            1-trades$w1-trades$w2), 
                      order.by = trade.dates)
  
  r.strategy <- PerformanceAnalytics::Return.portfolio(returns.xts, weights = weights)
  
  if (!index.flag) {
    r.equal <- PerformanceAnalytics::Return.portfolio(returns.xts, weights = c(rep(0.5,2),0),
                                                      rebalance_on = "days")
    
    r  <- cbind(r.strategy, r.equal) 
    
    dimnames(r)[[2]] <- c("Relative Value Strategy", "Equal Weights")
  } else {
    r <- r.strategy
    dimnames(r)[[2]] <- "Relative Value Strategy"
  }
  
  return(list(trades = trades, portfolio.return = r, 
              indices = prices,
              raw.returns = returns))
  
}

SelectAssetForCone <- function(data, name, asset.chosen) {
  
  asset <- data[,grepl(paste0(asset.chosen,"_approx"), colnames(data))]
  colnames(asset) <- name
  return(asset)
}

CalculateCone <- function(training.set, training.set.prices, svd.training.indices, 
                          svd.analysis.prices, lag, index.chosen, type, interval) {
  
  names <- c("down", "median", "up")
  
  ## Index
  training.index <- training.set.prices[[index.chosen]]
  
  svd.training.index <- svd.analysis.prices$reconstructed.indices[[glue::glue("{index.chosen}_approx")]]
  index <- tibble::tibble(index = training.index,
                          pcx_replica = svd.training.index) 
  
  if (type) {
    index <- index |> 
      dplyr::mutate(pcx_replica = dplyr::if_else(pcx_replica < 0, 1e-9,
                                                 pcx_replica))
  }
  
  
  # Returns
  
  ret.index <- index |> 
    purrr::map2(colnames(index),
                ~CalculateInvariant(.y, .x, type, lag)) |> 
    purrr::list_cbind()
  
  # Projected Cone
  
  svd.returns <- svd.training.indices$proj.returns |> 
    purrr::map2_dfc(names, ~SelectAssetForCone(.x, .y, index.chosen)) 
  
  svd.returns <- svd.returns |> 
    tail(interval) |> 
    dplyr::select(-median) |> 
    dplyr::rowwise() |> 
    dplyr::mutate(
      temp_down = min(up, down),
      temp_up = max(up, down),
      down = min(temp_up, temp_down),
      up = max(temp_up, temp_down)
    ) |> 
    dplyr::ungroup() |> 
    dplyr::select(-dplyr::contains("temp")) 
  
  if (type) {
    range <- exp(svd.returns) * tail(svd.training.index, 1)
  } else {
    range <- svd.returns*100 + tail(svd.training.index, 1)
  }
  return(list(ret.index = ret.index, index = tail(index, 1), range = range))
  
}

SelectAssetForConeVector <- function(list_to_flatten, index_chosen) {
  list_to_flatten |> 
    purrr::map(~.x[[glue::glue("{index_chosen}_approx")]]) |> 
    purrr::map(~tail(.x, 1))|> 
    purrr::list_c()
}

CalculateConeVector <- function(svd_results, universe_prices, index_chosen, type, 
                                k, LAG, ROLLING.WINDOW) {
  message(glue::glue("Calculating the cone for index = {index_chosen}"))
  ## Index
  
  universe_prices_index_chosen <- universe_prices |> 
    dplyr::select(dates, index_chosen) |> 
    na.omit()
  
  training.index <- universe_prices_index_chosen[[index_chosen]]
  
  svd_dates <- svd_results$eigendata$eigenvalues$Dates
  first_date <- universe_prices_index_chosen$dates[LAG*ROLLING.WINDOW + 1]
  
  starting_timeframe <- which(svd_dates == first_date)
  
  N <- length(svd_results$reconstructed.indices)
  dates_svd_estimation <- svd_results$eigendata$eigenvalues$Dates[starting_timeframe:N]
  
  svd.training.index <- svd_results$reconstructed.indices[starting_timeframe:N] |> 
    SelectAssetForConeVector(index_chosen) 
  
  svd.training.index_trimmed <- tail(svd.training.index, -1)
  
  complete.svd.training.index <- svd_results$reconstructed.indices[[starting_timeframe]] |> 
    dplyr::pull(glue::glue("{index_chosen}_approx")) |> 
    {\(x) c(x, svd.training.index_trimmed)}()
  
  index <- data.frame(Dates = universe_prices_index_chosen$dates,
                      index = training.index,
                      pcx_replica = complete.svd.training.index) 
  
  # if (type) {
  #   index <- index |> 
  #     dplyr::mutate(pcx_replica = dplyr::if_else(pcx_replica < 0, 1e-9,
  #                                                pcx_replica))
  # }
  
  svd.returns <- svd_results$svd.stresses |> 
    purrr::map(~.x[starting_timeframe:N]) |> 
    purrr::map(SelectAssetForConeVector, index_chosen = index_chosen) |> 
    as.data.frame() |> 
    dplyr::rowwise() |> 
    dplyr::mutate(
      temp_down = min(up, down),
      temp_up = max(up, down),
      down = min(temp_up, temp_down),
      up = max(temp_up, temp_down)
    ) |> 
    dplyr::ungroup() |> 
    dplyr::select(-dplyr::contains("temp")) 
  
  if (type) {
    range <- exp(svd.returns) * svd.training.index
  } else {
    range <- svd.returns*100 + svd.training.index
  }
  
  range <- range |> 
    dplyr::mutate(Dates = dates_svd_estimation) |> 
    dplyr::select(Dates, dplyr::everything())
  
  eigenvectors <- svd_results$eigenvectors[starting_timeframe:N] |> 
    purrr::map(~abs(.x[rownames(.x) %in% index_chosen])) |>
    purrr::map(t) |> 
    purrr::map(as.data.frame) |> 
    purrr::list_rbind()|> 
    dplyr::rename_all(~glue::glue("RC{1:k}")) |> 
    dplyr::mutate(Dates = dates_svd_estimation) |> 
    dplyr::select(Dates, dplyr::everything())
  
  svd_results$eigendata[["eigenvectors"]] <- eigenvectors
  
  return(list(graph.index = index, prices = range, eigendata = svd_results$eigendata))
}

CalculateTimeSeriesSingle <- function(ret, price, type) {
  
  x <- c(0, cumsum(ret))
  
  if (type) {
    prices <- price*exp(x)
  } else {
    prices <- price + x*100
  }
  
  return(prices)
}

CalculateTimeSeries <- function(training.indices, svd.training.indices,
                                prices, asset.1, type) {
  
  data <- cbind(training.indices, svd.training.indices) |> 
    dplyr::select(dplyr::contains(asset.1))
  
  t <- seq(from = 1, to = nrow(training.indices), by = 1)
  
  asset.2 <- glue::glue("{asset.1}_approx")
  
  x.ret <- data[[asset.1]]
  y.ret <- data[[asset.2]]
  
  returns <- tibble::tibble(time = t, asset.1 = x.ret, asset.2 = y.ret)
  
  x <- c(0, cumsum(x.ret))
  y <- c(0, cumsum(y.ret))
  
  # position <- which(colnames(training.indices) %in% asset.1)
  
  price <- prices |> 
    dplyr::pull(asset.1)
  
  if (type) {
    data <- tibble::tibble(asset.1 = price*exp(x), asset.2 = price*exp(y))
  } else {
    data <- tibble::tibble(asset.1 = price + x*100, asset.2 = price + y*100)
  }
  data <- data |> 
    dplyr::mutate(time = c(0,t)) 
  
  return(list(prices = data, returns = returns))
}

IncludeTestingSet <- function(testing.set, training.set, index) {
  testing.set |> 
    dplyr::select(-dates) |> 
    dplyr::slice(index) |> 
    {\(testing.set)dplyr::bind_rows(training.set, testing.set)}()
}

RunSVDAnalysis <- function(interval, whole.set, whole.set.prices, 
                           factors_chosen, lag, rolling.window, 
                           parallel = TRUE, perpetual = FALSE) {
  
  dates_df <- whole.set.prices |> 
    dplyr::pull(dates)
  
  whole.set <- dplyr::select(whole.set, -dates)
  whole.set.prices <- dplyr::select(whole.set.prices, -dates)
  
  first_value <- 1
  
  ending.window <- seq(first_value + rolling.window*lag, 
                       nrow(whole.set.prices), 
                       by = interval) 
  
  final.rebalancing.day <- ending.window[length(ending.window)]
  
  ending.window <- ending.window |> 
    c(nrow(whole.set.prices)) |> 
    unique()
  dates <- dates_df[ending.window]
  
  if (perpetual) {
    starting.window <- rep(1, length(ending.window))
  } else {
    starting.window <- ending.window - rolling.window*lag
  }
  
  whole.set.prices <- as.data.frame(whole.set.prices)
  
  if (parallel) {
    message("Sono dentro")
    doFuture::registerDoFuture()
    future::plan("multisession")
    tictoc::tic()
    svd <- foreach::foreach(index = 1:length(starting.window)) %dopar% {
      RunSVDSingle(whole.set, whole.set.prices, 
                   starting.window,ending.window, factors_chosen, index)
    }
    tictoc::toc()
    message("Sono fuori")
  } else {
    message("Sono dentro")
    svd <- list()
    message(Sys.time())
    tictoc::tic()
    message(glue::glue("Total RUNS {length(starting.window)}"))
    svd <- foreach::foreach(index = 1:length(starting.window)) %do% {
      RunSVDSingle(whole.set, whole.set.prices, 
                   starting.window,ending.window, factors_chosen, index)
    }
    tictoc::toc()
    message("Sono fuori")
  }
  message(Sys.time())
  
  rm(whole.set)
  rm(whole.set.prices)
  rm(starting.window)
  rm(ending.window)
  
  svd_t <- purrr::transpose(svd)
  
  # Extracting information for next steps
  
  eigendata <- purrr::transpose(svd_t$eigendata) |> 
    purrr::map(dplyr::bind_rows) |> 
    purrr::map(~dplyr::mutate(.x, Dates = dates)) |> 
    purrr::map(~dplyr::select(.x, Dates, dplyr::everything()))
  
  reconstructed.indices <- svd_t$svd.analysis.prices 
  
  names <- c("down", "median", "up")
  
  svd.stresses <- svd_t$svd.training.indices |> 
    purrr::map(~purrr::set_names(.x, names)) |> 
    purrr::transpose() |> 
    {\(x) x[!grepl("median", names(x))]}()
  
  eigenvectors <- svd_t$svd.analysis
  
  return(list(eigenvectors = eigenvectors,
              svd.stresses = svd.stresses,
              reconstructed.indices = reconstructed.indices,
              eigendata = eigendata))
}



RunSVDSingle <- function(whole.set, whole.set.prices, starting.window,
                         ending.window, factors_chosen, index, proj.days = 1) {
  if (index/500 == as.integer(index/500)) {
    message(index)
  }
  training.set <- SimulateConeDynamicColumns(whole.set,
                                             starting.window[index],
                                             ending.window[index],
                                             FALSE)
  rm(whole.set)
  svd.analysis <- RunSVD(training.set, factors_chosen) 
  rm(training.set)
  training.set.prices <- SimulateConeDynamicColumns(whole.set.prices,
                                                    starting.window[index],
                                                    ending.window[index],
                                                    TRUE)
  rm(whole.set.prices)
  rm(starting.window)
  rm(ending.window)
  
  svd.analysis.prices <- RunSVD(training.set.prices, factors_chosen) |>  
    purrr::pluck("reconstructed.indices")
  rm(training.set.prices)
  svd.training.indices <- svd.analysis |> 
    ProjectReturns(proj.days) |> 
    purrr::pluck("proj.returns")
  
  eigenvalues.perc.vec <- diag(svd.analysis$S)/sum(diag(svd.analysis$S))
  
  eigenvalues <- t(as.matrix(diag(svd.analysis$S)[1:factors_chosen])) |> 
    as.data.frame() |> 
    dplyr::rename_all(~glue::glue("RC{1:factors_chosen}"))
  
  eigenvalues.perc <- t(eigenvalues.perc.vec[1:factors_chosen]) |> 
    as.data.frame() |> 
    dplyr::rename_all(~glue::glue("RC{1:factors_chosen}"))
  
  return(list(svd.analysis = purrr::pluck(svd.analysis, "L"),
              svd.analysis.prices = svd.analysis.prices,
              svd.training.indices = svd.training.indices,
              eigendata = list(eigenvalues = eigenvalues,
                               eigenvalues.perc = eigenvalues.perc)))
}

ConeAlgo <- function(svd, index.chosen, type) {
  time.series <- CalculateCone(training.set, training.set.prices,
                               svd.training.indices, svd.analysis.prices, lag,
                               index.chosen, type, proj.days)
}

SimulateConeSingle <- function(training.set, training.set.prices, proj.days,
                               factors_chosen, lag, index.chosen, type) {
  
  svd.analysis <- RunSVD(training.set, factors_chosen) 
  
  svd.analysis.prices <- RunSVD(training.set.prices, factors_chosen) 
  svd.training.indices <- svd.analysis |> 
    ProjectReturns(proj.days)
  
  time.series <- CalculateCone(training.set, training.set.prices,
                               svd.training.indices, svd.analysis.prices, lag,
                               index.chosen, type, proj.days)
  
  eigenvectors.index <- rownames(svd.analysis$L) %in% index.chosen
  eigenvalues.perc.vec <- diag(svd.analysis$S)/sum(diag(svd.analysis$S))
  
  eigenvectors <- abs(svd.analysis$L[eigenvectors.index,]) |> 
    t() |> 
    as.data.frame()
  
  eigenvalues <- t(as.matrix(diag(svd.analysis$S)[1:factors_chosen])) |> 
    as.data.frame() |> 
    dplyr::rename_all(~glue::glue("RC{1:factors_chosen}"))
  
  eigenvalues.perc <- t(eigenvalues.perc.vec[1:factors_chosen]) |> 
    as.data.frame() |> 
    dplyr::rename_all(~glue::glue("RC{1:factors_chosen}"))
  
  
  return(list(time.series = time.series, 
              eigendata = list(eigenvectors = eigenvectors,
                               eigenvalues = eigenvalues,
                               eigenvalues.perc = eigenvalues.perc)))
}

SimulateConeDynamicColumns <- function(dataset, start, end, flag) {
  end <- ifelse(flag, end , end - 1) 
  subset <- dplyr::slice(dataset, start:end)
  test.valid.data <- purrr::map_lgl(subset,
                                    ~(any(is.na(.x)) | all(.x == 0) | 
                                        length(unique(.x)) == 1))
  subset <- subset[,!test.valid.data]
  return(subset)
}

SimulateConeParallel <- function(interval, whole.set, whole.set.prices, 
                                 index.chosen, factors_chosen, type, 
                                 lag, rolling.window, once.flag = FALSE,
                                 updateProgress = NULL) {
  
  dates_df <- whole.set.prices |> 
    dplyr::select(dates, dplyr::contains(index.chosen))
  
  whole.set <- dplyr::select(whole.set, -dates)
  whole.set.prices <- dplyr::select(whole.set.prices, -dates)
  
  position <- which(colnames(whole.set) %in% index.chosen)
  type <- type[position]
  first_value <- which.min(is.na(dates_df[[index.chosen]]))
  ending.window <- seq(first_value + rolling.window*lag, 
                       nrow(whole.set.prices), 
                       by = interval) 
  
  final.rebalancing.day <- ending.window[length(ending.window)]
  
  ending.window <- ending.window |> 
    c(nrow(whole.set.prices)) |> 
    unique()
  dates <- dates_df$dates[ending.window]
  starting.window <- ending.window - rolling.window*lag
  
  
  doFuture::registerDoFuture()
  future::plan("multisession")
  # progressr::handlers(global = TRUE)
  # progressr::handlers("progress")
  tictoc::tic()
  training.set <- foreach::foreach(x = starting.window,
                                   y = ending.window) %dopar% {
                                     SimulateConeDynamicColumns(whole.set, x, y, FALSE)
                                   }
  
  training.set.prices <- foreach::foreach(x = starting.window,
                                          y = ending.window) %dopar% {
                                            SimulateConeDynamicColumns(whole.set.prices, x, y, TRUE)
                                          }
  tictoc::toc()
  days.forward <- rep(1, length(training.set))
  tictoc::tic()
  
  
  # for(i in 301:length(training.set)) {
  # # for(i in 1:5727) {
  #   message(i)
  #   browser()
  #   time.series <- SimulateConeSingle(training.set[[i]], training.set.prices[[i]],
  #                                    days.forward[[i]], factors_chosen, lag,
  #                                    index.chosen, type)
  #   browser()
  # }
  
  time.series <- foreach::foreach(x = training.set,
                                  y = training.set.prices,
                                  z = days.forward) %dopar% {
                                    SimulateConeSingle(x, y, z, factors_chosen, lag,
                                                       index.chosen, type)
                                  }
  tictoc::toc()
  
  cone <- time.series |> 
    purrr::transpose() |> 
    purrr::map(purrr::transpose) |> 
    purrr::flatten() |> 
    purrr::map(purrr::list_rbind) |> 
    purrr::map(~dplyr::mutate(.x, Dates = dates))
  
  eingendata <- list()
  eingendata$eigenvectors <- cone$eigenvectors
  eingendata$eigenvalues <- cone$eigenvalues
  eingendata$eigenvalues.perc <- cone$eigenvalues.perc
  
  return(list(prices = cone$range, returns = cone$ret.index, 
              graph.index = cone$index, eingendata = eingendata,
              type = type))
}
SimulateCone <- function(interval, whole.set, whole.set.prices, 
                         index.chosen, factors_chosen, type, 
                         lag, rolling.window, 
                         once.flag = FALSE,
                         updateProgress = NULL) {
  dates <- dplyr::pull(whole.set, dates)
  
  whole.set <- dplyr::select(whole.set, -dates)
  whole.set.prices <- dplyr::select(whole.set.prices, -dates)
  position <- which(colnames(whole.set) %in% index.chosen)
  type <- type[position]
  
  ending.window <- seq(rolling.window*lag + 1, nrow(whole.set.prices), 
                       by = interval) 
  
  final.rebalancing.day <- ending.window[length(ending.window)]
  
  ending.window <- ending.window |> 
    c(nrow(whole.set.prices)) |> 
    unique()
  
  starting.window <- ending.window - rolling.window*lag
  days.forward <- diff(ending.window)
  meter <- 0
  for (time in 1:length(ending.window)) {
    
    start <- starting.window[time]
    end <- ending.window[time]
    # proj.days <- days.forward[time]
    proj.days <- interval
    
    training.set <- dplyr::slice(whole.set, start:(end - 1))
    test.valid.data <- purrr::map_lgl(training.set,
                                      ~(any(is.na(.x)) | all(.x == 0)))
    training.set <- training.set[,!test.valid.data]
    training.set.prices <- dplyr::slice(whole.set.prices, start:end)
    training.set.prices <- training.set.prices[,!test.valid.data]
    
    svd.analysis <- RunSVD(training.set, factors_chosen) 
    svd.analysis.prices <- RunSVD(training.set.prices, factors_chosen) 
    
    svd.training.indices <- svd.analysis |> 
      ProjectReturns(proj.days)
    
    if (!(index.chosen %in% colnames(training.set))) {
      meter <- meter + 1
      next
    }
    
    time.series <- CalculateCone(training.set, training.set.prices,
                                 svd.training.indices, svd.analysis.prices, lag,
                                 index.chosen, type, proj.days)
    eigenvectors.index <- rownames(svd.analysis$L) %in% index.chosen
    eigenvalues.perc.vec <- diag(svd.analysis$S)/sum(diag(svd.analysis$S))
    if ((time - meter) == 1) {
      ret.index <- time.series$ret.index
      graph.index <- time.series$index
      index <- time.series$range
      eigenvectors <- abs(svd.analysis$L[eigenvectors.index,])
      eigenvalues <- t(as.matrix(diag(svd.analysis$S)[1:factors_chosen]))
      eigenvalues.perc <- t(eigenvalues.perc.vec[1:factors_chosen])
    } else {
      ret.index <- dplyr::bind_rows(ret.index, 
                                    tail(time.series$ret.index, interval))
      index <- dplyr::bind_rows(index, time.series$range)
      graph.index <- dplyr::bind_rows(graph.index, 
                                      tail(time.series$index, interval))
      eigenvectors <- dplyr::bind_rows(eigenvectors,
                                       abs(svd.analysis$L[eigenvectors.index,]))
      eigenvalues <- rbind(eigenvalues,
                           t(diag(svd.analysis$S)[1:factors_chosen]))
      eigenvalues.perc <- rbind(eigenvalues.perc,
                                t(eigenvalues.perc.vec[1:factors_chosen]))
    }
    if (once.flag) break
    if (is.function(updateProgress)) {
      text <- glue::glue("SVD calibration Iteration {time} of \\
                         {length(days.forward)}")
      updateProgress(value = time, detail = text)
    }
  }
  
  if (final.rebalancing.day != ending.window[length(ending.window)]) {
    final.day <- ending.window[length(ending.window)]
    
    incomplete.interval <- tibble::tibble(
      index = whole.set.prices[[index.chosen]][(final.rebalancing.day + 1):final.day],
      pcx_replica = NA_real_)
    
    graph.index <- graph.index |> 
      dplyr::bind_rows(incomplete.interval)
    
    incomplete.interval <- tibble::tibble(
      index = whole.set[[index.chosen]][(final.rebalancing.day):(final.day - 1)],
      pcx_replica = NA_real_)
    
    ret.index <- ret.index |> 
      dplyr::bind_rows(incomplete.interval)
  }
  N <- length(ending.window)
  eingendata <- list()
  
  index <- index |> 
    dplyr::mutate(Dates = dates[ending.window[(meter + 1):N]-1]) |> 
    dplyr::select(Dates, dplyr::everything())
  
  ret.index <- ret.index |> 
    dplyr::mutate(Dates = dates[ending.window[(meter + 1):N]-1]) |> 
    dplyr::select(Dates, dplyr::everything())
  
  eingendata$eigenvectors <- data.frame(
    Dates = dates[ending.window[(meter + 1):N]-1]) |> 
    dplyr::bind_cols(as.data.frame(eigenvectors))
  eingendata$eigenvalues <- data.frame(
    Dates = dates[ending.window[(meter + 1):N]-1]) |> 
    dplyr::bind_cols(as.data.frame(eigenvalues)) |> 
    dplyr::rename_all(~c("Dates", glue::glue("RC{1:factors_chosen}")))
  eingendata$eigenvalues.perc <- data.frame(
    Dates = dates[ending.window[(meter + 1):N]-1]) |> 
    dplyr::bind_cols(as.data.frame(eigenvalues.perc))|> 
    dplyr::rename_all(~c("Dates", glue::glue("RC{1:factors_chosen}")))
  
  return(list(prices = index, returns = ret.index, graph.index = graph.index,
              type = type, eingendata = eingendata, meter = meter))
}

ConeInterval <- function(proj.prices, rebalancing.frequency, index) {
  
  proj.prices |> 
    purrr::map_dfc(~dplyr::select(.x, dplyr::contains(index))) |> 
    dplyr::rename_all(~c("down", "Median", "up")) |> 
    dplyr::select(-Median) |> 
    tail(rebalancing.frequency)
}

SimulateProjection <- function(interval, training.set, testing.set, starting.value, 
                               index.chosen, factors_chosen, type) {
  
  position <- which(colnames(training.set) %in% index.chosen)
  type <- type[position]
  
  rebalancing.dates <- c(0, seq(interval, nrow(testing.set), 
                                by = interval)) |> 
    c(nrow(testing.set)) |> 
    unique()
  
  for (time in seq_along(diff(rebalancing.dates))) {
    
    current <- rebalancing.dates[time]
    next.one <- rebalancing.dates[time + 1]
    interval <- next.one - current
    
    svd.training.indices <- IncludeTestingSet(testing.set, training.set, 
                                              current) |> 
      RunSVD(factors_chosen) |> 
      ProjectReturns(interval)
    
    training.indices <- IncludeTestingSet(testing.set, training.set, 
                                          next.one)
    
    time.series <- CalculateTimeSeries(training.indices, svd.training.indices, 
                                       starting.value, index.chosen, type) 
    
    distance.matrix <- CalculateDistance(time.series$prices, type)
    
    if (time == 1) {
      ret.index <- time.series$returns
      index <- tail(distance.matrix, interval)
      graph.index <- time.series$prices
    } else {
      ret.index <- dplyr::bind_rows(ret.index, 
                                    tail(time.series$returns, interval))
      index <- dplyr::bind_rows(index, tail(distance.matrix, interval))
      
      graph.index <- dplyr::bind_rows(graph.index, 
                                      distance.matrix |> 
                                        dplyr::select(-asset.1_std, -asset.2_std,
                                                      -distance) |> 
                                        tail(interval))
    }
  }
  
  median.distance <- index |> 
    dplyr::pull(distance) |> 
    median() |> 
    round(3)
  
  max.distance <- index |> 
    dplyr::pull(distance) |> 
    max()
  
  list(prices = index, returns = ret.index, median.distance = median.distance,
       max.distance = max.distance, graph.index = tail(graph.index, -1))
}

CalculateDistance <- function(data, type) {
  data <- data |> 
    dplyr::mutate(dplyr::across(dplyr::starts_with("asset"), 
                                ~base::scale(.x),
                                .names = "{col}_std"))
  
  if (!type) {
    data <- data |> 
      dplyr::mutate(distance = abs(asset.1 - asset.2))
  } else {
    data <- data |>
      dplyr::mutate(distance = abs(asset.1 - asset.2)/asset.1)
  }
  
  
  return(data)
  
}

AnalysisForRecommendation <- function(inputs, index, adjustment_flag) {
  
  scale.value <- 252
  name <- "Days with trade on"
  
  if (adjustment_flag) {
    stats.input <- list(type = inputs$type,
                        trades = inputs$trades_weighted)
  } else {
    stats.input <- list(type = inputs$type,
                        trades = inputs$trades)
  }
  
  stats <- CalculateStatisticsRT(stats.input)
  
  if(!adjustment_flag) {
    vola <- StdDev.annualized(inputs$performance,
                              scale = scale.value)[2]
    non.zero.returns <- inputs$performance[,2] 
    
    cumulative.scores <- inputs$performance[,2] |> 
      cumsum() |> 
      {\(x) x/vola}()
    
  } else {
    vola <- StdDev.annualized(inputs$performance_weighted,
                              scale = scale.value)[2]
    non.zero.returns <- inputs$performance_weighted[,2] 
    cumulative.scores <- inputs$performance_weighted[,2] |> 
      cumsum() |> 
      {\(x) x/vola}()
  }
  
  non.zero.returns[non.zero.returns == 0] <- NA
  perc.trading <- 1 - length(non.zero.returns[is.na(non.zero.returns)])/length(non.zero.returns)
  non.zero.returns <- na.omit(non.zero.returns)
  
  if(!adjustment_flag) {
    performance <- tail(inputs$performance, -1)
    
    summary <- tibble::tibble(
      Index = index,
      `Index Level` = tail(inputs$range$index, 1),
      Status = tail(inputs$trades$trade, 1),
      `Lower Boundary` = tail(inputs$range$down, 1),
      `Upper Boundary` = tail(inputs$range$up, 1)
    )
  } else {
    
    performance <- tail(inputs$performance_weighted, -1)
    summary <- tibble::tibble(
      Index = index,
      `Index Level` = tail(inputs$range_weighted$index, 1),
      Status = tail(inputs$trades_weighted$trade, 1),
      `Lower Boundary` = tail(inputs$range_weighted$down, 1),
      `Upper Boundary` = tail(inputs$range_weighted$up, 1)
    )
  }
  
  summary <- summary |> 
    dplyr::mutate(
      `Distance from Boundary` = ifelse(`Index Level` > `Upper Boundary`,
                                        `Index Level` - `Upper Boundary`,
                                        ifelse(`Index Level` < `Lower Boundary`,
                                               `Lower Boundary` - `Index Level`,
                                               0)),
      `Number of Trades` = nrow(stats),
      `Cumulative Return` = Return.cumulative(performance,
                                              geometric = inputs$type)[2],
      `Annualised Return` = Return.annualized(performance,
                                              geometric = inputs$type)[2],
      `Strategy Annualised Volatility` = vola,
      `Long Only Volatility` = StdDev.annualized(performance,
                                                 scale = scale.value)[1],
      `Sharpe stratetegy` = SharpeRatio.annualized(performance,
                                                   scale = scale.value)[2],
      `Sharpe long only` = SharpeRatio.annualized(performance,
                                                  scale = scale.value)[1],
      `Max Drawdown Strategy` = maxDrawdown(performance)[2],
      `Max Drawdown Long Only` = maxDrawdown(performance)[1],
      !!name := perc.trading,
      `Bets Won (in %)` = sum(stats$bet.result == "WON")/nrow(stats),
      `Kelly Criteria` = KellyCriteria(stats)
    )
  
  non.zero.returns <- as.numeric(base::scale(non.zero.returns))
  
  return(list(summary = summary, scaled.return = non.zero.returns,
              cumulative.scores = cumulative.scores))
  
}

fix_bets <- function(trades.pos) {
  N.in <- length(trades.pos$pos.in)
  N.out <- length(trades.pos$pos.out)
  if (N.in == N.out) {
    check <- trades.pos$pos.in < trades.pos$pos.out
    if(!all(check)) {
      trades.pos$pos.out <- trades.pos$pos.out[2:N.out]
      trades.pos$pos.in <- trades.pos$pos.in[1:(N.in - 1)]
    }
  } else {
    N <- max(c(N.in, N.out)) - min(c(N.in, N.out))
    for (i in 1:N) {
      check <- FALSE
      max_row <- min(c(N.in, N.out))
      test <- trades.pos$pos.in[1:max_row] < trades.pos$pos.out[1:max_row]
      if (any(!test)) {
        j <- which.min(!test)
        if (j == 1)
        {
          trades.pos$pos.out <- trades.pos$pos.out[2:N.out]
        } else {
          trades.pos$pos.out <- c(trades.pos$pos.out[1:(j-1)], trades.pos$pos.out[(j + 1):N.out])
        }
        N.out <- N.out - 1
        check <- TRUE
        next
      }
      if (N.in > max_row) {
        trades.pos$pos.in <- trades.pos$pos.in[1:max_row]
      } 
    }
  }
  return(trades.pos)
}



CalculateStatistics <- function(direction, type, trades) {
  
  in.dir  <- glue::glue("{direction}.in")
  out.dir <- glue::glue("{direction}.out")
  
  trades.pos <- list(pos.in = which(!is.na(trades[[in.dir]])),
                     pos.out = which(!is.na(trades[[out.dir]])))
  
  if (length(trades.pos$pos.in) == 0 || length(trades.pos$pos.out) == 0) {
    trade.summary <- NULL
  } else {
    trades.pos <- fix_bets(trades.pos)
    
    trade.summary <- data.frame(trade.in = trades[[in.dir]][trades.pos$pos.in],
                                trade.out = trades[[out.dir]][trades.pos$pos.out],
                                duration = trades.pos$pos.out - trades.pos$pos.in) |> 
      dplyr::mutate(
        return = dplyr::case_when(
          type == FALSE ~ (trade.out - trade.in)/100,
          TRUE ~ log(trade.out/trade.in)
        ),
        return = dplyr::case_when(
          grepl("Short", direction) ~ -return,
          TRUE ~ return
        ),
        bet.result = dplyr::if_else(return > 0, "WON", "LOST")) 
  }
  return(trade.summary)
  
}

CalculateLastDataAtomic <- function(range, range_graph, index, start, 
                                    end) {
  
  range_graph <- range_graph[start:end,]
  range <- range[start:end,]
  
  scale.index <- attr(base::scale(range_graph$index,
                                  center = FALSE),"scaled:scale")
  date <- tail(range$dates,1)
  last_value_index <- tail(range$index,1)
  last_value_pcx <- tail(range$pcx_replica,1)
  
  delta <- (last_value_index - last_value_pcx)/scale.index
  
  confidence_range <- (tail(range_graph,1)$up/scale.index)/0.75
  
  confidence_range <- min(c(confidence_range, 0.98))
  confidence_range <- max(c(confidence_range, 0))
  
  confidence_range_message <- switch(ceiling((confidence_range+0.01)*5),
                                     "Low confidence", "Medium-Low confidence",
                                     "Medium confidence","Medium-High confidence", 
                                     "High confidence")
  
  information_index <- abs((range$index - range$pcx_replica)/(range$down - range$pcx_replica)) |> 
    tail(1)
  
  return(tibble::tibble(Dates = date,
                        Index = index,
                        `Current Index Value` = last_value_index, 
                        `Distance from Estimated Fair Value (z-score)` = delta,
                        `Fair Value Estimation Confidence` = confidence_range_message,
                        `Information Index` = information_index))
}

CalculateZScoreDistance <- function(index, k, LAG, start_date, end_date, dir_output) {
  message(index)
  
  range <- AtomicParquetExtraction("range_range", "dates", k, LAG, index, 
                                   start_date, end_date, dir_output)
  
  range_graph <- AtomicParquetExtraction("range.graph_range", "dates", k, LAG, 
                                         index, start_date, end_date, 
                                         dir_output)
  
  if(nrow(range) > 252) {
    start <- seq(from = 1, to = nrow(range) - 252, by = 1)
    end <- seq(from = 253, to = nrow(range), by = 1)
  } else {
    start <- 1
    end <- nrow(range)
  }
  
  distance <- purrr::map2_dfr(start, end, 
                              ~CalculateLastDataAtomic(range, range_graph, index,
                                                       .x, .y))
  
  return(distance)
}

CalculateLastData <- function(index, K, LAG, start_date, end_date, dir) {
  
  range <- AtomicParquetExtraction("range_range", "dates", K, LAG, index, 
                                   start_date, end_date, dir)
  
  range_graph <- AtomicParquetExtraction("range.graph_range", "dates", K, LAG, 
                                         index, start_date, end_date, dir)
  
  data <- CalculateLastDataAtomic(range, range_graph, index, start = 1,
                                  end = nrow(range))
  
  return(data)
}

KellyCriteria <- function(statistics) {
  total_bets <- nrow(statistics)
  
  bets_won <- nrow(dplyr::filter(statistics, bet.result %in% "WON"))
  
  p = bets_won/total_bets
  
  average_won = statistics |> 
    dplyr::filter(bet.result %in% "WON") |> 
    dplyr::pull(return) |> 
    log() |> 
    mean() |> 
    exp()
  
  average_lost = statistics |> 
    dplyr::filter(bet.result %in% "LOST") |> 
    dplyr::mutate(return = return * -1) |> 
    dplyr::pull(return) |> 
    log() |> 
    mean() |> 
    exp()
  
  b <- average_won/average_lost
  
  k = (p*b-(1-p))/b
  
  return(round(k, 3))
}

# Plots -------------------------------------------------------------------


graph_index_and_eigendata <- function(K, LAG, index, start_date, end_date, dir,
                                      dir_information = NULL) {
  
  eigenvalues <- AtomicParquetExtraction("eigendata_eigenvalues.perc", "Dates", K, 
                                         LAG, index, start_date, end_date, dir) |> 
    tidyr::pivot_longer(-Dates, names_to = "RCx", values_to = "Value") 
  
  eigenvectors <- AtomicParquetExtraction("eigendata_eigenvectors", "Dates", K, 
                                          LAG, index, start_date, end_date, dir) |> 
    tidyr::pivot_longer(-Dates, names_to = "RCx", values_to = "Value")
  
  range <- AtomicParquetExtraction("range_range", "Dates", K, 
                                   LAG, index, start_date, end_date, dir) |> 
    dplyr::select(Dates, Index = index, "Estimated Fair Value" = pcx_replica) |> 
    tidyr::pivot_longer(-Dates, names_to = "Type", values_to = "Value")
  
  if (!is.null(dir_information)) {
    
    information_graph <- dir_information |> 
      arrow::open_dataset() |> 
      dplyr::filter(Index %in% index) |> 
      dplyr::filter(Dates > start_date) |> 
      dplyr::filter(Dates <= end_date) |> 
      dplyr::arrange(Dates) |> 
      dplyr::select(Dates, `Information Index`, `Mean Information Index`) |> 
      dplyr::collect() 
  }
  
  colors <- c("#f1c40f", "#2ecc71", "#9b59b6", "#e74c3c")
  
  hc <- highchart(type = "stock") |>
    hc_add_theme(my_hc_theme)
  
  if (!is.null(dir_information)) {
    hc <- hc |> 
      hc_yAxis_multiples(create_axis(naxis = 4, showLastLabel = TRUE, turnopposite = TRUE,
                                     heights = c(1, 1, 1, 1), sep = 0.05,
                                     title = c(list(list(text = "Index and Estimated Fair Values (EFV)")),
                                               list(list(text = "Information Ratio")),
                                               list(list(text = "Eigenvalues Contribution")),
                                               list(list(text = "Eigenvectors")))))
  } else {
    hc <- hc |> 
      hc_yAxis_multiples(create_axis(naxis = 3, showLastLabel = TRUE, turnopposite = TRUE,
                                     heights = c(1, 1, 1), sep = 0.05,
                                     title = c(list(list(text = "Index and Estimated Fair Values (EFV)")),
                                               list(list(text = "Eigenvalues Contribution")),
                                               list(list(text = "Eigenvectors")))))
  }
  hc <- hc |>
    hc_add_series(data = range,  yAxis = 0, hcaes(x = Dates, y = Value, group = Type),
                  type = "line", color = c("yellow", "red"))
  
  M <- 0
  if (!is.null(dir_information)) {
    
    hc <- hc |>
      hc_add_series(data = information_graph,  yAxis = 1, 
                    hcaes(x = Dates, y = `Information Index`),
                    type = "line", color = c("red"), name = "Information Ratio") |> 
      hc_add_series(data = information_graph,  yAxis = 1, 
                    hcaes(x = Dates, y = `Mean Information Index`),
                    type = "line", color = c("yellow"), name = "1Y Rolling Information Ratio") |> 
      hc_add_series(data = information_graph,  yAxis = 1, 
                    hcaes(x = Dates, y = 1),
                    type = "line", color = c("white"), name = "Information Ratio = 1")
    M <- 1
  } 
  
  hc <- hc |> 
    hc_add_series(data = eigenvalues,  yAxis = M + 1, hcaes(x = Dates, y = Value, group = RCx),
                  type = "line", color = colors[1:K]) |>
    hc_add_series(data = eigenvectors,  yAxis = M + 2, hcaes(x = Dates, y = Value, group = RCx),
                  type = "line", color = colors[1:K], showInLegend = FALSE) |> 
    hc_tooltip(valueDecimals = 3) |>
    hc_scrollbar(enabled = FALSE) |>
    hc_navigator(enabled = FALSE) |>
    hc_exporting(enabled = TRUE) |>
    hc_legend(enabled = TRUE) |>
    hc_rangeSelector(enabled = TRUE,
                     labelStyle = list(color = "white"),
                     inputStyle = list(color = "grey")) |>
    hc_add_theme(my_hc_theme) |> 
    hc_caption(text = "Source: HedgeAnalytics")
  
  return(hc)
}
TradeDistancePlot <- function(trades.data, type.flag, delta_t,
                              include_trades_flag, selected_index = NULL,
                              adjust_corridor_flag = FALSE) {
  if(type.flag) {
    if (adjust_corridor_flag) {
      corridor_name <- "Normalised Fair Value Estimation Corridor IR Adjusted"
      other_corridor_name <- "Normalised Fair Value Estimation Corridor"
    } else {
      corridor_name <- "Normalised Fair Value Estimation Corridor"
      other_corridor_name <- "Normalised Fair Value Estimation Corridor IR Adjusted"
    }
    
    if (!is.null(selected_index)) {
      line_name <- glue::glue("Normalised Distance from the FVE ({selected_index})")
    } else {
      line_name <- glue::glue("Normalised Distance from the FVE")
    }
    
    y_title = "Z-score"
    range <- trades.data$range.graph$range
    trades <- trades.data$trades.graph
    trades.daily <- trades.data$range.graph$range.daily 
    other_range <- trades.data$other.range.graph$range |> 
      dplyr::select(t, down_other = down, up_other = up)
  } else {
    if (adjust_corridor_flag) {
      corridor_name <- "Fair Value Estimation Corridor IR Adjusted"
      other_corridor_name <- "Fair Value Estimation Corridor"
    } else {
      corridor_name <- "Fair Value Estimation Corridor"
      other_corridor_name <- "Fair Value Estimation Corridor IR Adjusted"
    }
    if (!is.null(selected_index)) {
      line_name <- glue::glue("{selected_index}")
    } else {
      line_name <- "Selected Index"
    }
    y_title = "Level"
    range <- trades.data$range$range
    trades <- trades.data$trades
    trades.daily <- trades.data$range$range.daily 
    other_range <- trades.data$other.range$range |> 
      dplyr::select(t, down_other = down, up_other = up)
  }
  
  trade.graph <- trades |> 
    dplyr::select(time, Long.in, Short.in, Long.out, Short.out) |> 
    tidyr::pivot_longer(-time,
                        names_to = "Trade",
                        values_to = "Level") |> 
    dplyr::mutate(trade.side = dplyr::if_else(stringr::str_detect(Trade, "in"),
                                              "In", "Out"),
                  position = dplyr::if_else(stringr::str_detect(Trade, "Long"),
                                            "Long", "Short")) |> 
    na.omit() 
  
  if (is.null(trades.daily)) {
    data <- range |> 
      dplyr::rename("dates" = t, "index.strategy" = index)
  } else {
    data <- trades.daily |> 
      dplyr::left_join(range, by = c("dates" = "t")) |> 
      dplyr::left_join(other_range, by = c("dates" = "t")) |> 
      dplyr::select(dates, index.strategy, dplyr::contains("up"), dplyr::contains("down")) |> 
      tidyr::fill(up, down)
  }
  
  data <- data |> 
    dplyr::left_join(trade.graph, by = c("dates" = "time")) 
  
  if (type.flag) {
    
    scale.scale <- attr(base::scale(trades.daily$index.strategy,
                                    center = FALSE),"scaled:scale")
    
    data <- data |> 
      dplyr::mutate_if(is.numeric, ~.x/scale.scale)
    
    trade.graph$Level <- trade.graph$Level/scale.scale
    title.graph <- "Scaled Gap to Index (z-score)"
  } else {
    title.graph <- "Index and Estimated Fair Value Corridor"
  }
  
  data <- data[!is.na(data$down),]
  
  dtick <- ifelse(delta_t > 5*365, "M12", "M3")
  tickFormat <- ifelse(delta_t > 5*365, "%Y", "%b-%y")
  
  hc <- highchart(type = "stock") |> 
    hc_add_series(data = data, hcaes(x = dates, low = down_other, high = up_other), 
                  type = "arearange", color = "#E0E3E4", name = other_corridor_name) |> 
    hc_add_series(data = data, hcaes(x = dates, low = down, high = up), 
                  type = "arearange", color = "#818c95", name = corridor_name) |> 
    hc_add_series(data = data, hcaes(x = dates, y = index.strategy), 
                  color = "#f1c40f", type = "line", name = line_name) 
  
  if (include_trades_flag) {
    short_col <- "#9b59b6"
    long_col <- "#2ecc71"
    
    hc <- hc |> 
      hc_add_series(data = dplyr::filter(data, Trade %in% "Long.in"),
                    hcaes(x = dates, y = Level),
                    name = "Long In Trades",
                    color = long_col,
                    type = "scatter",
                    marker = list(symbol = "triangle",
                                  radius = 6.5,
                                  linecolor = "#ffffff"),
                    tooltip = list(pointFormat = "Date: {point.x: %Y-%m-%d} <br> Entry Level: {point.y}")) |> 
      hc_add_series(data = dplyr::filter(data, Trade %in% "Long.out"),
                    hcaes(x = dates, y = Level),
                    name = "Long Out Trades",
                    color = long_col,
                    type = "scatter",
                    marker = list(symbol = "triangle-down",
                                  radius = 6.5,
                                  linecolor = "#ffffff"),
                    tooltip = list(pointFormat = "Date: {point.x: %Y-%m-%d} <br> Exit Level: {point.y}")) |> 
      hc_add_series(data = dplyr::filter(data, Trade %in% "Short.in"),
                    hcaes(x = dates, y = Level),
                    name = "Short In Trades",
                    color = short_col,
                    type = "scatter",
                    marker = list(symbol = "triangle",
                                  radius = 6.5,
                                  linecolor = "#ffffff"),
                    tooltip = list(pointFormat = "Date: {point.x: %Y-%m-%d} <br> Entry Level: {point.y}")) |> 
      hc_add_series(data = dplyr::filter(data, Trade %in% "Short.out"),
                    hcaes(x = dates, y = Level),
                    name = "Short Out Trades",
                    color = short_col,
                    type = "scatter",
                    marker = list(symbol = "triangle-down",
                                  radius = 6.5,
                                  linecolor = "#ffffff"),
                    tooltip = list(pointFormat = "Date: {point.x: %Y-%m-%d} <br> Exit Level: {point.y}")) 
  }
  
  hc <- hc |>
    hc_yAxis(opposite = FALSE,
             title = list(text = y_title)) |> 
    hc_xAxis(title = list(text = "Date")) |> 
    hc_tooltip(valueDecimals = 3) |>
    hc_scrollbar(enabled = FALSE) |>
    hc_navigator(enabled = FALSE) |>
    hc_exporting(enabled = TRUE) |>
    hc_legend(enabled = TRUE) |>
    hc_rangeSelector(enabled = TRUE,
                     labelStyle = list(color = "white"),
                     inputStyle = list(color = "grey")) |>
    hc_add_theme(my_hc_theme) |> 
    hc_caption(text = "Source: HedgeAnalytics")
  
  return(hc)
}

PlotDistribution <- function(stats) {
  data = data.frame(return = density(stats$return)$x*100, 
                    frequency = density(stats$return)$y)
  p <- highchart() |> 
    hc_add_series(data = data,
                  mapping = hcaes(x = return, y = frequency),
                  type = "area",
                  tooltip = list(pointFormat = "Return: {point.x:.1f}% <br> Frequency: {point.y:.1f}%",
                                 headerFormat = NULL)) |>
    hc_xAxis(title = list(text = "Daily Strategy Returns distribution"),
             labels = list(format = "{value}%")) |> 
    hc_yAxis(title = list(text = "Frequency"),
             labels = list(format = "{value}%")) |> 
    hc_add_theme(my_hc_theme) |> 
    hc_legend(enabled = FALSE) |> 
    hc_tooltip(crosshairs = FALSE) |> 
    hc_caption(text = "Source: HedgeAnalytics")
  
  return(p)
}

PlotBettingResults <- function(stats) {
  
  if ("return_strategy" %in% colnames(stats)) {
    bet <- stats |> 
      dplyr::group_by(bet.result) |> 
      dplyr::summarise(n = dplyr::n(),
                       median_return = median(return_strategy))
  } else {
    bet <- stats |> 
      dplyr::count(bet.result)
  }
  
  bet <- bet |> 
    dplyr::mutate(n.perc = round(n/sum(n)*100, 2)) 
  
  if (nrow(bet) == 1) {
    if (bet$bet.result %in% "LOST") {
      bet <- bet |> 
        dplyr::bind_rows(
          tibble::tibble(
            bet.result = "WON",
            n = 0L,
            n.perc = 0,
            color = "#2ecc71"
          )
        )
    } else {
      bet <- bet |> 
        dplyr::bind_rows(
          tibble::tibble(
            bet.result = "LOST",
            n = 0L,
            n.perc = 0,
            color = "#e74c3c"
          )
        )
    }
  }
  
  bet <- bet |> 
    dplyr::arrange(bet.result) |> 
    dplyr::select(-n)
  
  if (nrow(bet) == 2) {
    bet <- bet |> 
      dplyr::mutate(color = c("#e74c3c", "#2ecc71"))
    
  } else {
    bet <- bet |> 
      dplyr::mutate(color = c("#e74c3c", "#f1c40f", "#2ecc71"))
  }
  
  p <- hchart(bet, type = "column", hcaes(x = bet.result, y = n.perc, color = color),
              colorByPoint = TRUE,
              tooltip = list(pointFormat = "{point.y:0f}%",
                             headerFormat = NULL)) |>
    hc_xAxis(title = list(text = "Trade result")) |> 
    hc_yAxis(title = list(text = "Percentage of bets won/lost"),
             labels = list(format = "{value}%")) |> 
    hc_add_theme(my_hc_theme) |> 
    hc_legend(enabled = TRUE) |> 
    hc_tooltip(valueDecimals = 3) |> 
    hc_caption(text = "Source: HedgeAnalytics")
  
  return(p)
}

PlotDuration <- function(stats) {
  min <- min(stats$duration)
  max <- max(stats$duration)
  
  p <- stats |> 
    dplyr::count(duration) |> 
    dplyr::mutate(n.perc = round(n/sum(n)*100, 2),
                  cum.n.perc = cumsum(n.perc)) |> 
    hchart(type = "line", hcaes(x = duration, y = cum.n.perc),
           name = "Cumulative frequency of trades' duration",
           tooltip = list(pointFormat = "Days: {point.x} <br> Cumulative Frequency: {point.y:.1f}%",
                          headerFormat = NULL)) |>
    hc_yAxis(title = list(text = "Cumulative frequency of trades' duration"),
             labels = list(format = "{value}%")) |> 
    hc_xAxis(title = list(text = "Number of days the trade is on"),
             labels = list(format = "{value} days")) |> 
    hc_add_theme(my_hc_theme) |> 
    hc_legend(enabled = FALSE) |> 
    hc_tooltip(valueDecimals = 3) |> 
    hc_caption(text = "Source: HedgeAnalytics")
  
  return(p)
}