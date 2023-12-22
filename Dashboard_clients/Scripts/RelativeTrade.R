
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
  
  actual <- projection$graph.index |> 
    tail(nrow(projection$prices))
  
  actual <- actual |> 
    dplyr::mutate(t = dates)
  
  if(normalise) {
    projection$prices$mean <- rowMeans(projection$prices[,2:3]) 
    
    range <- projection$prices |> 
      dplyr::mutate(up = up - mean,
                    down = down - mean) |> 
      head(nrow(actual)) |> 
      dplyr::bind_cols(actual) |> 
      dplyr::mutate(index = index - mean) 
  } else {
    range <- projection$prices |> 
      dplyr::bind_cols(actual) 
  }
  
  if (!is.null(daily.prices)) {
    if (normalise) {
      projection$prices <- projection$prices |> 
        dplyr::select(-Dates)
      
      range.daily <- daily.prices |> 
        dplyr::left_join(range, by = c("dates" = "t")) |> 
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

RelativeTradeAlgorithmCone <- function(range, delay = 0, updateProgress = NULL,
                                       resetProgress = FALSE, daily.prices = NULL) {
  
  over.count <- 0
  under.count <- 0
  in.count <- 0
  flag.in <- FALSE
  flag.long <- FALSE
  flag.short <- FALSE
  
  trades <- tibble::tibble(time = range$t, 
                           out.count = NA_integer_,
                           Long.in = NA_real_,
                           Short.in = NA_real_,
                           Long.out = NA_real_,
                           Short.out = NA_real_,
                           w1 = 0,
                           w2 = 1,
                           trade = NA_character_)
  
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
  
  for (i in 1:(nrow(trades))) {
    if (is.function(updateProgress)) {
      text <- glue::glue("Calculating trades at time {i} of \\
                         {nrow(trades)}")
      updateProgress(value = i, detail = text, reset = resetProgress)
      resetProgress <- FALSE
    }
    # Checks
    check.up <- range$index[i] > range$up[i]
    check.down <- range$index[i] < range$down[i]
    check.range <- !(check.up | check.down)
    # Counts
    under.count <- ifelse(check.down, under.count + 1, 0)
    over.count <- ifelse(check.up, over.count + 1, 0)
    trades$out.count[i] <- under.count + over.count
    in.count <- ifelse(check.range, in.count + 1, 0)
    switch.count <- ifelse(i < (delay + 1), NA_integer_, trades$out.count[i-(delay+1)])
    # Flags
    flag.switch <- ifelse(trades$out.count[i] == (delay + 1) & 
                            !is.na(switch.count) &
                            switch.count > 0,
                          TRUE, FALSE)
    flag.enter <- ifelse((!flag.in | flag.switch) & (trades$out.count[i] == (delay + 1)),
                         TRUE, 
                         FALSE)
    flag.out <- ifelse(flag.in & (flag.switch | in.count == (delay + 1)) & 
                         !flag.switch,
                       TRUE, FALSE)
    flag.in <- ifelse((!flag.in & flag.enter) | (flag.in & !flag.out),
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
      trades$w1[i] == 1  ~ "Below the corridor",
      trades$w1[i] == -1 ~ "Above the corridor",
      TRUE ~ "Within the corridor"
    )
  }
  
  trades <- trades |> 
    dplyr::select(-out.count)
  
  return(trades)
}

CalculateStatisticsRT <- function(relative.trade) {
  table_long <- CalculateStatistics("Long", relative.trade$type, 
                                    relative.trade$trades)
  table_short <- CalculateStatistics("Short", relative.trade$type, 
                                     relative.trade$trades)
  return(dplyr::bind_rows(table_long, table_short))
}

RelativeTradingCone <- function(projection, 
                                dates, lag.window = 0, type = TRUE, index.flag = TRUE,
                                backtest.return = TRUE, daily.returns = NULL,
                                daily.prices = NULL, updateProgress = NULL) {
  
  range <- PrepareRange(projection, dates, normalise = FALSE,
                        daily.prices = daily.prices)
  
  trades <- RelativeTradeAlgorithmCone(range$range, delay = lag.window,
                                       updateProgress = updateProgress,
                                       resetProgress = FALSE, 
                                       daily.prices = range$range.daily)

  range.graph <- PrepareRange(projection, dates, normalise = TRUE,
                              daily.prices = daily.prices)
  
  trades.graph <- RelativeTradeAlgorithmCone(range.graph$range, delay = lag.window,
                                             updateProgress = updateProgress,
                                             resetProgress = FALSE, 
                                             daily.prices = range.graph$range.daily) 
  
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
    
    r.strategy <- Return.portfolio(returns.xts, weights = weights,
                                   rebalance_on = "days", geometric = type)
    
    r.long.only <- Return.portfolio(long.only, weights = weights.long.only,
                                    rebalance_on = "days", geometric = type)
    
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
  
  r.strategy <- Return.portfolio(returns.xts, weights = weights)
  
  if (!index.flag) {
    r.equal <- Return.portfolio(returns.xts, weights = c(rep(0.5,2),0),
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
    purrr::map2_dfc(type,  ~CalculateInvariant(.x, .y, lag))
  
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
  
  return(list(ret.index = ret.index, index = index, range = range))
  
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
                                    ~(any(is.na(.x)) | all(.x == 0)))
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
  
  training.set <- foreach::foreach(x = starting.window,
                                   y = ending.window) %dopar% {
                                     SimulateConeDynamicColumns(whole.set, x, y, FALSE)
                                   }
  
  training.set.prices <- foreach::foreach(x = starting.window,
                                          y = ending.window) %dopar% {
                                            SimulateConeDynamicColumns(whole.set.prices, x, y, TRUE)
                                          }
  
  days.forward <- rep(1, length(training.set))
  tictoc::tic()
  
  
  # for(i in 5747:length(training.set)) {
  # # for(i in 1:5727) {
  #   message(i)
  #   time.series <- SimulateConeSingle(training.set[[i]], training.set.prices[[i]], 
  #                                    days.forward[[i]], factors_chosen, lag,
  #                                    index.chosen, type)
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

AnalysisForRecommendation <- function(inputs, index) {
  # AnalysisForRecommendation <- function(index, returns, prices, rebalance, K, 
  #                                       type, lag, delay, dates, window, 
  #                                       daily.flag = "Monthly", returns.daily = NULL,
  #                                       prices.daily = NULL) {
  
  # projection <- SimulateCone(rebalance, 
  #                            returns,
  #                            prices,
  #                            index,
  #                            K, type, lag, window) 
  # 
  # calculation.dates <- dates |> 
  #   tail(-window*lag) #|> 
  # # head(-1)
  # 
  # if (grepl("Daily", daily.flag) | (lag > 1)) {
  #   
  #   
  #   
  #   returns <- returns.daily |> 
  #     dplyr::select(dates, index.strategy = index)
  #   
  #   prices <- prices.daily |> 
  #     dplyr::select(dates, index.strategy = index)
  #   
  #   results <- RelativeTradingCone(projection, calculation.dates,
  #                                  daily.returns = returns, daily.prices = prices)
  #   name <- "Days with trade on"
  # } else {
  #   scale.value <- 12
  #   results <- RelativeTradingCone(projection, calculation.dates)
  #   name <- "Months with trade on"
  # }
  scale.value <- 252
  name <- "Days with trade on"
  stats <- CalculateStatisticsRT(inputs)
  vola <- StdDev.annualized(inputs$performance,
                            scale = scale.value)[2]
  non.zero.returns <- inputs$performance[,2] 
  non.zero.returns[non.zero.returns == 0] <- NA
  perc.trading <- 1 - length(non.zero.returns[is.na(non.zero.returns)])/length(non.zero.returns)
  non.zero.returns <- na.omit(non.zero.returns)
  
  cumulative.scores <- inputs$performance[,2] |> 
    cumsum() |> 
    {\(x) x/vola}()
  
  summary <- tibble::tibble(
    Index = index,
    `Index Level` = tail(inputs$range$index, 1),
    Status = tail(inputs$trades$trade, 1),
    `Lower Boundary` = tail(inputs$range$down, 1),
    `Upper Boundary` = tail(inputs$range$up, 1),
    `Distance from Boundary` = ifelse(`Index Level` > `Upper Boundary`,
                                      `Index Level` - `Upper Boundary`,
                                      ifelse(`Index Level` < `Lower Boundary`,
                                             `Lower Boundary` - `Index Level`,
                                             0)),
    `Number of Trades` = nrow(stats),
    `Cumulative Return` = Return.cumulative(inputs$performance,
                                            geometric = FALSE)[2],
    `Annualised Return` = Return.annualized(inputs$performance,
                                            geometric = FALSE)[2],
    `Strategy Annualised Volatility` = vola,
    `Long Only Volatility` = StdDev.annualized(inputs$performance,
                                               scale = scale.value)[1],
    `Sharpe stratetegy` = SharpeRatio.annualized(inputs$performance,
                                                 scale = scale.value)[2],
    `Sharpe long only` = SharpeRatio.annualized(inputs$performance,
                                                scale = scale.value)[1],
    `Max Drawdown Strategy` = maxDrawdown(inputs$performance)[2],
    `Max Drawdown Long Only` = maxDrawdown(inputs$performance)[1],
    !!name := perc.trading,
    `Bets Won (in %)` = sum(stats$bet.result == "WON")/nrow(stats)
  )
  
  non.zero.returns <- as.numeric(base::scale(non.zero.returns))
  
  return(list(summary = summary, scaled.return = non.zero.returns,
              cumulative.scores = cumulative.scores))
  
}

CalculateStatistics <- function(direction, type, trades) {
  
  in.dir  <- glue::glue("{direction}.in")
  out.dir <- glue::glue("{direction}.out")
  
  trades.pos <- list(pos.in = which(!is.na(trades[[in.dir]])),
                     pos.out = which(!is.na(trades[[out.dir]])))
  
  if (length(trades.pos$pos.in) == 0 || length(trades.pos$pos.out) == 0) {
    trade.summary <- NULL
  } else {
    N.in <- length(trades.pos$pos.in)
    N.out <- length(trades.pos$pos.out)
    if (N.in == N.out) {
      check <- trades.pos$pos.in < trades.pos$pos.out
      if(!all(check)) {
        trades.pos$pos.out <- trades.pos$pos.out[2:N.out]
        trades.pos$pos.in <- trades.pos$pos.in[1:(N.in - 1)]
      }
    } else {
      if (N.in > N.out){
        trades.pos$pos.in <- trades.pos$pos.in[1:(N.in - 1)]
      } else {
        if (N.out > N.in){
          trades.pos$pos.out <- trades.pos$pos.out[2:N.out]
        } else {
          stop("Error with the statistics algorithm")
        }
      }
    }
    
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

  return(tibble::tibble(Dates = date,
                        Index = index,
                        `Current Index Value` = last_value_index, 
                        `Distance from Estimated Fair Value (z-score)` = delta,
                        `Fair Value Estimation Confidence` = confidence_range_message))
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

# Plots -------------------------------------------------------------------


GraphRange <- function(range) {
  
  ggplot(range) +
    geom_line(aes(x = t, y = index)) +
    geom_line(aes(x = t, y = up), color = "blue") +
    geom_line(aes(x = t, y = down), color = "blue") + 
    geom_ribbon(aes(x = t, ymin = down, ymax = up), 
                fill="blue", alpha = 0.5, show.legend = FALSE, inherit.aes = TRUE)+
    tidyquant::theme_tq()
}

TradeInOutPlot <- function(range, trades.daily, delta_t) {
  
  data <- trades.daily |> 
    dplyr::left_join(range, by = c("dates" = "t")) |> 
    dplyr::select(dates, index.strategy, pcx_replica) |> 
    tidyr::fill(pcx_replica)
  
  data <- na.omit(data)
  
  dtick <- ifelse(delta_t > 5*365, "M12", "M3")
  tickFormat <- ifelse(delta_t > 5*365, "%Y", "%b-%y")
  
  p <- plot_ly(data = data, type = 'scatter', mode = "lines") |> 
    add_trace(x = ~dates, y = ~index.strategy, 
              line = list(color = "#000000"), name = "Index") |> 
    add_trace(x = ~dates, y = ~pcx_replica, 
              line = list(color = "red", width = 2.5,dash = "dot"), name = "Estimated Fair Value") |> 
    layout(legend = list(orientation = 'h',
                         xanchor = "center",
                         y = -0.2,
                         x = 0.475),
           xaxis = list(title = "Date",
                        showgrid = TRUE,
                        dtick = dtick, tickformat=tickFormat, hoverformat = "%d %b %Y"),
           yaxis = list(title = "Index and Estimated Fair Values",
                        showgrid = TRUE))
  
  return(p)
}

ChangeTrade <- function(data) {
  if(nrow(data) > 1) {
    data$Trade <- rep(glue::glue("{data$position[1]} {data$trade.side[1]} & \\
                                 {data$position[2]} {data$trade.side[2]}"),2)
    data <- data[1,]
  } else {
    data$Trade <- glue::glue("{data$position[1]} {data$trade.side[1]}")
  }
  return(data)
}

TradeDistancePlot <- function(trades.data, type.flag, delta_t,
                              include_trades_flag) {

  if(type.flag) {
    range <- trades.data$range.graph$range
    trades <- trades.data$trades.graph
    trades.daily <- trades.data$range.graph$range.daily 
  } else {
    range <- trades.data$range$range
    trades <- trades.data$trades
    trades.daily <- trades.data$range$range.daily 
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
    na.omit() #|> 
  # dplyr::group_nest(time) |>
  # dplyr::mutate(fixed.trade = purrr::map(data, ChangeTrade)) |>
  # dplyr::select(time, fixed.trade) |>
  # tidyr::unnest(fixed.trade)
  
  if (is.null(trades.daily)) {
    data <- range |> 
      dplyr::rename("dates" = t, "index.strategy" = index)
  } else {
    data <- trades.daily |> 
      dplyr::left_join(range, by = c("dates" = "t")) |> 
      dplyr::select(1:4) |> 
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
  
  p <- plot_ly(data = data, type = 'scatter', mode = "markers", 
               colors = c("#f7b7b4", "#f28f2b")) |> 
    add_trace(type = 'scatter', mode = "lines",inherit = FALSE,
              x = ~dates, y = ~up, 
              line = list(color = "#545AA7"), name = "Upper Boundary",
              showlegend = FALSE) |> 
    add_trace(type = 'scatter', mode = "lines",inherit = FALSE,
              x = ~dates, y = ~down, 
              line = list(color = "#545AA7"), name = "Lower Boundary",
              fill = 'tonexty', opacity = 0.1,
              fillcolor = "#b7b7ff", showlegend = FALSE) |>
    add_trace(type = 'scatter', mode = "lines",inherit = FALSE,
              x = ~dates, y = ~index.strategy, 
              line = list(color = "#000000"), name = "Index",
              showlegend = FALSE) 
  
  if (include_trades_flag) {
    p <- p |> 
      add_markers(data = dplyr::filter(trade.graph, trade.side %in% "In"), 
                  type = 'scatter', inherit = FALSE,
                  x = ~time, y = ~Level, color = ~factor(position), 
                  name = ~paste(position, "In"), marker = list(
                    size = 13, symbol = "circle",
                    line = list(
                      color = 'black',
                      width = 1
                    ))) |>
      add_markers(data = dplyr::filter(trade.graph, trade.side %in% "Out"), 
                  type = 'scatter', inherit = FALSE,
                  x = ~time, y = ~Level, color = ~factor(position), 
                  name = ~paste(position, "Out"), marker = list(
                    symbol = "x", size = 8, 
                    line = list(
                      color = 'black',
                      width = 1
                    ))) 
  }
  p <- p |>
    layout(legend = list(orientation = 'h',
                         xanchor = "center",
                         y = -0.2,
                         x = 0.475,
                         traceorder = "normal"),
           xaxis = list(title = "Date",
                        showgrid = TRUE,
                        dtick = dtick, tickformat = tickFormat,  
                        hoverformat = "%d %b %Y"),
           yaxis = list(title = title.graph,
                        showgrid = TRUE))
  
  return(p)
}

PlotEigenData <- function(eigendata, type, delta_t) {
  
  if (grepl("values",type)) {
    title.graph <- "Eigenvalue"
  } else {
    if (grepl("vectors.perc",type)) {
      title.graph <- "% Eigenvalue on total"
    } else {
      title.graph <- "Eigenvector"
    }
  }
  
  data <- eigendata |> 
    tidyr::pivot_longer(-Dates, names_to = "RCx", values_to = "Value")
  
  dtick <- ifelse(delta_t > 5*365, "M12", "M3")
  tickFormat <- ifelse(delta_t > 5*365, "%Y", "%b-%y")
  
  p <- plot_ly(data = data, x = ~Dates, y = ~Value, color = ~RCx,
               type = 'scatter', mode = "lines") |>
    layout(legend = list(orientation = 'h',
                         xanchor = "center",
                         y = -0.2,
                         x = 0.475),
           xaxis = list(title = "Date",
                        showgrid = TRUE,
                        dtick = dtick, tickformat=tickFormat, hoverformat = "%d %b %Y"),
           yaxis = list(title = title.graph,
                        showgrid = TRUE))
  
  return(p)
}


PlotReplica <- function(raw.returns, dates, calibration.date) {
  M <- nrow(raw.returns)
  
  raw.returns |> 
    dplyr::mutate(time = tail(as.Date(dates),M)) |> 
    tidyr::pivot_longer(-time, names_to = "asset", values_to = "return") |> 
    ggplot(aes(x = time, y = return, linetype = asset)) + 
    geom_line() + 
    tidyquant::theme_tq() +
    labs(x = "Date",
         y = "Return",
         title = "Return Reconstruction with Principal Components") + 
    scale_linetype_discrete(name = "Asset", labels = c("Index", "PCx replica")) + 
    scale_x_date(date_breaks = "years" , date_labels = "%Y")  
}

PlotDistribution <- function(stats) {
  
  p <- stats |> 
    ggplot(aes(x = return)) + 
    geom_density(aes(y = ..scaled..), fill = "#4E79A7", alpha = 0.4) +
    geom_jitter(aes(x = return, y = 0), height = 0.001) + 
    tidyquant::theme_tq() +
    scale_x_continuous(labels = scales::percent) + 
    scale_y_continuous(labels = scales::percent) + 
    labs(x = "Returns distribution",
         y = "Density",
         title = "Distribution of the returns") 
  
  return(plotly::ggplotly(p))
}

PlotBettingResults <- function(stats) {
  p <- stats |> 
    dplyr::count(bet.result) |> 
    dplyr::mutate(n.perc = round(n/sum(n)*100, 2)) |> 
    plotly::plot_ly(
      type = 'bar',
      x = ~bet.result,
      y = ~n.perc,
      color = ~bet.result,
      colors = c("#FF6965", "#5FA777"),
      hovertemplate = paste(
        "<br>Bets %{x}: %{y}%<br>"
      )
    ) |>
    plotly::layout(xaxis = list(title = "Trade type",
                                showgrid = TRUE),
                   yaxis = list(title = "Percentage of bets won/lost",
                                showgrid = TRUE),
                   title = "Percentage of bets won/lost",
                   showlegend = FALSE)
  
  return(p)
}

PlotDuration <- function(stats) {
  min <- min(stats$duration)
  max <- max(stats$duration)
  
  breaks <- ifelse(max > 10, 2, 1) 
  
  p <- stats |> 
    dplyr::count(duration) |> 
    dplyr::mutate(n.perc = round(n/sum(n)*100, 2),
                  cum.n.perc = cumsum(n.perc))  |> 
    plotly::plot_ly(
      type = 'scatter',
      mode = 'line',
      x = ~duration,
      y = ~cum.n.perc,
      hovertemplate = paste(
        "Duration of trades: %{x} days<br>",
        "Cumulative percentage: %{y}%<br>"
      )
    ) |>
    plotly::layout(xaxis = list(title = "Number of days the trade is on",
                                showgrid = TRUE,
                                dtick = breaks),
                   yaxis = list(title = "Cumulative frequency of trades' duration",
                                showgrid = TRUE),
                   title = "Cumulative distribution of trades' duration")
  
  return(p)
}