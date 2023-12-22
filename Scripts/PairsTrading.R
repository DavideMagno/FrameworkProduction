get_max_min <- function(data, indices_table, confidence_flag, tradable_flag,
                        previous_pair = NULL, acceptability_range = 3) {
  
  if (tradable_flag) {
    
    tradable <- indices_table$Index[grepl("Tradable", 
                                          indices_table$Type)]
    
    data <- data[data$Index %in% tradable,]
  }
  
  if (confidence_flag) {
    confidence_range_set <- c("Medium confidence","Medium-High confidence", 
                              "High confidence")
    
    data <- data[data$`Fair Value Estimation Confidence` %in% confidence_range_set,]
  }
  
  data <- data[order(data$`Distance from Estimated Fair Value (z-score)`),] 
  
  zscore <- data$`Distance from Estimated Fair Value (z-score)`
  
  min_index <- data$Index[which(zscore == min(zscore))]
  max_index <- data$Index[which(zscore == max(zscore))]
  
  if (!is.null(previous_pair) & !is.null(acceptability_range)) {
    long_position <- which(data$Index == previous_pair$Long)
    short_position <- which(data$Index == previous_pair$Short)
    
    if (length(long_position) > 0) {
      if (long_position <= acceptability_range) {
        min_index <- previous_pair$Long
      }
    } 
    
    if (length(short_position) > 0) {
      if (short_position >= (nrow(data) - acceptability_range)) {
        max_index <- previous_pair$Short
      }
    } 
  }
  
  portfolio <- data.frame(Long = min_index, Short = max_index) 
  
  return(portfolio)
}

calculate_pairs_trading <- function(distance, FrameworkIndices, 
                                    start_date, end_date,
                                    tradable_flag, confidence_flag,
                                    acceptability_range) {
  
  if ("Taboo" %in% colnames(FrameworkIndices)) {
    
    `%!in%` <- Negate(`%in%`)
    taboo_indices <- FrameworkIndices$Index[FrameworkIndices$Taboo]
    
    distance <- distance[distance$Index %!in% taboo_indices,]
  }
  
  pairs_trade <- distance |> 
    dplyr::group_nest(Dates)
  
  pairs_trade <- pairs_trade[pairs_trade$Dates >= start_date & pairs_trade$Dates <= end_date,]
  pairs_trade <- pairs_trade[order(pairs_trade$Dates),]
  
  ### Trading on Wednesdays
  dates <- pairs_trade$Dates
  
  pairs_trade <- pairs_trade |> 
    dplyr::mutate(weekday = weekdays(Dates)) |> 
    dplyr::filter(weekday %in% "Wednesday") |> 
    dplyr::select(-weekday)
  ####
  
  if (acceptability_range > 0) {
    Long_col <- c()
    Short_col <- c()
    
    for (row in 1:nrow(pairs_trade)) {
      if (row == 1) {
        assets_to_trade <- get_max_min(pairs_trade$data[[row]],
                                       FrameworkIndices, confidence_flag, tradable_flag)
      } else {
        assets_to_trade <- get_max_min(pairs_trade$data[[row]],
                                       FrameworkIndices, confidence_flag, tradable_flag, assets_to_trade_old,
                                       acceptability_range)
      }
      Long_col <- c(Long_col, assets_to_trade$Long)
      Short_col <- c(Short_col, assets_to_trade$Short)
      assets_to_trade_old <- assets_to_trade
    }
    pairs_trade <- pairs_trade |> 
      dplyr::bind_cols(data.frame(Long = Long_col, Short = Short_col)) |> 
      dplyr::select(-data)
  } else {
    pairs_trade <- pairs_trade |> 
      dplyr::mutate(assets_to_trade = purrr::map(data,
                                                 ~get_max_min(.x, 
                                                              FrameworkIndices, 
                                                              tradable_flag, 
                                                              confidence_flag))) |> 
      tidyr::unnest(assets_to_trade) |> 
      dplyr::select(-data)
  }
  pairs_trade <- pairs_trade |> 
    dplyr::mutate(pair = glue::glue("{Long} ~ {Short}"))
  
  pairs_trade_complete <- data.frame(Dates = dates) |> 
    dplyr::left_join(pairs_trade, by = "Dates") |> 
    tidyr::fill(!Dates, .direction = "downup")
  
  return(pairs_trade_complete)
}

convert_format_pairs_trade <- function(pairs_trade) {
  
  long_tables <- pairs_trade |> 
    dplyr::pull("Long") |> 
    unique() |> 
    as.list() |> 
    purrr::map(~calculate_start_end(pairs_trade, .x, "Long")) |> 
    purrr::list_rbind()
  
  short_tables <- pairs_trade |> 
    dplyr::pull("Short") |> 
    unique() |> 
    as.list() |> 
    purrr::map(~calculate_start_end(pairs_trade, .x, "Short")) |> 
    purrr::list_rbind()
  
  table <- dplyr::bind_rows(long_tables, short_tables)
  
  return(table)
}

calculate_start_end <- function(pairs_trade, index, side) {
  positions <- which(pairs_trade[[side]] %in% index)
  
  start <- c()
  end <- c()
  
  for (i in 1:length(positions)) {
    if (i == 1) {
      start <- c(start, positions[i])
      if (i == length(positions)) {
        end <- c(end, min(positions[i] + 1, nrow(pairs_trade)))
      }
      next
    } 
    if (positions[i] != positions[i - 1] + 1) {
      end <- c(end, positions[i - 1] + 1)
      start <- c(start, positions[i])
    }
    if (i == length(positions)) {
      end <- c(end, min(positions[i] + 1, nrow(pairs_trade)))
    }
  }
  
  data.frame(Position = side,
             Index = index,
             start = pairs_trade$Dates[start],
             end = pairs_trade$Dates[end],
             color = dplyr::case_when(
               side %in% "Long" ~ "#00bfc4",
               side %in% "Short" ~ "#f8766d",
               .default = "#000000"
             ))
  
}

plot_pairs <- function(pairs_trade) {
  x <- unique(pairs_trade$Long) 
  y <- unique(pairs_trade$Short)
  
  assets <- union(x, y) |> 
    sort(decreasing = TRUE)
  
  test <- pairs_trade |> 
    dplyr::left_join(data.frame(x = assets,
                                "Long_num" = seq(length(assets)) - 1), 
                     by = c("Long" = "x")) |> 
    dplyr::left_join(data.frame(y = assets,
                                "Short_num" = seq(length(assets)) - 1), 
                     by = c("Short" = "y")) 
  
  fntltp <- JS("function(){
                  return Highcharts.dateFormat('%d-%m-%y', this.point.x) + ' ~ ' +
                         this.series.yAxis.categories[this.point.y];}")

  hc <- highchart() |>
    hc_add_series(type = "scatter", data = dplyr::select(test, Dates, Long_num),
                  hcaes(x = Dates, y = Long_num), name = "Long Trades") |>
    hc_add_series(type = "scatter", data = dplyr::select(test, Dates, Short_num),
                  hcaes(x = Dates, y = Short_num), color = "red", name = "Short Trades",
                  marker = list(symbol = "circle")) |> 
    hc_yAxis(categories = assets) |>
    hc_xAxis(type = "datetime", 
             crosshair = TRUE) |> 
    hc_add_theme(my_hc_theme) |> 
    hc_tooltip(formatter = fntltp) |> 
    hc_exporting(enabled = TRUE) |> 
    hc_rangeSelector(enabled = TRUE,
                     labelStyle = list(color = "white"),
                     inputStyle = list(color = "grey")) |> 
    hc_caption(text = "Source: HedgeAnalytics")
  
  return(hc)
}

plot_heatmap <- function(portfolio, portfolio_name) {
  x <- portfolio |> 
    dplyr::pull("Long") |> 
    unique() |> 
    sort(decreasing = FALSE)
  
  y <- portfolio |> 
    dplyr::pull("Short") |> 
    unique() |> 
    sort(decreasing = TRUE)
  
  heatmap <- portfolio |> 
    dplyr::count(Long,Short) |> 
    dplyr::arrange(Long, Short) |> 
    dplyr::rename("Days" = n) |> 
    dplyr::left_join(data.frame(x = x,
                                "Longid" = seq(length(x)) - 1), 
                     by = c("Long" = "x")) |> 
    dplyr::left_join(data.frame(y = y,
                                "Shortid" = seq(length(y)) - 1), 
                     by = c("Short" = "y")) |> 
    dplyr::select(Longid, Shortid, Days) 
  
  fntltp <- JS("function(){
                  return this.series.xAxis.categories[this.point.x] + ' ~ ' +
                         this.series.yAxis.categories[this.point.y] + ': <b>' +
                         Highcharts.numberFormat(this.point.value, 0)+'</b>';
               ; }")
  
  cor_colr <- list( list(0, '#F0FFFE'),
                    list(0.5, '#90CCFE'),
                    list(1, '#007FFE')
  )
  chart <- highchart() |> 
    hc_chart(type = "heatmap") |> 
    hc_xAxis(categories = x, title = list(text = "Long Trades")) |> 
    hc_yAxis(categories = y, title = list(text = "Short Trades")) |> 
    hc_add_series(data = list_parse2(heatmap)) |> 
    hc_plotOptions(
      series = list(
        boderWidth = 0,
        dataLabels = list(enabled = TRUE)
      )) |> 
    hc_tooltip(formatter = fntltp) |> 
    hc_legend(align = "right", layout = "vertical",
              margin = 0, verticalAlign = "top",
              y = 25, symbolHeight = 280) |> 
    hc_colorAxis(stops= cor_colr, min = min(heatmap$Days), max = max(heatmap$Days)) |> 
    hc_add_theme(my_hc_theme) |> 
    hc_caption(text = "Source: HedgeAnalytics")
  
  return(chart)
}

get_returns <- function(asset, dir_home, asset_manager, K) {
  returns <- glue::glue("{dir_home}/Data/output{asset_manager}/{asset}/K={K}/LAG=252/portfolio.return") |>
    arrow::open_dataset() |> 
    dplyr::collect() |> 
    dplyr::select(Dates, index = `Long Only Strategy`) |> 
    dplyr::rename(!!asset := index)
  
  return(returns)
}

get_weights <- function(portfolio) {
  portfolio <- portfolio |> 
    dplyr::arrange(Dates)
  
  weights <- NULL
  
  for (row in 1:nrow(portfolio)) {
    weights_date <- data.table::data.table(Dates = portfolio$Dates[row],
                                           Long = 1,
                                           Short = -1) 
    
    colnames(weights_date) <- c("Dates", portfolio$Long[row], 
                                portfolio$Short[row])
    
    weights <- rbind(weights, weights_date, fill = TRUE)
  }
  
  weights[is.na(weights)] <- 0
  return(weights)
}

analyse_strategy <- function(port, dir_home, asset_manager, K) {
  weights <- get_weights(port) 
  
  assets <- c(port$Long, port$Short) |> 
    unique()
  
  returns <- purrr::map(assets, ~get_returns(.x, dir_home, asset_manager, K)) |> 
    purrr::reduce(dplyr::full_join, by = "Dates") |> 
    dplyr::arrange(Dates)
  
  returns <- returns[names(weights)] |> 
    dplyr::filter(Dates %in% weights$Dates) |> 
    {\(x) replace(x, is.na(x), 0)}() |> 
    tail(-1)
  
  returns_xts <- xts::xts(cbind(dplyr::select(returns, -Dates),
                                rep(0, nrow(returns))),
                          order.by = returns$Dates)
  
  weights <- head(weights, -1) |> 
    as.data.frame()
  
  weights_xts <- xts::xts(cbind(dplyr::select(weights, -Dates),
                                rep(1, nrow(weights))),
                          order.by = weights$Dates)
  
  
  r.strategy <- PerformanceAnalytics::Return.portfolio(returns_xts, weights = weights_xts)
  return(list(r.strategy = r.strategy, returns = returns))
}


calculate_stats <- function(pairs_trading, pairs_performance) {

  ret <- pairs_performance |> 
    purrr::pluck("returns") |> 
    tidyr::pivot_longer(cols = -Dates,
                        names_to = "Index",
                        values_to = "Return")
  
  r.strategy <- pairs_performance |> 
    purrr::pluck("r.strategy")
  
  returns <- as.numeric(r.strategy)
  
  returns.df <- data.frame(Dates = index(r.strategy), 
                           return_strategy = unname(coredata(r.strategy))) 

  bets <- pairs_trading |> 
    dplyr::left_join(ret, by = c("Dates", "Long" = "Index")) |> 
    dplyr::rename("Return Long" = Return) |> 
    dplyr::left_join(ret, by = c("Dates", "Short" = "Index")) |> 
    dplyr::rename("Return Short" = Return) |> 
    # na.omit() |> 
    dplyr::mutate(bet.result = dplyr::case_when(
      `Return Long` > 0 & `Return Short` < 0 ~ "WON",
      `Return Long` < 0 & `Return Short` > 0 ~ "LOST",
      .default = "MIXED")) |> 
    dplyr::left_join(returns.df, by = c("Dates")) |> 
    dplyr::select(pair, bet.result, return_strategy)
  
  duration <- rep(NA_integer_, nrow(pairs_trading))
  
  duration[1] <- 1
  
  for (i in 2:(nrow(pairs_trading))) {
    if (pairs_trading$pair[i] == pairs_trading$pair[i-1]) {
      duration[i] <- duration[i - 1] + 1
    } else {
      duration[i] <- 1
      duration[(i - (duration[i -1])):(i-1)] <- duration[i -1]
    }
  }
  
  duration <- bets |> 
    dplyr::mutate(duration = duration[1:length(duration)]) |> 
    dplyr::group_by(pair, duration) |> 
    dplyr::summarise(return = prod(1 + return_strategy) - 1) |> 
    dplyr::ungroup() |> 
    dplyr::select(-pair)
  
  return(list(
    bets = na.omit(dplyr::select(bets, -pair)),
    returns = returns,
    duration = na.omit(duration)
  ))
}