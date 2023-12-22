na.skip <- function (x, FUN=NULL, ...) 
{ 
  nx <- na.omit(x)
  fx <- FUN(nx, ... = ...)
  if (is.vector(fx)) {
    result <- .xts(fx, .index(x), .indexCLASS = indexClass(x))
  }
  else {
    result <- merge(fx, .xts(, .index(x)))
  }
  return(result)
}

calculate_cumulative_returns <- function (R, wealth.index = FALSE, geometric = TRUE, legend.loc = NULL, 
                                          begin = c("first", "axis"), ...) 
{
  begin = begin[1]
  x = checkData(R)
  columns = ncol(x)
  columnnames = colnames(x)
  one = 0
  if (!wealth.index) 
    one = 1
  if (begin == "first") {
    length.column.one = length(x[, 1])
    start.row = 1
    start.index = 0
    while (is.na(x[start.row, 1])) {
      start.row = start.row + 1
    }
    x = x[start.row:length.column.one, ]
    if (geometric) 
      reference.index = na.skip(x[, 1], FUN = function(x) {
        cumprod(1 + x)
      })
    else reference.index = na.skip(x[, 1], FUN = function(x) {
      cumsum(x)
    })
  }
  for (column in 1:columns) {
    if (begin == "axis") {
      start.index = FALSE
    }
    else {
      start.row = 1
      while (is.na(x[start.row, column])) {
        start.row = start.row + 1
      }
      start.index = ifelse(start.row > 1, TRUE, FALSE)
    }
    if (start.index) {
      if (geometric) 
        z = na.skip(x[, column], FUN = function(x, index = reference.index[(start.row - 
                                                                              1)]) {
          rbind(index, 1 + x)
        })
      else z = na.skip(x[, column], FUN = function(x, 
                                                   index = reference.index[(start.row - 1)]) {
        rbind(1 + index, 1 + x)
      })
    }
    else {
      z = 1 + x[, column]
    }
    column.Return.cumulative = na.skip(z, FUN = function(x, 
                                                         one, geometric) {
      if (geometric) 
        cumprod(x) - one
      else (1 - one) + cumsum(x - 1)
    }, one = one, geometric = geometric)
    if (column == 1) 
      Return.cumulative = column.Return.cumulative
    else Return.cumulative = merge(Return.cumulative, column.Return.cumulative)
  }
  if (columns == 1) 
    Return.cumulative = as.xts(Return.cumulative)
  colnames(Return.cumulative) = columnnames
  return(Return.cumulative)
}

my_hc_theme <- highcharter::hc_theme_superheroes(
  chart = list(backgroundColor = "#1e3142"),
  xAxis = list(
    # tickColor = "#FFFFFF",
    labels = list(style = list(color = "#FFFFFF")),
    title = list(style = list(color = "#FFFFFF"))),
  yAxis = list(
    # tickColor = "#FFFFFF",
    labels = list(style = list(color = "#FFFFFF")),
    title = list(style = list(color = "#FFFFFF")))
)



performance_graphs <- function(performance, geometric = TRUE, panel) {
  
  returns <- calculate_cumulative_returns(tail(performance, -1), wealth.index = FALSE,
                                          geometric = geometric)
  drawdowns <- Drawdowns(tail(performance, -1))
  
  name_1 <- dplyr::case_match(panel,
                              "Self-hedging Portfolios" ~ "Selected Portfolio",
                              "Relative Value" ~ "Long Only",
                              "Hedging/Replication" ~ "Index",
                              "Pairs Trading" ~ "Pairs Trading Strategy")
  
  flag_portfolio_n <- dplyr::case_match(panel,
                                        "Self-hedging Portfolios" ~ 3,
                                        "Relative Value" ~ 2,
                                        "Hedging/Replication" ~ 2,
                                        "Pairs Trading" ~ 1)
  
  colors <- c("#f1c40f", "#2ecc71", "#9b59b6")
  
  hc <- highchart(type = "stock") |>
    hc_yAxis_multiples(create_axis(naxis = 2, showLastLabel = TRUE,
                                   heights = c(2, 1), sep = 0.05,
                                   title = c(list(list(text = "Cumulative Performance")),
                                             list(list(text = "Drawdown"))),
                                   labels = c(list(list(format = "{value}%")),
                                              list(list(format = "{value}%"))))) |>
    hc_add_series(returns[,1]*100, yAxis = 0, name = name_1,
                  color = colors[1]) |>
    hc_add_series(drawdowns[,1]*100,  yAxis = 1, name = name_1,
                  color = colors[1], showInLegend = FALSE) 
  
  if (flag_portfolio_n > 1) {
    name_2 <- dplyr::case_match(panel,
                                "Self-hedging Portfolios" ~ "Algo Portfolio",
                                "Relative Value" ~ "Relative Value Strategy",
                                "Hedging/Replication" ~ "Hedging Portfolio")
    
    hc <- hc |> 
      hc_add_series(returns[,2]*100, yAxis = 0, name = name_2,
                    color = colors[2]) |>
      hc_add_series(drawdowns[,2]*100,  yAxis = 1, name = name_2,
                    color = colors[2], showInLegend = FALSE)
  }
  
  if (flag_portfolio_n > 2) {
    name_3 <- "Equal Weights Portfolio"
    
    hc <- hc |> 
      hc_add_series(returns[,3]*100, yAxis = 0, name = name_3,
                    color = colors[3]) |>
      hc_add_series(drawdowns[,3]*100,  yAxis = 1, name = name_3,
                    color = colors[3], showInLegend = FALSE)
  }
  
  hc <- hc |> 
    hc_tooltip(valueDecimals = 2) |>
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

