source("~/FrameworkProduction/Scripts/InvarianceTest.R")
source("~/FrameworkProduction/Scripts/SVD.R")
source("~/FrameworkProduction/Scripts/Portfolios.R")
source("~/FrameworkProduction/Scripts/myDatepickerInput.R")
source("~/FrameworkProduction/Scripts/RelativeTrade.R")
source("~/FrameworkProduction/Scripts/Hedging.R")
source("~/FrameworkProduction/Scripts/FrameworkFunctions.R")
source("~/FrameworkProduction/Scripts/xts_graphs.R")

options(shiny.reactlog = TRUE)
# thematic::thematic_shiny(font = "auto")

asset_manager <- "_ccy1y"

`%not_in%` <- Negate("%in%")

lag.options <- c("Monthly" = 1, "Quarterly" = 3)

dates.options <- c("2 Years" = 2, "3 Years" = 3, "5 Years" = 5,
                   "Max" = "Max")

stats.choice <- c("Annualised Returns", "Annualised Volatility",
                  "Annualised Sharpe Ratio", "Conditional Drawdown",
                  "Maximum Drawdown")

# type <- c(rep(TRUE, 12), FALSE, rep(TRUE, 3), rep(FALSE, 7), rep(TRUE, 14))

CalculateType <- function(prices, dropdown_am = "") {
  
  load(glue::glue("/home/threshold/FrameworkData/Data/IndexMapping{dropdown_am}.RData"))
  
  type <- tibble::tibble(Index = tail(colnames(prices$prices),-1)) |> 
    dplyr::left_join(FrameworkIndices, by = "Index") |> 
    dplyr::mutate(type_flag = dplyr::if_else(grepl("Log", invariant), TRUE, FALSE)) |> 
    dplyr::pull(type_flag) 
  
  return(type)
}

CalculateTradable <- function(prices, dropdown_am = "") {
  load(glue::glue("/home/threshold/FrameworkData/Data/IndexMapping{dropdown_am}.RData"))
  tradable_flag <- tibble::tibble(Index = tail(colnames(prices$prices),-1)) |> 
    dplyr::left_join(FrameworkIndices, by = "Index") |> 
    dplyr::mutate(type_flag = dplyr::if_else(grepl("Tradable", Type), TRUE, FALSE)) |> 
    dplyr::pull(type_flag) 
  
  return(tradable_flag)
}


assetTags <- function(portfolio, name) {
  
  if(!is.null(portfolio$`Long Indices`$names)) {
    longTag <- checkboxGroupButtons(
      direction = "horizontal",
      inputId = glue::glue("id_long_{name}"),
      label = "Long Indices",
      choiceNames = portfolio$`Long Indices`$names,
      choiceValues = portfolio$`Long Indices`$positions,
      selected = portfolio$`Long Indices`$positions
    )
  } else {
    longTag <- ""
  }
  
  if(!is.null(portfolio$`Short Indices`$names)) {
    shortTag <- checkboxGroupButtons(
      direction = "horizontal",
      inputId = glue::glue("id_short_{name}"),
      label = "Short Indices",
      choiceNames = portfolio$`Short Indices`$names,
      choiceValues = portfolio$`Short Indices`$positions,
      selected = portfolio$`Short Indices`$positions
    )
  } else {
    shortTag <- ""
  }
  
  tag <- 
    # tagList(
    card(
      # width = "4 col-lg-3",
      card_title(
        glue::glue("Factor {name}")
      ),
      card_body(
        longTag,
        shortTag
      )
    )
  # )
  
  return(tag)
}

CreateInputTags <- function(portfolio, name) {
  input.tag <- c()
  if(!is.null(portfolio$`Long Indices`$names))
    input.tag <- c(input.tag, glue::glue({"long_{name}"}))
  if(!is.null(portfolio$`Short Indices`$names))
    input.tag <- c(input.tag, glue::glue({"short_{name}"}))
  return(input.tag)
}

GraphReturns <- function(portfolio.returns) {
  
  plot <- portfolio.returns |> 
    PerformanceAnalytics::apply.rolling(1) |> 
    as.data.frame() |> 
    tibble::rownames_to_column() |> 
    dplyr::rename_all(~c("Date", "Selected Portfolio", "Algo Portfolio",
                         "Equal Weights Portfolio")) |> 
    tidyr::pivot_longer(-Date, names_to = "Portofolio", values_to = "Returns") |> 
    plot_ly(type = "scatter", mode = "lines+markers") |>  
    add_trace(x = ~Date, y = ~Returns, color = ~Portfolio) |> 
    layout(legend = list(title = list(text = "Portfolio"),
                         orientation = 'h'),
           yaxis = list(title = "Cumulative Return",
                        tickformat= ".3%"),
           xaxis = list(title = "Date"))
}

GraphDistributions <- function(stats, variable) {
  
  graph <- stats |> 
    purrr::transpose() |> 
    purrr::pluck(variable) |> 
    as.data.frame() |> 
    tidyr::pivot_longer(cols = dplyr::everything(),
                        names_to = "Portfolio",
                        values_to = "Statistic") |> 
    ggplot(aes(Statistic, fill = Portfolio, colour = Portfolio)) + 
    geom_density(alpha = 0.1) + 
    tidyquant::theme_tq() +
    labs(x = variable,
         y = "Density") 
  
  return(graph)
}

SplitTimeSet <- function(returns, prices, training_dates, keep.dates.flag = FALSE) {
  training.set <- returns |> 
    dplyr::filter(dates >= training_dates[1], 
                  dates <= training_dates[2])
  
  testing.set <- returns |> 
    dplyr::filter(dates > training_dates[2]) 
  
  prices.training.set <- prices |> 
    dplyr::filter(dates >= training_dates[1], 
                  dates <= training_dates[2]) 
  
  if (!keep.dates.flag) {
    training.set <- training.set |> 
      dplyr::select(-dates)
    
    prices.training.set <- prices.training.set |> 
      dplyr::select(-dates)
  }
  
  prices.testing.set <- prices |> 
    dplyr::filter(dates > training_dates[2]) 
  
  return(list(training.set = training.set,
              testing.set = testing.set,
              prices.testing.set = prices.testing.set,
              prices.training.set = prices.training.set))
  
}

mytheme <- fresh::create_theme(
  fresh::adminlte_color(
    light_blue = "#434C5E"
  ),
  fresh::adminlte_sidebar(
    width = "400px",
    dark_bg = "#D8DEE9",
    dark_hover_bg = "#81A1C1",
    dark_color = "#2E3440"
  ),
  fresh::adminlte_global(
    content_bg = "#FFF",
    box_bg = "#D8DEE9", 
    info_box_bg = "#D8DEE9"
  )
)