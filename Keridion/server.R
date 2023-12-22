library(shiny)
library(openxlsx)
library(PerformanceAnalytics)
library(shinycssloaders)
library(sparseIndexTracking)
library(highcharter)
library(reactlog)

shinyServer(function(input, output, session) {
  LAG <- reactive({252})
  K <- reactive(as.numeric(input$factors_chosen))
  REBALANCE <- reactive(1)
  ROLLING.WINDOW <- reactive(as.numeric(input$rolling.window))
  INPUT.DATA.TYPE <- reactive({"Daily"})
  
  # Returns, Dates and Prices ------------------------------------------------------
  
  FrameworkIndices <- reactive({
    load(glue::glue("/home/threshold/FrameworkData/Data/IndexMapping{input$asset_manager}.RData"))
    
    if (any(colnames(FrameworkIndices) %in% "Taboo")) {
      FrameworkIndices <- FrameworkIndices |> 
        dplyr::filter(!Taboo)
    }
    
    return(FrameworkIndices)
  })
  
  prices <- reactive({
    req(FrameworkIndices(), input$dates_prices)
    
    dir <- glue::glue("/home/threshold/FrameworkData/Data/input{input$asset_manager}")
    
    indices <- FrameworkIndices()$Index
    prices <- CalculatePrices(252, start_date = input$dates_prices[1],
                              end_date = input$dates_prices[2], dir, indices)
    return(prices)
  })
  
  type <- reactive({
    req(prices())
    
    CalculateType(prices(), FrameworkIndices = FrameworkIndices())
  })
  
  type_index <- reactive({
    req(prices(), !is.na(type()))
    
    index <- input$index.strategy
    pos <- which(tail(colnames(prices()$prices), -1) %in% index)
    
    return(type()[pos])
    
  })
  
  tradable_flag <- reactive({
    req(prices())
    CalculateTradable(prices(), FrameworkIndices = FrameworkIndices())
  })
  
  
  returns <- reactive({
    req(prices(), !is.na(type()))
    returns <- CalculateReturns(prices(), type())
    return(returns)
    
  })
  
  split.returns <- reactive({
    req(returns(), prices(), input$training_dates)
    
    SplitTimeSet(returns()$returns, prices()$prices, 
                 input$training_dates)
  })
  
  split.returns.economical <- reactive({
    req(returns(), prices(), input$training_dates)
    
    SplitTimeSet(returns()$returns.cut, prices()$prices.cut, 
                 input$training_dates)
  })
  
  split.returns.hedging <- reactive({
    req(returns(), prices(), input$training_dates_hedging)
    
    SplitTimeSet(returns()$returns, prices()$prices, 
                 input$training_dates_hedging, TRUE)
  })
  
  split.returns.hedging.economical <- reactive({
    req(returns(), prices(), input$training_dates_hedging)
    
    SplitTimeSet(returns()$returns.cut, prices()$prices.cut, 
                 input$training_dates_hedging, TRUE)
  })
  
  
  # Relative Trade Reactives ------------------------------------------------
  
  statistics <- reactive({
    req(relative_value())
    relative.trade <- list(type = type_index(),
                           trades = relative_value()$trades)
    
    CalculateStatisticsRT(relative.trade)
  })
  
  stats.relative.trade <- reactive({
    req(relative_value())
    
    return <- PerformanceAnalytics::Return.annualized(relative_value()$performance[,c(2,1)],
                                                      geometric = type_index())
    
    sd <- PerformanceAnalytics::sd.annualized(relative_value()$performance[,c(2,1)],
                                              scale = 252)
    
    sharpe <- PerformanceAnalytics::SharpeRatio.annualized(relative_value()$performance[,c(2,1)],
                                                           scale = 252, 
                                                           geometric = type_index())
    
    drawdown <- PerformanceAnalytics::maxDrawdown(relative_value()$performance[,c(2,1)],
                                                  geometric = type_index())
    
    var <- PerformanceAnalytics::VaR(relative_value()$performance[,c(2,1)])
    
    rbind(return, sd, sharpe, drawdown, var) |> 
      as.data.frame() |> 
      tibble::rownames_to_column() |> 
      dplyr::rename_all(~c("Metric", "Relative Value Strategy", "Long Only")) |> 
      dplyr::mutate(Metric = c("Annualised Return", "Annualised Std Dev",
                               "Annualised Sharpe (Rf=0%)", 
                               "Maximum Drawdown",
                               "Historical VaR (95%)"))
  })
  
  landing_table <- reactive({
    req(input$dates_prices)
    
    start_date <- input$dates_prices[1]
    end_date <- input$dates_prices[2]
    dir <- glue::glue("/home/threshold/FrameworkData/Data/output_distance{input$asset_manager}")
    
    market_data <- glue::glue("{dir}/K={K()}") |> 
      arrow::open_dataset() |>
      dplyr::collect() |> 
      dplyr::filter(Dates %in% end_date) |> 
      dplyr::select(-YearMonth) 
    
    table <- FrameworkIndices() |> 
      dplyr::mutate(Index = as.character(Index)) |> 
      dplyr::select(Index,`Long Name`, Currency,`Starting Date`,`Asset Class`) |> 
      dplyr::left_join(market_data, by = "Index") |> 
      dplyr::rename("Ticker" = Index)
    
    return(table)
  })
  
  lending_table_dt <- reactive({
    req(landing_table())
    table <- DT::datatable(landing_table(), fillContainer = TRUE, 
                           style = "bootstrap4",
                           selection = "single",
                           options = list(dom = "t",
                                          filter = list(position = "top"),
                                          sDom  = '<"top">lrt<"bottom">ip',
                                          paging = FALSE)) |> 
      DT::formatRound("Current Index Value", 2) |> 
      DT::formatRound("Distance from Estimated Fair Value (z-score)", 2)  
    
    return(table)
  })
  
  
  # Portfolio Reactives -----------------------------------------------------
  
  portfolios <- reactive({
    req(split.returns.economical(), tradable_flag())
    training.data <- split.returns.economical()$training.set
    # if(is.null(input.file.economical()) | 
    # input$choose.frequency.portfolio == "1") {
    items.to.remove <- 0
    # } else {
    #   items.to.remove <- which(grepl("Market", market.type.economical())) |> 
    #     length()
    # }
    
    training.data <- training.data[,tradable_flag()]
    
    test.valid.data <- purrr::map_lgl(training.data,
                                      ~(any(is.na(.x)) | all(.x == 0)))
    training.data <- training.data[,!test.valid.data]
    
    trained.model <- RunSVD(training.data = training.data, 
                            k = K(), 
                            rotation.flag = TRUE) 
    
    purrr::map(1:K(), ~ExtractFeatures(trained.model$L, K(), .x, 
                                       training.data,
                                       input$threshold, items.to.remove)) |> 
      purrr::set_names(glue::glue("RC{1:K()}"))
  })
  
  
  chosen.portfolios <- reactive({
    req(portfolios())
    # inputs <- names(reactiveValuesToList(input)) |> 
    #   stringr::str_detect("id_") |> 
    #   {\(x) reactiveValuesToList(input)[x]}()
    
    inputs <- list() 
    names <- c()
    for (i in 1:length(portfolios())) {
      for (j in c("long", "short")) {
        name <- glue::glue("id_{j}_RC{i}")
        item  <- input[[name]]
        names <- c(names, name)
        inputs <- c(inputs, list(item))
      }
    }
    
    names(inputs) <- names
    
    if (length(inputs) > 0) {
      chosen.portfolio <- purrr::imap(portfolios(), CreateInputTags) |> 
        purrr::reduce(c) |> 
        CreateCustomPortfolio(portfolios(), inputs) |> 
        CreatePortfolio(input$factors_chosen, length(type()))
      
      full.portfolio <- CreatePortfolio(portfolios(), input$factors_chosen,
                                        length(type()))
    } else {
      chosen.portfolio <- NULL
      full.portfolio <- NULL
    }
    return(list(chosen.portfolio = chosen.portfolio, 
                full.portfolio = full.portfolio,
                equal.portfolio = rep(1/length(type()), length(type()))))
  })
  
  portfolio.returns <- reactive({
    req(chosen.portfolios())
    # if (input$choose.frequency.portfolio == 1) {
    #   testing.set <- split.returns()$testing.set
    # } else {
    testing.set <- split.returns.economical()$testing.set
    # }
    
    N <- unique(purrr::map_dbl(chosen.portfolios(), length))
    testing.returns <- xts::xts(testing.set[,-1], 
                                order.by = testing.set$dates)
    testing.returns <- testing.returns[,1:N]
    
    
    portfolio.returns <- chosen.portfolios() |> 
      purrr::map(
        ~PerformanceAnalytics::Return.portfolio(testing.returns, 
                                                weights = .x,
                                                rebalance_on = "days")) |> 
      purrr::reduce(cbind) 
    
    dimnames(portfolio.returns)[[2]] <- c("Selected Portfolio", 
                                          "Algo Portfolio",
                                          "Equal Weights Portfolio")
    return(portfolio.returns)
  })
  
  simulation.results <- eventReactive(input$button, {
    if (input$choose.frequency.portfolio == "1") {
      returns <- returns()$returns
    } else {
      returns <- returns()$returns.monthly
      remove <- length(type.economical()) - length(type())
      returns <- returns[,1:(ncol(returns) - remove)]
    }
    
    RandomAnalysis(returns, input$simulations, LAG(),
                   input$factors_chosen, input$threshold,
                   chosen.portfolios())
  })
  
  stats <- reactive({
    req(portfolio.returns())
    
    param <- list(PerformanceAnalytics::Return.annualized, 
                  PerformanceAnalytics::sd.annualized,
                  PerformanceAnalytics::SharpeRatio.annualized,
                  PerformanceAnalytics::maxDrawdown,
                  PerformanceAnalytics::VaR)
    
    purrr::map(param, ~.x(portfolio.returns())) |> 
      purrr::reduce(rbind) |> 
      as.data.frame() |> 
      tibble::rownames_to_column() |> 
      dplyr::rename_all(~c("Metric", "Selected Portfolio", "Algo Portfolio",
                           "Equal Weights Portfolio")) |> 
      dplyr::mutate(Metric = c("Annualised Return", "Annualised Std Dev",
                               "Annualised Sharpe (Rf=0%)", 
                               "Maximum Drawdown",
                               "Historical VaR (95%)"))
  })
  
  
  # Hedging Reactives -------------------------------------------------------
  
  hedging.assets <- reactive({
    req(returns(), tradable_flag())
    
    assets <- colnames(returns()$returns.monthly[-1])
    
    # assets <- assets[tradable_flag()]
    
    assets.for.replication <- assets[tradable_flag()]
    
    return(list(assets = assets,
                assets.for.replication = assets.for.replication))
  })
  
  hedging <- reactive({
    req(split.returns.hedging.economical(),
        input$index.hedging)
    
    # if (input$choose.frequency.hedging == "1") {
    #   training.set <- split.returns.hedging()$training.set
    #   testing.set <- split.returns.hedging()$testing.set
    # }
    # else {
    training.set <- split.returns.hedging.economical()$training.set
    testing.set <- split.returns.hedging.economical()$testing.set
    # } 
    
    xts.training.set <- SliceData(input$index.hedging, training.set,
                                  hedging.assets()$assets.for.replication,
                                  input$use.shorts)
    xts.testing.set <- SliceData(input$index.hedging, testing.set,
                                 hedging.assets()$assets.for.replication,
                                 input$use.shorts)
    
    hedge <- CalculateHedge(xts.training.set, xts.testing.set,
                            input$dropdown_method,
                            input$max_weight)
    
    graph.weight <- GraphWeights(hedge$w, FrameworkIndices())
    
    training.returns <- GraphHedgingReturns(xts.training.set$target,
                                            hedge$r.training)
    
    testing.returns <- GraphHedgingReturns(xts.testing.set$target,
                                           hedge$r.testing)
    
    returns <- rbind(training.returns, testing.returns)
    
    table.results <- rbind(table.AnnualizedReturns(returns)[1:3,], 
                           table.DownsideRisk(returns)[7:8,]) |> 
      tibble::rownames_to_column(var = "Metric")
    
    hedge.efficiency.training <- 
      1 - StdDev.annualized(training.returns[,3])/StdDev.annualized(training.returns[,1]) |> 
      as.numeric()
    
    hedge.efficiency.testing <- 
      1 - StdDev.annualized(testing.returns[,3])/StdDev.annualized(testing.returns[,1]) |> 
      as.numeric()
    
    return(list(hedge = hedge,
                graph.weight = graph.weight,
                graphs.training = training.returns,
                graphs.testing = testing.returns,
                hedge.efficiency.training = hedge.efficiency.training,
                hedge.efficiency.testing = hedge.efficiency.testing,
                table.results = table.results))
  })
  
  graphs.training <- reactive({
    req(heding())
    
    GraphReturns()
  })
  
  # Observers ---------------------------------------------------------------
  
  
  output$page_report <- downloadHandler(
    filename <- function(index) {
      glue::glue("{input$index.strategy}IndexReport.xlsx")
    },
    content <- function(file) {
      
      range <- relative_value()$range
      trades <- relative_value()$trades
      trades.daily <- relative_value()$replica.daily
      
      if (is.null(trades.daily)) {
        data <- range |> 
          dplyr::rename("dates" = t) |> 
          dplyr::mutate(index.strategy = index)
      } else {
        data <- trades.daily |> 
          dplyr::left_join(range, by = c("dates" = "t")) |> 
          tidyr::fill(up, down, pcx_replica)
      }
      
      trade.graph <- trades |> 
        dplyr::select(time, Long.in, Short.in, Long.out, Short.out) 
      
      table <-  data |> 
        dplyr::left_join(trade.graph, by = c("dates" = "time")) |> 
        dplyr::select(-index) |> 
        dplyr::select(Date = dates, Index = index.strategy, PCx_replica = pcx_replica,
                      Limit_down = down, Limit_up = up, dplyr::everything())
      
      styleT <- createStyle(numFmt = "#,##0.00")
      wb <- createWorkbook()
      addWorksheet(wb, sheetName = glue::glue("{input$index.strategy}"))
      writeDataTable(wb, sheet = glue::glue("{input$index.strategy}"), 
                     table,
                     withFilter = openxlsx_getOp("withFilter", FALSE),
                     tableStyle = "TableStyleMedium9") 
      addStyle(wb, glue::glue("{input$index.strategy}"), style = styleT, 
               cols = c(2:9), rows = 2:(nrow(table) + 1), gridExpand = TRUE)
      saveWorkbook(wb, file, overwrite = TRUE)
    })
  
  output.result <- reactiveVal()
  
  observeEvent(input$final_report,{
    
    indices <- tail(colnames(prices()$prices), -1)
    start_date <- input$dates_prices[1]
    end_date <- input$dates_prices[2]
    dir <- glue::glue("/home/threshold/FrameworkData/Data/output{input$asset_manager}")
    
    show_modal_progress_line(
      value = 0,
      text = glue::glue("Analysing the {indices[1]} index \\
                            (1 of \\
                            {length(indices)})")
    )
    looping.array <- 1:(length(indices))
    
    for (i in looping.array) {
      update_modal_progress(
        value = i/length(indices),
        text = glue::glue("Analysing the {indices[i]} index \\
                            ({i} of \\
                            {length(indices)})")
      )
      index <- tail(colnames(prices()$prices), -1)[i]
      inputs <- GetDataFromParquet(K(), LAG(), index, start_date, end_date,
                                   dir)
      inputs <- c(inputs, list(type = type()[i]))
      
      result <- AnalysisForRecommendation(inputs, index)
      
      if (i == looping.array[1]) {
        final.table <- result$summary
        cumulative.score <- tibble::tibble(
          Date = index(result$cumulative.score),
          !!indices[i] := result$cumulative.score |> 
            as.numeric())
      } else {
        final.table <- dplyr::bind_rows(final.table, result$summary)
        if (length(as.numeric(result$cumulative.score)) < nrow(cumulative.score)) {
          diff <- nrow(cumulative.score) - length(as.numeric(result$cumulative.score))
          result$cumulative.score <- c(rep(0, diff), result$cumulative.score)
        }
        cumulative.score <- dplyr::bind_cols(cumulative.score, 
                                             tibble::tibble(
                                               !!indices[i] := result$cumulative.score |> 
                                                 as.numeric()))
      }
      
    }
    remove_modal_progress()
    
    result <- list(final.table = final.table, 
                   cumulative.score = cumulative.score)
    output.result(result)
    runjs("$('#downloadData')[0].click();")
    
  }
  
  )
  
  output$downloadData <- downloadHandler(
    filename = function() {
      "RelativeValueReport.xlsx"
    },
    content = function(file) {
      final.table <- output.result()$final.table
      cumulative.score <- output.result()$cumulative.score
      
      vols <- final.table$`Strategy Annualised Volatility`
      rets <- t(t(cumulative.score[,2:ncol(cumulative.score)]) * vols) |>
        tibble::as_tibble()
      
      rets.table <- cumulative.score[,1] |>
        dplyr::bind_cols(rets)
      
      summary <- tibble::tibble(
        cumulative.score[,1],
        `Average ZScore` = rowMeans(cumulative.score[,2:ncol(cumulative.score)]),
        `Average Return` = rowMeans(rets))
      
      recommendation.table <- final.table[, 1:6]
      backtesting.table <- final.table[, -(2:6)]
      styleT <- createStyle(numFmt = "#,##0.00")
      pct = createStyle(numFmt="0.00%")
      wb <- createWorkbook()
      addWorksheet(wb, sheetName = "Status Table")
      addWorksheet(wb, sheetName = "Backtesting Results")
      addWorksheet(wb, sheetName = "Zscores")
      writeDataTable(wb, sheet = "Backtesting Results", backtesting.table,
                     withFilter = openxlsx_getOp("withFilter", FALSE),
                     tableStyle = "TableStyleMedium9")
      writeData(wb, sheet = "Zscores", cumulative.score)
      writeDataTable(wb, sheet = "Status Table", recommendation.table,
                     withFilter = openxlsx_getOp("withFilter", FALSE),
                     tableStyle = "TableStyleMedium9")
      addStyle(wb, "Status Table", style = styleT,
               rows = 2:40, cols = c(2, 4:6), gridExpand = TRUE)
      addStyle(wb, "Backtesting Results", style = pct,
               rows = 2:40, cols = 3:13, gridExpand = TRUE)
      addWorksheet(wb, sheetName = "Average Results")
      writeDataTable(wb, sheet = "Average Results", summary,
                     withFilter = openxlsx_getOp("withFilter", FALSE),
                     tableStyle = "TableStyleMedium9")
      addStyle(wb, "Average Results", style = pct,
               rows = 2:(nrow(summary) + 1), cols = 3, gridExpand = TRUE)
      
      # zscore.graph <- ggplot(summary, aes(x = Date, y = `Average ZScore`)) +
      #   geom_line() +
      #   tidyquant::theme_tq() +
      #   labs(title = "Average ZScore of all the indices")
      # 
      # rets.graph <- ggplot(summary, aes(x = Date, y = `Average Return`)) +
      #   geom_line() +
      #   tidyquant::theme_tq() +
      #   scale_y_continuous(labels = scales::percent) +
      #   labs(title = "Average Return of all the indices")
      # 
      # print(zscore.graph)
      # insertPlot(wb, sheet = "Average Results", startRow = 1,
      #            startCol = 5)
      # print(rets.graph)
      # insertPlot(wb, sheet = "Average Results", startRow = 22, startCol = 5)
      saveWorkbook(wb, file, overwrite = TRUE)
    }
  )
  
  observeEvent(input$asset_manager, {
    
    if (!grepl("Indices summary",input$main_panel)) {
      nav_show("main_panel", target = "homepage")
    }
  })
  
  # Parquet Reactives -------------------------------------------------------
  
  relative_value <- reactive({
    req(K(), input$asset_manager, input$dates_prices, input$index.strategy)
    
    
    FrameworkIndices <- FrameworkIndices()
    
    if (!is.null(input$index.strategy)) {
      index <- input$index.strategy
      if (index %in% FrameworkIndices$Index) {
      } else {
        index <- FrameworkIndices$Index[1]
      } 
    } else {
      index <- FrameworkIndices$Index[1]
    }
    
    LAG <- LAG()
    
    start_date <- input$dates_prices[1]
    end_date <- input$dates_prices[2]
    dir <- glue::glue("/home/threshold/FrameworkData/Data/output{input$asset_manager}")
    
    GetDataFromParquet(K(), LAG, index, start_date, end_date, dir)
  })
  
  # Outputs -----------------------------------------------------------------
  
  
  # Conditional Panels Variables --------------------------------------------
  
  output$df_check <- reactive({
    req(returns())
    
    ifelse(is.list(returns()), TRUE, FALSE)
  })
  outputOptions(output, 'df_check', suspendWhenHidden = FALSE)
  
  output$economical_check <- reactive({
    FALSE
    # ifelse(!is.null(input.file.economical()), TRUE, FALSE)
  })
  outputOptions(output, 'economical_check', suspendWhenHidden = FALSE)
  
  output$hedging_check <- reactive({
    req(input$choose.frequency.hedging)
    ifelse(input$choose.frequency.hedging == "2", TRUE, FALSE)
  })
  outputOptions(output, 'hedging_check', suspendWhenHidden = FALSE)
  
  # Reactive UI -------------------------------------------------------------
  
  output$dropdown_lag_ui <- renderUI({
    
    lag.choices <- c("Daily" = 252, "Weekly" = 52, "Monthly" = 12, "Quarterly" = 4)
    selected <- 52
    
    pickerInput(inputId = "dropdown_lag",
                label = "Choose the frequency of the returns",
                selected = selected,
                choices = lag.choices,
                multiple = FALSE,
                choicesOpt = list(
                  style = rep("color: black;", length(lag.choices))))
  })
  
  output$dates <- renderUI({
    req(prices())
    min.date <- as.Date(min(prices()$prices.cut$dates))
    max.date <- as.Date(max(prices()$prices.cut$dates))
    
    dateRangeInput(
      inputId = "training_dates",
      label = "Training Interval",
      start = min.date,
      end = as.Date(min.date + 0.8*(max.date - min.date)),
      max = max.date,
      min = min.date,
      format = "yyyy-mm"
    )
  })
  outputOptions(output, 'dates', suspendWhenHidden = FALSE)
  
  output$dates_hedging <- renderUI({
    req(prices())
    
    min.date <- as.Date(min(prices()$prices.cut$dates))
    max.date <- as.Date(max(prices()$prices.cut$dates))
    
    dateRangeInput(
      inputId = "training_dates_hedging",
      label = "Training Interval",
      start = min.date,
      end = as.Date(min.date + 0.8*(max.date - min.date)),
      max = max.date,
      min = min.date,
      format = "yyyy-mm"
    )
  })
  outputOptions(output, 'dates_hedging', suspendWhenHidden = FALSE)
  
  
  output$return.analysis <- renderUI({
    req(portfolios())
    card(
      layout_columns(
        col_widths = c(8, 4),
        card(
          full_screen = TRUE,
          card_header("Cumulative performance and drawdowns of the self-hedging portfolios"),
          highcharter::highchartOutput("plot")
        ),
        card(
          full_screen = TRUE,
          card_header("Recap risk/return table for the self-hedging portfolios"),
          DT::DTOutput("table")
        )
      )
    )#,
    #   nav_panel(
    #     "Randomized Performance Analysis",
    #     fluidRow(
    #       column(6, numericInput(inputId = "simulations",
    #                              label = "Input the number of simulations",
    #                              value = 20,
    #                              min = 500,
    #                              max = 1000,
    #                              step = 20), align = "center"),
    #       column(6, actionButton("button", "Run The Simulations",
    #                              style = "margin-top: 24px;"),
    #              align = "center", inline = TRUE))),
    #   fluidRow(
    #     conditionalPanel(
    #       condition = "output.df_check == true",
    #       column(6,
    #              withSpinner(tableOutput("table.random")), align = "center",
    #              inline = TRUE),
    #       column(6,
    #              conditionalPanel("input.button",
    #                               pickerInput(inputId = "dropdown_statistic",
    #                                           label = "Choose the statistic",
    #                                           selected = "Annualised Returns",
    #                                           choices = stats.choice,
    #                                           multiple = FALSE,
    #                                           choicesOpt = list(
    #                                             style = rep("color: black;", length(stats.choice))))),
    #              withSpinner(plotOutput("graph.random")), align = "center"))
    #   )
    # )
  })
  
  output$control_centre_relative_value <- renderUI({
    if (input$asset_manager %in% "_new3") {
      fluidRow(
        column(4,align = "center", uiOutput('relative.value.asset')),
        column(4,align = "center", switchInput(
          inputId = "remove_trades",
          label = "Include the trades in the graphs", 
          labelWidth = "250px",
          width = "250px"
        )),
        column(4,align = "center", downloadButton("page_report", "Download the trading data",
                                                  style = "margin-top:28;"))
      )
    } else {
      fluidRow(
        column(4,align = "center", uiOutput('relative.value.asset')),
        column(4,align = "center", switchInput(
          inputId = "remove_trades",
          label = "Include the trades in the graphs", 
          labelWidth = "250px",
          width = "250px"
        )),
        column(4,align = "center", downloadButton("page_report", "Download the trading data",
                                                  style = "margin-top:28;"))
      )
    }
    
  })
  outputOptions(output, "control_centre_relative_value", suspendWhenHidden = FALSE)
  
  output$relative.value.asset <- renderUI({
    req(FrameworkIndices())
    
    test <- FrameworkIndices() |> 
      dplyr::select(`Asset Class`,`Long Name`, Index) |> 
      dplyr::group_nest(`Asset Class`) 
    
    indices <- purrr::map(test$data,
                          ~as.list(.x$Index) |> 
                            purrr::set_names(.x$`Long Name`)) |> 
      purrr::set_names(test$`Asset Class`)
    
    shinyWidgets::pickerInput(inputId = "index.strategy",
                              label = "Index for relative value trade",
                              choices = indices,
                              selected = FrameworkIndices()$Index[1],
                              multiple = FALSE,
                              width = "auto",
                              options = list(`live-search` = TRUE))
  })
  outputOptions(output, "relative.value.asset", suspendWhenHidden = FALSE, priority = 1)
  
  
  output$hedging.asset <- renderUI({
    req(hedging.assets())
    
    test <- FrameworkIndices() |> 
      dplyr::select(`Asset Class`,`Long Name`, Index) |> 
      dplyr::group_nest(`Asset Class`) 
    
    indices <- purrr::map(test$data,
                          ~as.list(.x$Index) |> 
                            purrr::set_names(.x$`Long Name`)) |> 
      purrr::set_names(test$`Asset Class`)
    
    shinyWidgets::pickerInput(inputId = "index.hedging", 
                              label = "Index to be hedged",
                              choices = indices,
                              width = "auto",
                              multiple = FALSE)
  })
  outputOptions(output, "hedging.asset", suspendWhenHidden = FALSE)
  
  output$select.portfolios <- renderUI({
    req(portfolios())
    
    selected.portfolios <- purrr::imap(portfolios(), assetTags)
    
    card(
      card_header("Self Hedging Portfolio Components"),
      card_body(
        class = "align-items-center",
        selected.portfolios)
    )
  })
  
  output$rolling.window.ui <- renderUI({
    
    req(input$cut.dates)
    
    if (input$cut.dates %in% "2") {
      choices <- c("1 Year" = 1, "1 Year and a Half" = 1.5)
    } else {
      if (input$cut.dates %in% "3") {
        choices <- c("1 Year" = 1, "1 Year and a Half" = 1.5,
                     "2 Years" = 2)
      } else {
        choices <- c("1 Year" = 1, "1 Year and a Half" = 1.5,
                     "2 Years" = 2, "3 Years" = 3)
      }
    }
    
    selected <- "1 Year"
    
    pickerInput(inputId = "rolling.window",
                label = "Choose the width of the rolling window",
                selected = selected,
                choices = choices)
  })
  outputOptions(output, "rolling.window.ui", suspendWhenHidden = FALSE)
  
  output$date.selection <- renderUI({
    
    prices <- arrow::open_dataset(glue::glue("/home/threshold/FrameworkData/Data/input{input$asset_manager}")) |> 
      dplyr::collect()
    
    min.date <- as.Date(min(prices$date))
    max.date <- as.Date(max(prices$date))
    
    start.date <- max.date - lubridate::years(1)
    
    dateRangeInput("dates_prices", "Select the range of analysis:",
                   format = "yyyy-mm-dd",
                   start = start.date,
                   end = max.date,
                   min = min.date,
                   max = max.date)
  })
  
  # Outputs -----------------------------------------------------------------
  
  
  # 1st pane ----------------------------------------------------------------
  
  output$landing_table <- DT::renderDT({
    req(lending_table_dt())
    lending_table_dt()
  })
  
  output$index_and_eigendata <- highcharter::renderHighchart({
    req(K(), LAG(), input$landing_table_rows_selected, landing_table())
    index <- landing_table()$Ticker[input$landing_table_rows_selected]
    start_date <- input$dates_prices[1]
    end_date <- input$dates_prices[2]
    delta_t <-end_date - start_date
    
    dir <- glue::glue("/home/threshold/FrameworkData/Data/output{input$asset_manager}")
    
    graph_index_and_eigendata(K(), LAG(), index, start_date, end_date, dir)
  })
  
  # 2nd pane ----------------------------------------------------------------
  
  output$plot <- highcharter::renderHighchart({
    req(portfolio.returns())
    
    if (input$main_panel %in% "Self-hedging Portfolios") {
      performance_graphs(portfolio.returns(), geometric = TRUE, input$main_panel)
    }
  })
  
  output$table <- DT::renderDT({
    DT::datatable(stats(),
                  options = list(dom = "t",
                                 columnDefs = list(list(className = 'dt-center', 
                                                        targets = "_all")))) |> 
      DT::formatPercentage(2:4, digits = 3)
    
  })
  
  output$graph.random <- renderPlot({
    GraphDistributions(simulation.results()$stats, 
                       input$dropdown_statistic)
  })
  
  output$table.random <- renderTable({
    simulation.results()$stats.table
  })
  
  # 3rd pane ----------------------------------------------------------------
  
  output$table.relative.value <- DT::renderDT({
    DT::datatable(stats.relative.trade(), 
                  options = list(dom = "t",
                                 columnDefs = list(list(className = 'dt-center', 
                                                        targets = "_all")))) |> 
      DT::formatPercentage(2:3, digits = 3)
    
  })
  
  output$graph_distance_TRUE <- highcharter::renderHighchart({
    req(relative_value())
    
    start_date <- input$dates_prices[1]
    end_date <- input$dates_prices[2]
    
    trades.data <- list(trades.graph = relative_value()$trades_graph,
                        trades = relative_value()$trades,
                        range = list(range.daily = relative_value()$replica.daily,
                                     range = relative_value()$range),
                        range.graph = list(range.daily = relative_value()$replica.daily_graph,
                                           range = relative_value()$range_graph))
    
    delta_t <-end_date - start_date
    
    TradeDistancePlot(trades.data, TRUE, delta_t, input$remove_trades) 
  })
  
  output$graph_distance_FALSE <- highcharter::renderHighchart({
    req(relative_value())
    
    start_date <- input$dates_prices[1]
    end_date <- input$dates_prices[2]
    
    trades.data <- list(trades.graph = relative_value()$trades_graph,
                        trades = relative_value()$trades,
                        range = list(range.daily = relative_value()$replica.daily,
                                     range = relative_value()$range),
                        range.graph = list(range.daily = relative_value()$replica.daily_graph,
                                           range = relative_value()$range_graph))
    
    delta_t <-end_date - start_date
    
    TradeDistancePlot(trades.data, FALSE, delta_t, input$remove_trades) 
  })
  
  output$graph_pnl_relative <- highcharter::renderHighchart({
    req(relative_value())
    
    if (input$main_panel %in% "Relative Value") {
      performance_graphs(relative_value()$performance, geometric = type_index(),
                         input$main_panel)
    }
    
  })
  
  output$distribution_returns <- highcharter::renderHighchart({
    req(statistics())
    PlotDistribution(statistics())
  })
  
  output$betting_results <- highcharter::renderHighchart({
    req(statistics())
    
    PlotBettingResults(statistics())
  })
  
  output$duration_results <- highcharter::renderHighchart({
    req(statistics())
    PlotDuration(statistics())
  })
  
  output$model.value <- renderText({
    req(relative_value())
    
    round(tail(relative_value()$range$pcx_replica,1)*100,0)/100 
  })
  
  output$status.trade <- renderText({
    req(relative_value())
    
    tail(relative_value()$trades$trade,1)
  })
  
  output$zscore <- renderText({
    req(relative_value())
    
    value <- relative_value()$replica.daily_graph$index.strategy |> 
      base::scale(center = FALSE) |> 
      tail(1)
    
    round(value*1000,0)/1000
  })
  
  output$bet.won <- renderText({
    req(statistics())
    vec <- statistics() |> 
      dplyr::pull(bet.result)
    
    paste0(as.character(round(sum(grepl("WON", vec))/length(vec)*100, 2)),"%")
    
  })
  
  output$kelly_criteria <- renderText({
    req(statistics())
    
    KellyCriteria(statistics())
  })
  
  # 4th pane ----------------------------------------------------------------
  
  pairs_trading <- reactive({
    tictoc::tic()
    load(glue::glue("/home/threshold/FrameworkData/Data/IndexMapping{input$asset_manager}.RData"))
    
    pairs_trade <- arrow::open_dataset(glue::glue("/home/threshold/FrameworkData/Data/\\
                                                 output_distance{input$asset_manager}/K={K()}")) |>
      dplyr::collect() |> 
      calculate_pairs_trading(FrameworkIndices,
                              input$dates_prices[1],
                              input$dates_prices[2],
                              input$confidence_switch,
                              input$tradable_switch,
                              input$tails_cutoff)
    tictoc::toc()
    return(pairs_trade)
  })
  
  pairs_performance <- reactive({
    req(pairs_trading())
    performance <- pairs_trading() |> 
      analyse_strategy("/home/threshold/FrameworkData",
                       input$asset_manager,
                       K())
    return(performance)
  })
  
  statistics_pairs <- reactive({
    req(pairs_trading(), pairs_performance())
    calculate_stats(pairs_trading(), pairs_performance())
  })
  
  output$current_pair <- reactive({
    
    req(pairs_trading())
    last <- tail(pairs_trading(), 1)
    pair <- glue::glue("Long: {last$Long} - Short: {last$Short}")
    return(pair)
      
  })
  
  output$pairs_trading_graph <- highcharter::renderHighchart({
      req(pairs_trading())
      pairs_trading() |> 
        plot_pairs()
      })
  
  
  
  output$pairs_trading_heatmap <- highcharter::renderHighchart({
      plot_heatmap(pairs_trading())
      })
  
  output$pairs_trading_performance <- highcharter::renderHighchart({
    req(pairs_performance())
    pairs_performance() |> 
      purrr::pluck("r.strategy") |> 
      performance_graphs(geometric = TRUE, input$main_panel)
  })
  
  output$table_pairs <- DT::renderDT({
    req(pairs_performance())
    
    return <- PerformanceAnalytics::Return.annualized(pairs_performance()$r.strategy,
                                                      geometric = TRUE)
    
    sd <- PerformanceAnalytics::sd.annualized(pairs_performance()$r.strategy,
                                              scale = 252)
    
    sharpe <- PerformanceAnalytics::SharpeRatio.annualized(pairs_performance()$r.strategy,
                                                           scale = 252, 
                                                           geometric = TRUE)
    
    drawdown <- PerformanceAnalytics::maxDrawdown(pairs_performance()$r.strategy,
                                                  geometric = TRUE)
    
    var <- PerformanceAnalytics::VaR(pairs_performance()$r.strategy)
    
    table <- rbind(return, sd, sharpe, drawdown, var) |> 
      as.data.frame() |> 
      tibble::rownames_to_column() |> 
      dplyr::rename_all(~c("Metric", "Pairs Trading Strategy")) |> 
      dplyr::mutate(Metric = c("Annualised Return", "Annualised Std Dev",
                               "Annualised Sharpe (Rf=0%)", 
                               "Maximum Drawdown",
                               "Historical VaR (95%)"))

    DT::datatable(table, 
                  options = list(dom = "t",
                                 columnDefs = list(list(className = 'dt-center', 
                                                        targets = "_all")))) |> 
      DT::formatPercentage(2, digits = 3)
    
  })
  
  output$distribution_returns_pairs <- highcharter::renderHighchart({
    req(statistics_pairs())
    PlotDistribution(statistics_pairs())
  })
  
  output$betting_results_pairs <- highcharter::renderHighchart({
    req(statistics_pairs())
    
    PlotBettingResults(statistics_pairs()$bets)
  })
  
  output$duration_results_pairs <- highcharter::renderHighchart({
    req(statistics_pairs())
    PlotDuration(statistics_pairs()$duration)
  })
  
  output$pairs_report <- downloadHandler(
    filename <- function(index) {
      glue::glue("PairsTradingReportReport-{Sys.Date()}.xlsx")
    },
    content <- function(file) {
      params <- data.frame(
        "Parameter" = c("Exclude Low Confidence Indices", 
                        "Exclude non tradable indices",
                        "Trade Stability Parameter"),
        "Value" = c(as.character(input$confidence_switch), 
                    as.character(input$tradable_switch), 
                    as.character(input$tails_cutoff))
      )
      
      returns <- pairs_performance() |> 
        purrr::pluck("r.strategy") 

      performance_returns <- data.frame(
        "Dates" = index(returns),
        "Daily returns" = as.numeric(returns),
        "Cumulative returns" = calculate_cumulative_returns(returns, 
                                                            wealth.index = FALSE,
                                                            geometric = TRUE) |> 
          as.numeric(),
        "Drawdowns" = Drawdowns(returns) |> 
          as.numeric()
      )

      styleT <- createStyle(numFmt = "#,##0.00")
      wb <- createWorkbook()
      addWorksheet(wb, sheetName = "Run Parameters")
      writeDataTable(wb, sheet = "Run Parameters", 
                     params,
                     withFilter = openxlsx_getOp("withFilter", FALSE),
                     tableStyle = "TableStyleMedium9") 
      addWorksheet(wb, sheetName = "Pairs Trading Through Time")
      writeDataTable(wb, sheet = "Pairs Trading Through Time", 
                     pairs_trading(),
                     withFilter = openxlsx_getOp("withFilter", FALSE),
                     tableStyle = "TableStyleMedium9")
      addWorksheet(wb, sheetName = "Heatmap")
      writeDataTable(wb, sheet = "Heatmap", 
                     dplyr::count(pairs_trading(), Long,Short) |> 
                       dplyr::rename("Days" = n),
                     tableStyle = "TableStyleMedium9")
      addWorksheet(wb, sheetName = "Performance and Drawdown")
      writeDataTable(wb, sheet = "Performance and Drawdown", 
                     performance_returns,
                     withFilter = openxlsx_getOp("withFilter", FALSE),
                     tableStyle = "TableStyleMedium9")
      addStyle(wb, "Performance and Drawdown", style = styleT,
               cols = c(2:4), rows = 2:(nrow(performance_returns) + 1), gridExpand = TRUE)
      saveWorkbook(wb, file, overwrite = TRUE)
    })
  # 5th pane ----------------------------------------------------------------
  
  output$hedging <- renderText(hedging())
  
  output$hedging_weights <- highcharter::renderHighchart({
    req(hedging())
    hedging()$graph.weight
  })
  
  output$hedging_training <- highcharter::renderHighchart({  
    req(hedging())
    
    performance_graphs(hedging()$graphs.training[,1:2], geometric = type_index(),
                       input$main_panel)
    # charts.PerformanceSummary(hedging()$graphs.training[,1:2],
    #                           main = "Performance over Training Range",
    #                           legend.loc = "bottomright",
    #                           geometric = FALSE)
    # tictoc::toc()
  })
  
  output$hedging_testing <- highcharter::renderHighchart({
    req(hedging())
    performance_graphs(hedging()$graphs.testing[,1:2], geometric = type_index(),
                       input$main_panel)
    # charts.PerformanceSummary(hedging()$graphs.testing[,1:2], 
    #                           main = "Performance over Testing Range",
    #                           legend.loc = "bottomright",
    #                           geometric = FALSE) 
  })
  
  output$risk.table <- DT::renderDT({
    req(hedging())
    DT::datatable(hedging()$table.results, 
                  options = list(dom = "t",
                                 columnDefs = list(list(className = 'dt-center', 
                                                        targets = "_all")))) |> 
      DT::formatPercentage(2:4, digits = 3)
  })
  
  output$hedge.efficiency <- renderText({
    req(hedging())
    glue::glue("The hedge efficiency of the selected portfolio in the training set is \\
               {round(hedging()$hedge.efficiency.training,2)*100}% while it is \\
               {round(hedging()$hedge.efficiency.testing,2)*100}% in the testing set")
  })
  
  
  
  
})
