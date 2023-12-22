`%!in%` <- Negate(`%in%`)

calculate_trading_matrix <- function(prices, asset_manager, FrameworkIndices, K, LAG) {
  N_tail <- 1
  
  futures_info <- FrameworkIndices |> 
    dplyr::select(index = Index, `Long Name`, tick_size = Tick.Size, 
                  minimum_increment = Minimum.Increment)
  
  dir_old_trades <- glue::glue("/home/threshold/FrameworkData/Data/trade_history{asset_manager}")
  if (!dir.exists(dir_old_trades)) {
    current_portfolio <- NULL
    current_portfolio_indices <- NULL
  } else {
    current_portfolio <- dir_old_trades |> 
      arrow::open_dataset() |> 
      dplyr::collect() |> 
      dplyr::group_nest(Dates) |> 
      dplyr::arrange(Dates) |> 
      tail(N_tail) |> 
      head(1) |> 
      tidyr::unnest(data) |> 
      dplyr::select(-Dates)
    
    current_portfolio_indices <- current_portfolio |> 
      dplyr::pull(index)
  }
  
  dir <- glue::glue("/home/threshold/FrameworkData/Data/output_distance{asset_manager}")
  dir_output <- glue::glue("/home/threshold/FrameworkData/Data/output{asset_manager}")
  
  tradable_indices_info <- FrameworkIndices |> 
    dplyr::filter(Type %in% "Tradable") |> 
    dplyr::select(Index) # Add the contract size
  
  indices_table <- glue::glue("{dir}/K={K}") |> 
    arrow::open_dataset() |>
    dplyr::select(-YearMonth) |> 
    dplyr::collect() |> 
    dplyr::group_nest(Dates) |> 
    dplyr::arrange(Dates) |> 
    tail(N_tail) |> 
    head(1) |> 
    tidyr::unnest(data) |> 
    dplyr::filter(Index %in% tradable_indices_info$Index) |> 
    dplyr::select(index = Index, level = `Current Index Value`,
                  z_score = `Distance from Estimated Fair Value (z-score)`,
                  info_ratio = `Information Index`)
  
  indices <- indices_table |> 
    dplyr::pull(index)
  
  signals <- tibble::tibble()
  
  for (index in indices) {
    signal <- glue::glue("{dir_output}/{index}/K={K}/LAG={LAG}/trades") |> 
      arrow::open_dataset() |> 
      dplyr::collect() |> 
      tail(N_tail) |> 
      head(1) |> 
      dplyr::summarise(signal = dplyr::case_match(trade,
                                                  "Above the corridor" ~ "Short",
                                                  "Below the corridor" ~ "Long",
                                                  .default = "Neutral"
      )) |>
      dplyr::mutate(index = index) |> 
      dplyr::select(index, signal)
    
    signal <- signal |> 
      dplyr::left_join(indices_table, by = "index")
    
    signals <- signals |> 
      dplyr::bind_rows(signal)
  }
  
  if (!is.null(current_portfolio)) {
    history_current_portfolio <- prices$prices.cut |> 
      dplyr::select(dplyr::contains(current_portfolio_indices)) 
    
    current_portfolio <- history_current_portfolio |> 
      dplyr::mutate_all(.funs = ~abs(.x - dplyr::lag(.x))) |> 
      dplyr::slice(-1) |> 
      purrr::map_df(mean)|> 
      tidyr::pivot_longer(cols = dplyr::everything(), 
                          names_to = "index", values_to = "mean_tick") |> 
      dplyr::left_join(futures_info, by = "index") |> 
      dplyr::mutate(average_value_exchanged = mean_tick/minimum_increment*tick_size) |> 
      dplyr::filter(!grepl("(ETF|UCITS)", `Long Name`)) |> 
      dplyr::select(index, average_value_exchanged, `Long Name`) |> 
      dplyr::left_join(current_portfolio, by = "index") |> 
      dplyr::left_join(signals, by = "index") |> 
      dplyr::mutate(signal = dplyr::if_else(signal == Direction, 
                                            "Keep", signal)) |> 
      dplyr::mutate(info_ratio = round(info_ratio, 2),
                    average_value_exchanged = round(average_value_exchanged, 2)) |> 
      dplyr::arrange(signal, dplyr::desc(info_ratio)) |> 
      dplyr::select(index, `Long Name`, dplyr::everything())
    
  } else {
    history_current_portfolio <- NULL
  }
  
  tradable_indices <- signals |> 
    dplyr::filter(signal %!in% "Neutral") |> 
    dplyr::filter(index %!in% current_portfolio_indices)
  
  
  
  history_assets_to_trade <- prices$prices.cut |> 
    dplyr::select(dplyr::contains(tradable_indices$index)) 
  
  trade_matrix <- history_assets_to_trade |> 
    dplyr::mutate_all(.funs = ~abs(.x - dplyr::lag(.x))) |> 
    dplyr::slice(-1) |> 
    purrr::map_df(mean) |> 
    tidyr::pivot_longer(cols = dplyr::everything(), 
                        names_to = "index", values_to = "mean_tick") |> 
    dplyr::left_join(futures_info, by = "index") |> 
    dplyr::mutate(average_value_exchanged = mean_tick/minimum_increment*tick_size) |> 
    dplyr::filter(!grepl("(ETF|UCITS)", `Long Name`)) |> 
    dplyr::select(index, average_value_exchanged, `Long Name`) |> 
    dplyr::left_join(tradable_indices, by = "index") |> 
    dplyr::mutate(info_ratio = round(info_ratio, 2),
                  average_value_exchanged = round(average_value_exchanged, 2)) |> 
    dplyr::arrange(signal, dplyr::desc(info_ratio)) |> 
    dplyr::select(index, `Long Name`, dplyr::everything())
  
  
  return(list(trade_matrix = trade_matrix, 
              history_assets_to_trade = history_assets_to_trade,
              current_portfolio = current_portfolio,
              history_current_portfolio = history_current_portfolio))
}







