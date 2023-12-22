ExtractFeatures <- function(L.rot, k, n, invariant.matrix, threshold,
                            items.to.remove) {
  variable <- glue::glue("RC{n}")
  
  indices.names <- L.rot |> 
    tibble::as_tibble() |> 
    dplyr::rename_all(~glue::glue("RC{1:k}")) |> 
    dplyr::mutate(indices = colnames(invariant.matrix)) |> 
    dplyr::select(indices, variable) 
  
  if (items.to.remove != 0) {
    indices.names <- indices.names |> 
      head(-items.to.remove)
  }
  
  indices.names <- indices.names |>  
    dplyr::filter(abs(!!rlang::sym(variable)) > threshold) 
  
  if (nrow(indices.names) > 0) {
    indices.names <- indices.names |> 
      dplyr::mutate(long.short = sign(!!rlang::sym(variable))) |> 
      dplyr::group_nest(long.short) |> 
      dplyr::arrange(desc(long.short)) |> 
      dplyr::pull(data) |> 
      purrr::map(~purrr::pluck(.x, "indices")) 
    
    if(length(indices.names) == 1)  {
      names(indices.names) <- "Long Indices"
    } else {
      names(indices.names) <- c("Long Indices", "Short Indices")
    }
    
    indices.position <- indices.names |> 
      purrr::map(~which(colnames(invariant.matrix) %in% .x))
    
    ret <- list(names = indices.names, positions = indices.position) |> 
      purrr::transpose()
    
  } else {
    ret <- list(`Long Indices` = c(), `Short Indices` = c())
  }
  
  
  return(ret)
}

ExtractFeaturesFolding <- function(L.rot, N, invariant.matrix, threhsold) {
  assets <- purrr::map(N, ~ExtractFeatures(L.rot, max(N), .x, invariant.matrix,
                                           threshold)) |> 
    purrr::set_names(glue::glue("RC{1:max(N)}"))
  return(assets)
}

PopulateWeightsVector <- function(weight, assets, total.n.assets) {
  
  w <- rep(0, total.n.assets)
  w[assets] <- weight
  return(w)
}

CreatePortfolio <- function(asset.sets, max.order, total.n.assets) {
  long.check <- asset.sets |> 
    purrr::map(purrr::pluck("Long Indices")) |> 
    purrr::map_lgl(is.null) |> 
    {\(x) x[1:max.order]}()
  
  short.check <- asset.sets |> 
    purrr::map(purrr::pluck("Long Indices")) |> 
    purrr::map_lgl(is.null)|> 
    {\(x) x[1:max.order]}()
  
  items <- 1:max.order |> 
    {\(x) x[!(long.check & short.check)]}()
  
  assets <- asset.sets[items] |> 
    purrr::map(purrr::transpose) |> 
    purrr::map(purrr::pluck("positions")) |> 
    purrr::map(purrr::flatten_dbl)
  
  weights <- assets |> 
    purrr::map(~(1/length(.x))/length(items)) |>
    purrr::map(~ifelse(is.infinite(.x), 0, .x)) |> 
    purrr::map2_dfr(assets, ~PopulateWeightsVector(.x, .y, total.n.assets)) |> 
    purrr::reduce(`+`)
  
  return(weights)
  
}

CreatePortfolioFolding <- function(asset.sets, recurring = TRUE) {
  if(recurring) {
    weights.array <- purrr::map(1:length(asset.sets), 
                                ~CreatePortfolio(asset.sets, .x)) |> 
      purrr::set_names(glue::glue("RC{1:length(asset.sets)}"))
  } else {
    weights.array <- CreatePortfolio(asset.sets, length(asset.sets)) |> 
      list() |> 
      purrr::set_names(glue::glue("RC{length(asset.sets)}"))
  }
  
  return(weights.array)
}

CreateCustomPortfolio <- function(bets, base.portfolio, new.assets) {
  for (bet in bets) {
    name <- stringr::str_extract(bet, "(?<=\\_)\\w+")
    direction <- stringr::str_extract(bet, "\\w+(?=\\_)") |> 
      {\(x) ifelse(grepl("long", x),
                   "Long Indices", "Short Indices")}()

    base.portfolio[[c(name, direction, "positions")]] <- 
      as.numeric(new.assets[[glue::glue("id_{bet}")]])
    
  }
  return(base.portfolio)
}


CalculatePortfolioReturnFolding <- function(weights.array = NULL, ret, portfolios, 
                                            dashboard = FALSE) {
 
  rr <- PerformanceAnalytics::Return.portfolio(ret, 
                                               weights = rep(1/ncol(ret),
                                                             ncol(ret)))
  
  if (dashboard) {
    rr.chosen <- PerformanceAnalytics::Return.portfolio(ret, portfolios$chosen.portfolio)
    rr.full <- PerformanceAnalytics::Return.portfolio(ret, portfolios$full.portfolio)
    
    ret.total <- cbind(rr, rr.chosen, rr.full)
    
    dimnames(ret.total)[[2]] <- c("Equal weights",  
                                  "Selected Portfolio", "Algo Portfolio")
  } else {
    port.ret <- purrr::map(weights.array, 
                           ~PerformanceAnalytics::Return.portfolio(ret, 
                                                                   weights = .x)) |> 
      purrr::reduce(cbind)
    
    ret.total <- cbind(rr, port.ret)
    
    dimnames(ret.total)[[2]] <- c("Equal weights", 
                                  glue::glue("RC{1:length(weights.array)}"))
    
  }
  
  return(ret.total)
}

ExtractInfo <- function(returns, funs) {
  purrr::map(returns, funs) |> 
    purrr::reduce(rbind) |> 
    tibble::as_tibble() 
}

GetTable <- function(returns) {
  param <- list(PerformanceAnalytics::Return.annualized, 
                PerformanceAnalytics::sd.annualized,
                PerformanceAnalytics::SharpeRatio.annualized,
                PerformanceAnalytics::CDD,
                PerformanceAnalytics::maxDrawdown)
  
  table <- purrr::map(param, ~ExtractInfo(returns, .x)) |> 
    purrr::set_names(c("Annualised Returns", "Annualised Volatility", "Annualised Sharpe Ratio",
                       "Conditional Drawdown", "Maximum Drawdown"))
  return(table)
}

PlotAssets <- function(training.data, dates, asset1, asset2) {
  library(ggplot2)
  training.data |> 
    dplyr::select(asset1, asset2) |> 
    dplyr::mutate(dates = as.Date(dates)) |> 
    tidyr::pivot_longer(-dates, names_to = "asset", values_to = "values") |> 
    ggplot(aes(x = dates, y = values, colour = asset)) +
    geom_line()
}

RandomAnalysis <- function(invariant.matrix, n.folds, freq, k, threshold,
                           portfolios, variable) {
  
  cv <- modelr::crossv_mc(invariant.matrix[,-1], test = 0.2, n = n.folds)
  
  frequency <- dplyr::case_when(
    freq == 1 ~ "month",
    freq == 3 ~ "quarter",
    freq == 4 ~ "year",
    TRUE ~ "month"
  )
  
  # trained.portfolios <- purrr::map(cv$train, ~RunSVD(invariant.matrix[sample(.x$idx),-1], 
  #                                                    as.numeric(k))) |> 
  #   furrr::future_map(~ExtractFeaturesFolding(.x$L, 1:k, invariant.matrix[,-1],
  #                                             threshold)) |> 
  #   purrr::map(CreatePortfolioFolding, FALSE) 
  # 
  testing.set <- purrr::map(cv$test,
                            ~xts::xts(invariant.matrix[.x$idx,-1],
                                      order.by = seq.Date(Sys.Date(), by = frequency,
                                                          length.out = length(.x$idx))))
  
  # tested.portfolios <- trained.portfolios |> 
  #   furrr::future_map2(testing.set, ~CalculatePortfolioReturnFolding(.x, .y, portfolios,
  #                                                                    TRUE))
  
  tested.portfolios <- testing.set |> 
    furrr::future_map(~CalculatePortfolioReturnFolding(ret = .x, portfolios = portfolios, 
                                                       dashboard = TRUE))
  
  stats <- GetTable(tested.portfolios) |> 
    purrr::transpose()
  
  stats.table <- stats |> 
    purrr::map_dfr(~tibble::as_tibble(.x) |> colMeans()) |> 
    dplyr::mutate(Portfolio = names(stats)) |> 
    dplyr::select(Portfolio, dplyr::everything())
  
  return(list(stats.table =stats.table,
              stats = stats))
  
}
