SliceData <- function(index, data.set, assets.to.keep, short.flag) {

  dates <- data.set |> 
    dplyr::pull(dates)
  
  data.set <- dplyr::select(data.set, -dates)
  
  if (short.flag) {
    data.set.short <- (data.set*-1) |> 
      tibble::as_tibble() |> 
      dplyr::rename_all(~glue::glue("{colnames(data.set)}_short"))
    
    data.set <- data.set |> 
      dplyr::bind_cols(data.set.short)
    
    assets.to.keep <- c(assets.to.keep, glue::glue("{assets.to.keep}_short"))
  }
  
  not.na.index.positions <- na.omit(data.set[[index]]) |> 
    length()
  
  data.set <- tail(data.set, not.na.index.positions)
  
  test.valid.data <- purrr::map_lgl(data.set,
                                    ~(any(is.na(.x)) | all(.x == 0)))
  
  data.set <- data.set[,!test.valid.data]
  
  dates <- tail(dates, not.na.index.positions)
  
  xts.data.set <- xts::xts(data.set, 
                           order.by = dates)
  
  target <- xts.data.set[, index]

  universe.indices <- intersect(setdiff(colnames(data.set),
                              c("dates", index)), assets.to.keep)
  
  universe <- xts.data.set[, universe.indices]
  
  return(list(target = target,
              universe = universe))
}

CalculateHedge <- function(xts.training.set, 
                           xts.testing.set, method = "ete", max.weight = 0.5, 
                           lambda = 1e-7) {

  w <- spIndexTrack(xts.training.set$universe,
                    xts.training.set$target,
                    lambda = lambda,
                    u = max.weight,
                    measure = method)
  
  r.training <- Return.portfolio(xts.training.set$universe,
                        weights = w,
                        rebalance_on = "days")
  
  w_fixed <- c()
  
  for (index in names(xts.testing.set$universe)) {
    if (index %in% names(w)) {
      w_fixed <- c(w_fixed, w[[index]])
    } else {
      w_fixed <- c(w_fixed,0)
    }
  }
  
  names(w_fixed) <- names(xts.testing.set$universe)
  
  r.testing <- Return.portfolio(xts.testing.set$universe,
                                 weights = w_fixed,
                                 rebalance_on = "days")
  
  return(list(r.training = r.training, r.testing = r.testing, w = w_fixed))
}

GraphWeights <- function(weights) {
  
  data.frame(indices = names(weights),
             weights = weights) |> 
    dplyr::filter(weights > 0) |> 
    ggplot(aes(x = weights, y = indices)) + 
    geom_col()
}

GraphHedgingReturns <- function(target, hedging.portfolio.returns) {
  returns_testing <- cbind(target,
                           hedging.portfolio.returns,
                           target - hedging.portfolio.returns)
  
  colnames(returns_testing)[2] <- "Hedging Portfolio"
  colnames(returns_testing)[3] <- "Net portfolio"
  
  return(returns_testing)
}

