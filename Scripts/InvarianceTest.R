library(ggplot2)

EllipsoidGraph <- function(invariance.test, series.name) {
  invariance.test$data |> 
    ggplot(aes(x = X, y = Y)) + 
    geom_point(alpha = 0.5) + 
    geom_point(data = invariance.test$ellipsoid, aes(x = X, y = Y), colour = "red",
               size = 1) + 
    theme_minimal() + 
    labs(x = glue::glue("{series.name}"),
         y = glue::glue("{series.name} lagged")) +
    theme(text = element_text(size=10)) +
    coord_fixed(ratio = 1) 
}


InvarianceTest <- function(data, scale) {
  data <- data |> 
    purrr::discard(is.na)
  
  X <- data[1:(length(data) - 1)]
  Y <- data[2:(length(data))]
  
  # Ellipsoid Analysis
  data <- tibble::tibble(X = X, Y = Y)
  m <- colMeans(data)
  S <- cov(data)
  eig <- eigen(S)
  
  angle <- seq(0, 2*pi, by = pi/500)
  m.matrix <- matrix(rep(m, each = length(angle)), nrow = length(angle))
  
  ellipsoid <- tibble::tibble(X.ellips = sin(angle), Y.ellips = cos(angle)) |>
    as.matrix() |> 
    {\(angle) t((eig$vectors %*% diag(sqrt(eig$values))) %*% t(angle))}() |> 
    {\(centered_ellipse) m.matrix + scale * centered_ellipse}() |> 
    tibble::as_tibble()  |> 
    dplyr::rename("X" = V1, "Y" = V2)
  
  return(list(data = data, ellipsoid = ellipsoid))
}


TestSquareRootRule <- function(data, series.name, traded.asset, LAG) {
  
  LAG <- c(1, LAG)
  
  if(traded.asset) {
    invariant <- log(data/dplyr::lag(data, 1)) |> na.omit()
    projection <- purrr::map(LAG, ~RcppRoll::roll_sum(invariant, n = .x, 
                                                      by = .x) - 1)
  } else {
    invariant <- diff(data)/100
    projection <- purrr::map(LAG, ~RcppRoll::roll_sum(invariant, n = .x, 
                                                      by = .x))
    
  }
  
  test.results <- projection |> 
    purrr::map(InvarianceTest, 1.5) 
  
  data <- test.results[[1]] |> 
    purrr::pluck("data")
  
  ggplot(data, aes(x = X, y = Y)) + 
    geom_point(alpha = 0.5) + 
    geom_point(data = test.results[[1]]$ellipsoid, aes(x = X, y = Y), 
               colour = "red", size = 1) + 
    geom_point(data = test.results[[2]]$ellipsoid, aes(x = X, y = Y), 
               colour = "blue", size = 1) + 
    geom_point(data = test.results[[3]]$ellipsoid, aes(x = X, y = Y), 
               colour = "purple", size = 1) + 
    theme_minimal() + 
    labs(x = glue::glue("{series.name}"),
         y = glue::glue("{series.name} lagged")) +
    theme(text = element_text(size=7)) +
    coord_fixed(ratio = 1) +
    scale_x_continuous(labels = scales::percent) + 
    scale_y_continuous(labels = scales::percent) 
}

CalculateInvariant <- function(index, data, traded.asset, LAG = 1){
  if (any(is.na(data))) {
    not_na <- which(!is.na(data))
    count_na_start <- not_na[1] - 1
    count_na_end <- length(data) - tail(not_na, 1)
    data <- data[not_na]
  } else {
    count_na_start <- 0
    count_na_end <- 0
  }
  
  if(traded.asset) {
    # if (all(data > 0)) {
    # invariant <- log(data/dplyr::lag(data, 1)) |> na.omit()
    # projection <- RcppRoll::roll_sum(invariant, n = LAG, by = LAG)
    invariant <- (data/dplyr::lag(data, 1) - 1) |> na.omit()
    projection <- RcppRoll::roll_prod(1 + invariant, n = LAG, by = LAG) - 1
    # } else {
    #   invariant <- (data/dplyr::lag(data, 1) - 1) |> na.omit()
    #   projection <- RcppRoll::roll_prod(1 + invariant, n = LAG, by = LAG) - 1
    # }
  } else {
    invariant <- ((data - dplyr::lag(data, 1))/100) |> na.omit()
    projection <- RcppRoll::roll_sum(invariant, n = LAG, by = LAG)
  }
  
  projection <- c(rep(NA_real_, count_na_start), projection, rep(NA_real_, count_na_end))
  
  res <- data.frame(index = projection)
  colnames(res) <- index
  
  return(res)
}
