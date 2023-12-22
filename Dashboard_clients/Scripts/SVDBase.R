
RunSVD <- function(training.data, k, rotation.flag = FALSE) {
  m <- ncol(training.data)
  n <- nrow(training.data)
  
  center <- unlist(lapply(training.data, mean))
  sigma <- unlist(lapply(training.data, sd))
  training.data <- base::scale(training.data, TRUE, TRUE)
  svd.decompositions <- svd(training.data, nu = k, nv = k)
  
  U <- svd.decompositions$u
  V <- svd.decompositions$v
  S <- diag(svd.decompositions$d)
  
  L.lambda <- (V %*% S[1:k,1:k])

  if (rotation.flag & k > 1) {
    rotation <- varimax(L.lambda)
    
    R <- rotation$rotmat
    U.rot <- U.lambda %*% R
    L.rot <- L.lambda %*% R
  } else {
    U.rot <- U
    L.rot <- L.lambda
  }
  
  rownames(L.rot) <- colnames(training.data)
  colnames(L.rot) <- glue::glue("RC{1:k}")
  
  reconstructed.indices <- descale(U.rot, L.rot, sigma, center) |> 
    as.data.frame()
  
  colnames(reconstructed.indices) <- glue::glue("{colnames(training.data)}_approx")
  
  return(list(U = U.rot, L = L.rot, S = S, 
              reconstructed.indices = reconstructed.indices,
              sigma = sigma, center = center, V = V))
}


descale <- function(U, L, scale, center) {
  mat <- t(t(U %*% t(L)) * scale)
  mat <- t(t(mat) + center)
  return(mat)
}


CalcualateMedianUvector <- function(yearly.data, interval) {
  yearly.data |> 
    dplyr::summarise(median = rep(median(value), interval),
                     up = rep(mean(value) + 0.75 * sd(value), interval),
                     down = rep(mean(value) - 0.75 * sd(value), interval)) |> 
    dplyr::mutate(time.lag = 1:interval)
}

ConsolidateAndTranslate <- function(data, U, svd.decomposition) {
  
  forward.U <- data |> 
    tidyr::pivot_wider(names_from = PC,
                       values_from = values) |> 
    dplyr::select(-time.lag)
  
  projeted.U <- dplyr::bind_rows(U, 
                                 forward.U) |> 
    as.matrix()
  
  forward.indices <- descale(projeted.U, 
                             svd.decomposition$L, 
                             svd.decomposition$sigma, 
                             svd.decomposition$center) |> 
    tibble::as_tibble()
  
  colnames(forward.indices) <- colnames(svd.decomposition$reconstructed.indices)
  
  return(forward.indices)
}

ProjectReturns <- function(svd.decomposition, interval) {
  
  U <- svd.decomposition[["U"]] |> 
    tibble::as_tibble()
  
  U |> 
    dplyr::mutate(obs.n = 1:nrow(U)) |> 
    tidyr::pivot_longer(cols = -obs.n,
                        names_to = "PC",
                        values_to = "value")  |> 
    dplyr::group_nest(PC) |> 
    dplyr::mutate(median.u = purrr::map(data, 
                                        ~CalcualateMedianUvector(.x, 
                                                                 interval))) |> 
    dplyr::select(-data) |> 
    tidyr::unnest(cols = c(median.u))  |> 
    tidyr::pivot_longer(c(-PC, -time.lag), names_to = "type", 
                        values_to = "values") |> 
    dplyr::group_nest(type) |> 
    dplyr::mutate(proj.returns = purrr::map(data, 
                                            ~ConsolidateAndTranslate(.x, U, 
                                                                     svd.decomposition))) |>  
    dplyr::select(-data) 
  
  
}