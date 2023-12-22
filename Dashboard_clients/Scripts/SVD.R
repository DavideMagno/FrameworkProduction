
RunSVD <- function(training.data, k, rotation.flag = FALSE) {

  m <- ncol(training.data)
  n <- nrow(training.data)
  
  svd.decompositions <- impute(training.data, ncp = k)
  
  U <- svd.decompositions$svd$U
  V <- svd.decompositions$svd$V
  S <- diag(svd.decompositions$svd$vs)
  center <- svd.decompositions$center
  sigma <- svd.decompositions$scale
  
  L.lambda <- (V %*% S[1:k,1:k])
  U.lambda <- U
  
  if (rotation.flag & k > 1) {
    rotation <- varimax(L.lambda)
    
    R <- rotation$rotmat
    U.rot <- U.lambda %*% R
    L.rot <- L.lambda %*% R
  } else {
    U.rot <- U.lambda
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

moy.p <- function(V, poids) {
  
  res <- sum(V * poids, na.rm = TRUE)/sum(poids[!is.na(V)])
}
ec <- function(V, poids) {
  res <- sd(V)
}

impute <- function(X, recursive = FALSE, ncp = 4, scale = TRUE, method = "em", 
                   row.w = NULL, coeff.ridge = 1, ...) {
  nrX <- nrow(X) 
  ncX <- ncol(X) 
  
  if (is.null(row.w)) 
    row.w = rep(1, nrow(X))/nrow(X)
  X <- as.matrix(X)
  ncp <- min(ncp, ncol(X), nrow(X) - 1)
  
  mean.p <- apply(X, 2, moy.p, row.w)
  Xhat <- t(t(X) - mean.p)
  if (scale) {
    et <- apply(Xhat, 2, ec, row.w)
    Xhat <- t(t(Xhat)/et)
  }
  
  svd.res <- FactoMineR::svd.triplet(Xhat, row.w = row.w, 
                                     ncp = ncp)
  
  sigma2 <- nrX * ncX/min(ncX, nrX - 1) * sum((svd.res$vs[-c(1:ncp)]^2)/((nrX - 
                                                                            1) * ncX - (nrX - 1) * ncp - ncX * ncp + ncp^2))
  sigma2 <- min(sigma2 * coeff.ridge, svd.res$vs[ncp + 
                                                   1]^2)
  if (method == "em") 
    sigma2 <- 0
  lambda.shrinked = (svd.res$vs[1:ncp]^2 - sigma2)/svd.res$vs[1:ncp]
  result <- list()
  if (recursive) {
    fittedXRecursive <- list()
    for (i in seq_len(ncp)) {
      fittedX <- tcrossprod(t(t(svd.res$U[, i, drop = FALSE] * 
                                  row.w) * lambda.shrinked[i]), svd.res$V[, i, 
                                                                          drop = FALSE])
      fittedX <- fittedX/row.w
      fittedXRecursive <- c(fittedXRecursive, list(fittedX))
    } 
    fittedX <- purrr::reduce(fittedXRecursive, `+`) 
    if (scale) 
      fittedX <- t(t(fittedX) * et)
    fittedX <- t(t(fittedX) + mean.p)
    fittedX <- tibble::as_tibble(fittedX)
    colnames(fittedX) <- colnames(X)
    result$fittedXRecursive <- fittedXRecursive
  } else {
    fittedX <- tcrossprod(t(t(svd.res$U[, 1:ncp, drop = FALSE] * 
                                row.w) * lambda.shrinked), svd.res$V[, 1:ncp, 
                                                                     drop = FALSE])
    fittedX <- fittedX/row.w
    if (scale) 
      fittedX <- t(t(fittedX) * et)
    fittedX <- t(t(fittedX) + mean.p)
    fittedX <- tibble::as_tibble(fittedX)
    colnames(fittedX) <- colnames(X)
  }
  result$fittedX <- fittedX
  result$svd <- svd.res
  result$center <- mean.p
  result$scale <- et
  
  return(result)
}



descale <- function(U, L, scale, center) {
  mat <- t(t(U %*% t(L)) * scale)
  mat <- t(t(mat) + center)
  return(mat)
}

UInversion <- function(target, L, scale, center) {
  n <- nrow(target)
  mat <- t(t(target) - center)
  mat <- t(t(mat)/scale) %*% MASS::ginv(t(L))/sqrt(n)
  return(mat)
}


scale <- function(X, scale, center) {
  mat <- t(t(X) - center)
  mat <- t(t(mat)/scale)
  return(mat)
}

CalcualateMedianUvector <- function(yearly.data, interval) {

  yearly.data |> 
      dplyr::summarise(median = rep(mean(value), interval),
                       up = mean(value)*(1:interval) + 0.75*sd(value)*sqrt(1:interval),
                       down = mean(value)*(1:interval) - 0.75*sd(value)*sqrt(1:interval)) |> 
      dplyr::mutate(time.lag = 1:interval)
}

ConsolidateAndTranslate <- function(data, U, svd.decomposition, n) {

  forward.U <- data |> 
    tidyr::pivot_wider(names_from = PC,
                       values_from = values) |> 
    dplyr::select(-time.lag)
  
  projeted.U <- dplyr::bind_rows(U, 
                                 forward.U) |> 
    as.matrix()
  
  forward.indices <- descale(projeted.U, 
                             abs(svd.decomposition$L), 
                             svd.decomposition$sigma, 
                             svd.decomposition$center) |> 
    tibble::as_tibble()
  
  colnames(forward.indices) <- colnames(svd.decomposition$reconstructed.indices)
  
  return(forward.indices)
}

ProjectReturns <- function(svd.decomposition, interval, ratio) {
  
  U <- svd.decomposition[["U"]] |> 
    tibble::as_tibble() 
  
  n <- nrow(U)

  U |> 
    plyr::mutate(obs.n = 1:n) |> 
    tidyr::pivot_longer(cols = -obs.n,
                        names_to = "PC",
                        values_to = "value") |> 
    dplyr::group_nest(PC) |> 
    dplyr::mutate(median.u = purrr::map(data, 
                                        ~CalcualateMedianUvector(.x, interval))) |> 
    dplyr::select(-data) |> 
    tidyr::unnest(cols = c(median.u)) |> 
    tidyr::pivot_longer(c(-PC, -time.lag), names_to = "type", values_to = "values") |> 
    dplyr::group_nest(type) |> 
    dplyr::mutate(proj.returns = purrr::map(data, 
                                            ~ConsolidateAndTranslate(.x, U, svd.decomposition,
                                                                     n))) |> 
    dplyr::select(-data) 
  
}