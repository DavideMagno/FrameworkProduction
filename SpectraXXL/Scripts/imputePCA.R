moy.p <- function(V, poids) {
  res <- sum(V * poids, na.rm = TRUE)/sum(poids[!is.na(V)])
}
ec <- function(V, poids) {
  res <- sqrt(sum(V^2 * poids, na.rm = TRUE)/sum(poids[!is.na(V)]))
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