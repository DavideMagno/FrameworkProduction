LAG <- 1

df <- read.csv(here::here("Data/All_data.csv")) 

type <- c(rep(TRUE, 12), FALSE, rep(TRUE, 3), rep(FALSE, 7), rep(TRUE, 14))

CalculateInvariant <- function(data, traded.asset, LAG){
  if(traded.asset) {
    invariant <- log(data/dplyr::lag(data, LAG)) |> na.omit()
  } else {
    invariant <- (data - dplyr::lag(data, LAG)) |> na.omit(s)
  }
  return(invariant)
}

ContributionToFactor <- function(pca, factor, sector.matrix, cumulative.flag) {

  if (cumulative.flag) {
    factor2analyse <- 1:factor
  } else {
    factor2analyse <- factor
  }
  
  factoextra::facto_summarize(pca, element = "var", result = "contrib", axes = factor2analyse) |> 
    dplyr::left_join(sector.matrix, by = c("name" = "Index")) |> 
    dplyr::group_by(Market) |> 
    dplyr::summarise(contrib = sum(contrib)) |> 
    dplyr::select(contrib)
}

sector.matrix <- tibble::tibble(Index = colnames(df)[2:ncol(df)],
                                Market = c(rep("Central Bank", 9),
                                           rep("Equity", 7),
                                           rep("Interest Rate", 7),
                                           rep("Commodities", 7),
                                           rep("FX", 7)))

invariant.matrix <- df[2:ncol(df)] |> 
  purrr::map2_dfc(type, ~CalculateInvariant(.x, .y, LAG)) 

invariant.matrix.dates <- invariant.matrix |> 
  dplyr::mutate(period = as.Date(df$period[(LAG + 1):nrow(df)])) |> 
  dplyr::select(period, dplyr::everything()) 

pca <- prcomp(invariant.matrix, scale = TRUE, center = TRUE)

factoextra::fviz_eig(pca, addlabels = TRUE, n = 15)
purrr::map_dfc(1:8, ~ContributionToFactor(pca, .x, sector.matrix, TRUE)) |> 
  purrr::set_names(paste("Principal Component #", 1:8))

