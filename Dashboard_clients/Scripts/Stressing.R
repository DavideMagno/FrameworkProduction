library(ggplot2)
source(here::here("Scripts/SVD.R"))
source(here::here("Scripts/RelativeTrade.R"))
source(here::here("Scripts/InvarianceTest.R"))

LAG <- 1

df <- read.csv(here::here("Data/All_data.csv")) 

CalculateTrainingSets <- function(df, LAG) {
  N <- ncol(df)
  
  type <- df |> 
    dplyr::slice(1) |> 
    dplyr::select(-period) |> 
    dplyr::mutate_all(~dplyr::if_else(grepl("Log", .x), TRUE, FALSE)) |> 
    purrr::flatten_lgl() 
  
  prices <- df |> 
    dplyr::slice(-1:-2) |> 
    dplyr::mutate(dates = lubridate::as_date(period, format = "%d/%m/%Y"),
                  dplyr::across(2:N, as.numeric)) |> 
    dplyr::select(-period) |> 
    dplyr::select(1:10)
  
  dates <- prices$dates[seq(from = LAG + 1, to = nrow(prices), by = LAG)]
  
  returns <- prices |> 
    dplyr::select(-dates) |> 
    purrr::map2_dfc(type, ~CalculateInvariant(.x, .y, LAG)) |> 
    dplyr::mutate(dates = as.Date(dates)) |> 
    dplyr::select(dates, dplyr::everything())
  
  training.set <- returns |> 
    dplyr::select(-dates) 
  
  training.set.prices <- prices |> 
    dplyr::select(-dates) |> 
    tibble::as_tibble()
  
  n.components <- 10
  svd.analysis <- RunSVD(training.set, n.components, TRUE)
  svd.analysis.price <- RunSVD(training.set.prices, n.components, TRUE)
  
  return(list(svd.analysis = svd.analysis, 
              svd.analysis.price = svd.analysis.price))
}

N <- nrow(df)

base <- CalculateTrainingSets(df, 1)

U.delta.0 <- UInversion(base$svd.analysis$reconstructed.indices, 
                        base$svd.analysis$L, 
                        base$svd.analysis$sigma, 
                        base$svd.analysis$center)/base$svd.analysis$U -1

df[N,2] <- as.character(as.numeric(df[N,2]) * 1.05)

stressed <- CalculateTrainingSets(df, 1)

U.delta.1 <- UInversion(stressed$svd.analysis$reconstructed.indices, 
                        base$svd.analysis$L, 
                        base$svd.analysis$sigma, 
                        base$svd.analysis$center)/base$svd.analysis$U -1

plot(U.delta.1[N - 3,], base$svd.analysis$U[1,])


df[N,2] <- as.character(as.numeric(df[N,2]) / 1.05)
df[N - 1,2] <- as.character(as.numeric(df[N - 1,2]) * 1.05)

stressed <- CalculateTrainingSets(df, 1)

U.delta.2 <- UInversion(stressed$svd.analysis$reconstructed.indices, 
                        base$svd.analysis$L, 
                        base$svd.analysis$sigma, 
                        base$svd.analysis$center)/base$svd.analysis$U -1
