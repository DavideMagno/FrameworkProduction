landing_table() |> 
  dplyr::mutate(Currency = stringr::str_extract(Ticker, "^\\D{6}"),
                `Asset Class` = stringr::str_remove(Ticker, "^\\D{6}") |> 
                  stringr::str_squish()) |> 
  reactable::reactable(
    filterable = TRUE, resizable = TRUE, showPageSizeOptions = FALSE, 
    selection = "single", onClick = "select", bordered = TRUE, 
    borderless = TRUE, striped = TRUE, highlight = TRUE, compact = TRUE, 
    
    showSortable = TRUE, height = "auto", 
    # theme = reactable::reactableTheme(cellPadding = "8px 12px"),
    groupBy = c("Currency", "Asset Class"),
    columns = list(
      `Starting Date` = reactable::colDef(align = "center", cell = function(value) {
        strftime(value, format = "%Y-%m-%d")
      }),
      `Current Index Value` = reactable::colDef(
        filterable = FALSE, align = "center",
        format = reactable::colFormat(digits = 2, separators = TRUE)
      ),
      `Distance from Estimated Fair Value (z-score)` = reactable::colDef(
        filterable = FALSE, align = "center",
        format = reactable::colFormat(digits = 2, separators = TRUE)
      ),
      Dates = reactable::colDef(show = FALSE),
      `Fair Value Estimation Confidence` = reactable::colDef(
        align = "center",
        cell = function(value) {
          flag <- ifelse(grepl("Low",value, ignore.case = TRUE),
                         "low",
                         ifelse(grepl("High",value, ignore.case = TRUE), "high", 
                                "medium"))
          class <- paste0("tag status-", flag)
          div(class = class, value)
        })
    ),
    defaultColDef = reactable::colDef(vAlign =  "center",
                                      filterInput = function(values, name) {
                                        dataListFilter("items-list")(values, name)
                                      }
    ),
    theme = theme,
    style = list(fontFamily = "Lato, sans-serif", fontSize = "0.875rem")
  )
