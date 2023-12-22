library(shiny)
library(bslib)
library(shinyjs)
library(shinycssloaders)
library(shinydashboard)
library(shinyWidgets)
library(plotly)
library(glue)
library(shinybusy)
library(openxlsx)

page_sidebar(
  theme = bs_theme(bootswatch = "superhero",
                   font_scale = 0.9,
                   version = 5),
  title = "Framework Dashboard - Hedge Analytics Ltd",
  sidebar = sidebar(width = "300px",
                    open = "desktop",
                    numericInput(inputId = "factors_chosen", 
                                 label = "Choose the number of principal components",
                                 value = 4,
                                 min = 4,
                                 max = 4),
                    uiOutput('date.selection'),
                    conditionalPanel(
                      condition = "input.main_panel === 'Relative Value'",
                      actionButton("final_report", "Download report for all the indices",
                                   icon("download"),
                                   style = "margin-top:15px; margin-bottom:15px;"),
                      conditionalPanel(
                        "false", # always hide the download button
                        downloadButton("downloadData")
                      )
                    )
  ),
  tagList(
    tags$head(
      tags$link(rel = "stylesheet", type = "text/css", href = "appstyle.css")
    ),
    navset_tab(
      id = "main_panel",
      nav_panel(
        "Indices summary",
        card(
          full_screen = FALSE,
          card_header("Table of assets in the universe and fair value estimation"),
          height = "500px",
          card_body(
            DT::DTOutput("landing_table")
          )
        ),
        card(
          full_screen = TRUE,
          card_header("Market, fair value estimation and eigendata plots"),
          # column(12, DT::DTOutput("landing_table"),
          #        style = "height:500px; overflow-y: scroll;")),
          highcharter::highchartOutput("index_and_eigendata", height = "1000px")
          # style = "resize:vertical;"
          # fluidRow(column(12, highcharter::highchartOutput("index_and_eigendata",
          #                                                  height = "750px")))
          # )
        )),
      nav_panel(
        "Self-hedging Portfolios",
        card( 
          layout_columns(
            col_widths = c(2, 10),
            card(
              card_header("Control panel for self hedging portfolios"),
              card_body(
                class = "text-align: center",
                numericInput(inputId = "threshold", 
                             label = "Input the threshold of significance for the factors:",
                             value = 0.8,
                             min = 0.05,
                             max = 1,
                             step = 0.05),
                uiOutput('dates')
              )
            ),
            uiOutput('select.portfolios')
          )
        ),
        uiOutput('return.analysis')
      ),
      nav_panel(
        "Relative Value",
        useShinyjs(),
        div(id='bigbox',
            fluidRow(
              column(4,align = "center", uiOutput('relative.value.asset')),
              column(4,align = "center", switchInput(
                inputId = "remove_trades",
                label = "Include the trades in the graphs", 
                labelWidth = "250px",
                width = "250px"
              )),
              column(4,align = "center", downloadButton("page_report", "Download the trading data",
                                                        style = "margin-top:28;"))
            )
        ),
        fluidRow(
          column(3, h5("Estimated Fair Value"), 
                 wellPanel (
                   div(textOutput("model.value"),style = "font-size:150%"),
                   style="padding: 15px;padding-top: 0px;padding-bottom: 0px;margin-top: 0px;"
                 ), align = 'center', style="margin-top: 0px;"),
          column(3, h5("Trading level"), 
                 wellPanel (
                   div(textOutput("status.trade"),style = "font-size:150%"),
                   style="padding: 15px;padding-top: 0px;padding-bottom: 0px;margin-top: 0px;"
                 ), align = 'center', style="margin-top: 0px;"),
          column(3, h5("Gap to index (z-score)"),
                 wellPanel (
                   div(textOutput("zscore"),style = "font-size:150%"),
                   style="padding: 15px;padding-top: 0px;padding-bottom: 0px;margin-top: 0px;"
                 ), align = 'center', style="margin-top: 0px;"),
          column(3, h5("% of Bets won"),
                 wellPanel (
                   div(textOutput("bet.won"),style = "font-size:150%"),
                   style="padding: 15px;padding-top: 0px;padding-bottom: 0px;margin-top: 0px;"
                 ), align = 'center', style="margin-top: 0px;")
        ),
        br(),
        layout_columns(
          width = 1/2,
          card(
            full_screen = TRUE,
            card_header("Index and Fair Value Estimation Corridor"),
            highcharter::highchartOutput("graph_distance_FALSE")
          ),
          card(
            full_screen = TRUE,
            card_header("Normalised distance between the index and the center of the Fair Value Estimation Corridor"),
            highcharter::highchartOutput("graph_distance_TRUE")
          )
        ),
        br(),
        layout_columns(
          card(
            full_screen = TRUE,
            card_header("Performance of the Relative Trading Strategy (vs Long Only)"),
            highcharter::highchartOutput("graph_pnl_relative")
          ),
          card(
            full_screen = TRUE,
            card_header("Risk Return analysis of the Relative Trading Strategy (vs Long Only)"),
            DT::DTOutput("table.relative.value")
          )
        ),
        br(),
        layout_columns(
          width = 1/3,
          card(
            full_screen = TRUE,
            card_header("Distribution of the Relative Trading Strategy Daily returns"),
            highcharter::highchartOutput("distribution_returns")
          ),
          card(
            full_screen = TRUE,
            card_header("% of trade bets won/lost"),
            highcharter::highchartOutput("betting_results")
          ),
          card(
            full_screen = TRUE,
            card_header("Cumulative distribution of the trades' duration"),
            highcharter::highchartOutput("duration_results")
          )
        )
      ),
      nav_panel(
        "Hedging/Replication",
        div(id='bigbox',
            fluidRow(
              column(4,  align = "center", uiOutput('hedging.asset')),
              column(2, align = "center", uiOutput('dates_hedging')),
              column(2, align = "center", numericInput(inputId = "max_weight",
                                                       label = "Input the maximum weight per index:",
                                                       value = 0.5,
                                                       min = 0,
                                                       max = 1,
                                                       step = 0.1)),
              column(2, align = "center", pickerInput(inputId = "dropdown_method",
                                                      label = "Choose the methodology:",
                                                      selected = "Empirical Tracking Error",
                                                      choices = c("Empirical Tracking Error" = "ete", "Downside Risk" = "dr"),
                                                      multiple = FALSE)),
              column(2,align = "center", switchInput(
                inputId = "use.shorts",
                label = "Allow short selling", 
                labelWidth = "250px",
                width = "250px"
              )
              )
            )
        ),
        layout_columns(
          width = 1/2,
          card(
            full_screen = TRUE,
            card_header("Replication Weights"),
            highcharter::highchartOutput("hedging_weights")
          ),
          card(
            full_screen = TRUE,
            card_header("Replication Efficiency"),
            card_body(
              class = "align-items-center",
              DT::DTOutput("risk.table"),
              textOutput("hedge.efficiency")
            )
          )
        ),
        br(),
        layout_columns(
          width = 1/2,
          card(
            full_screen = TRUE,
            card_header("Performance and drowdown over Training Range"),
            highcharter::highchartOutput("hedging_training")
          ),
          card(
            full_screen = TRUE,
            card_header("Performance and drowdown over Testing Range"),
            highcharter::highchartOutput("hedging_testing")
          )
        )
      )
    )
  )
)