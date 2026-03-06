# ============================================================
#  CWS Baseball Pipeline - Shiny App
#  Tabs: Latest Game | Recent Recap | Season Stats
# ============================================================

library(shiny)
library(bslib)
library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)
library(GeomMLBStadiums)
library(glue)
library(DT)
library(scales)
library(lubridate)
library(tidyr)

# ── DB connection ──────────────────────────────────────────────────────────────

get_con <- function() {
  dbConnect(
    RPostgres::Postgres(),
    dbname   = "postgres",
    host     = "aws-1-us-east-1.pooler.supabase.com",
    port     = 6543,
    user     = "postgres.phvritbiwlcsjxqhizpt",
    password = Sys.getenv("PGPASSWORD")
  )
}

# ── Helpers ────────────────────────────────────────────────────────────────────

cws_colour  <- "#27251F"
cws_silver  <- "#C4CED4"
cws_accent  <- "#BF0D3E"

fmt_pct <- function(x) sprintf("%.3f", x)
fmt_era <- function(x) sprintf("%.2f", x)
fmt_ip  <- function(x) sprintf("%.1f", x)

value_box_card <- function(title, value, icon = NULL, colour = cws_accent) {
  div(
    class = "value-box-custom",
    style = glue("border-left: 4px solid {colour}; padding: 16px 20px;
                  background: #1a1a1a; border-radius: 6px; margin-bottom: 12px;"),
    div(style = "font-size: 11px; color: #888; text-transform: uppercase;
                 letter-spacing: 1px; margin-bottom: 4px;", title),
    div(style = glue("font-size: 28px; font-weight: 700; color: {colour};"), value)
  )
}

result_colour <- function(result) {
  if (is.null(result) || is.na(result)) return("#888")
  if (grepl("^W", result)) "#4CAF50" else if (grepl("^L", result)) cws_accent else "#888"
}

# ── UI ─────────────────────────────────────────────────────────────────────────

ui <- page_navbar(
  title = div(
    img(src = "https://upload.wikimedia.org/wikipedia/commons/thumb/3/39/Chicago_White_Sox_Logo.svg/200px-Chicago_White_Sox_Logo.svg.png",
        height = "32px", style = "margin-right: 10px; vertical-align: middle;"),
    span("CWS Baseball", style = "font-weight: 700; color: white;")
  ),
  theme = bs_theme(
    bg         = "#111111",
    fg         = "#EEEEEE",
    primary    = cws_accent,
    secondary  = cws_silver,
    base_font  = font_google("Inter"),
    heading_font = font_google("Inter"),
    bootswatch = "darkly"
  ),
  fillable = FALSE,

  # ── TAB 1: Latest Game ──────────────────────────────────────────────────────
  nav_panel(
    "Latest Game",
    icon = bsicons::bs_icon("activity"),
    div(style = "max-width: 1200px; margin: 0 auto; padding: 20px;",

      # Game header
      uiOutput("game_header"),
      hr(style = "border-color: #333;"),

      # Box score
      h4("Box Score", style = "color: #ccc; margin-bottom: 12px;"),
      div(style = "overflow-x: auto;",
        tableOutput("linescore_table")
      ),
      hr(style = "border-color: #333;"),

      # Key stats row
      h4("Game Highlights", style = "color: #ccc; margin-bottom: 12px;"),
      uiOutput("game_highlights"),
      hr(style = "border-color: #333;"),

      # Visuals row 1: spray charts
      h4("Batted Ball Spray Charts", style = "color: #ccc; margin-bottom: 12px;"),
      fluidRow(
        column(6, plotOutput("cws_spray",  height = "420px")),
        column(6, plotOutput("opp_spray",  height = "420px"))
      ),
      hr(style = "border-color: #333;"),

      # Win probability
      h4("Win Probability", style = "color: #ccc; margin-bottom: 12px;"),
      plotOutput("win_prob_chart", height = "340px"),
      hr(style = "border-color: #333;"),

      # Pitch speed + best/worst PAs
      fluidRow(
        column(6,
          h4("Pitch Speed by Pitcher", style = "color: #ccc; margin-bottom: 12px;"),
          plotOutput("pitch_speed_chart", height = "340px")
        ),
        column(6,
          h4("Best & Worst Plate Appearances", style = "color: #ccc; margin-bottom: 12px;"),
          uiOutput("best_worst_pa")
        )
      )
    )
  ),

  # ── TAB 2: Recent Recap ─────────────────────────────────────────────────────
  nav_panel(
    "Recent Recap",
    icon = bsicons::bs_icon("journal-text"),
    div(style = "max-width: 900px; margin: 0 auto; padding: 20px;",

      h3("Series & Recent Game Recap", style = "color: white; margin-bottom: 6px;"),
      p("A look back at the most recent series.", style = "color: #888; margin-bottom: 24px;"),

      # Recent games results strip
      h5("Recent Results", style = "color: #ccc; margin-bottom: 12px;"),
      uiOutput("recent_results_strip"),
      hr(style = "border-color: #333; margin: 24px 0;"),

      # Recap text input (admin)
      h5("Series Recap", style = "color: #ccc; margin-bottom: 12px;"),
      uiOutput("recap_display"),
      hr(style = "border-color: #333; margin: 24px 0;"),

      # Last 5 games batting leaders
      h5("Last 5 Games: Batting Leaders", style = "color: #ccc; margin-bottom: 12px;"),
      DTOutput("recent_batting_table"),
      br(),
      h5("Last 5 Games: Pitching Leaders", style = "color: #ccc; margin-bottom: 12px;"),
      DTOutput("recent_pitching_table")
    )
  ),

  # ── TAB 3: Season Stats ─────────────────────────────────────────────────────
  nav_panel(
    "Season Stats",
    icon = bsicons::bs_icon("bar-chart"),
    div(style = "max-width: 1200px; margin: 0 auto; padding: 20px;",

      h3("2025 Season Statistics", style = "color: white; margin-bottom: 6px;"),
      uiOutput("season_record_header"),
      hr(style = "border-color: #333; margin: 20px 0;"),

      # Filters
      fluidRow(
        column(4,
          selectInput("stat_team_filter", "Team",
            choices = c("Chicago White Sox (CWS)" = "CWS", "All opponents" = "ALL"),
            selected = "CWS"
          )
        ),
        column(4,
          selectInput("stat_split_filter", "Split",
            choices = c("All games" = "all", "Home" = "home", "Away" = "away"),
            selected = "all"
          )
        )
      ),

      tabsetPanel(
        tabPanel("Batting",
          br(),
          DTOutput("season_batting_table")
        ),
        tabPanel("Pitching",
          br(),
          DTOutput("season_pitching_table")
        ),
        tabPanel("Game Log",
          br(),
          DTOutput("game_log_table")
        )
      )
    )
  )
)

# ── Server ─────────────────────────────────────────────────────────────────────

server <- function(input, output, session) {

  # -- Data loading ------------------------------------------------------------

  con <- get_con()
  onStop(function() dbDisconnect(con))

  latest_game <- reactive({
    dbGetQuery(con,
      "SELECT * FROM games ORDER BY game_date DESC LIMIT 1"
    )
  })

  latest_pitches <- reactive({
    g <- latest_game()
    dbGetQuery(con, glue_sql(
      "SELECT * FROM pitches WHERE game_date = {g$game_date}",
      .con = con
    ))
  })

  latest_batted <- reactive({
    g <- latest_game()
    dbGetQuery(con, glue_sql(
      "SELECT * FROM batted_balls WHERE game_date = {g$game_date}",
      .con = con
    ))
  })

  latest_linescore <- reactive({
    g <- latest_game()
    dbGetQuery(con, glue_sql(
      "SELECT * FROM linescore WHERE game_pk = {g$game_pk} ORDER BY inning",
      .con = con
    ))
  })

  all_games <- reactive({
    dbGetQuery(con, "SELECT * FROM games ORDER BY game_date DESC")
  })

  all_batting <- reactive({
    dbGetQuery(con, "SELECT * FROM batting_logs ORDER BY game_date DESC")
  })

  all_pitching <- reactive({
    dbGetQuery(con, "SELECT * FROM pitching_logs ORDER BY game_date DESC")
  })

  # -- TAB 1: Latest Game ------------------------------------------------------

  output$game_header <- renderUI({
    g <- latest_game()
    if (nrow(g) == 0) return(div("No games found."))

    result_col <- result_colour(g$result)
    home_away  <- if (g$cws_home) "vs" else "@"
    date_fmt   <- format(as.Date(g$game_date), "%A, %B %d %Y")

    div(
      style = "display: flex; align-items: center; justify-content: space-between;
               flex-wrap: wrap; gap: 16px; margin-bottom: 20px;",
      div(
        div(style = "font-size: 13px; color: #888; margin-bottom: 4px;", date_fmt),
        div(style = "font-size: 26px; font-weight: 700; color: white;",
          glue("CWS {home_away} {g$opponent}")
        ),
        div(style = "font-size: 14px; color: #888; margin-top: 4px;",
          glue("{g$venue_name}  |  {g$temperature}F  |  {g$wind}")
        )
      ),
      div(
        style = glue("font-size: 52px; font-weight: 900; color: {result_col};
                      line-height: 1;"),
        glue("{g$cws_score} - {g$opp_score}")
      ),
      div(
        style = glue("font-size: 20px; font-weight: 700; color: {result_col};
                      padding: 8px 20px; border: 2px solid {result_col};
                      border-radius: 4px;"),
        g$result
      )
    )
  })

  output$linescore_table <- renderTable({
    ls <- latest_linescore()
    g  <- latest_game()
    if (nrow(ls) == 0) return(data.frame(Message = "No linescore data"))

    innings <- ls$inning
    cws_name <- if (g$cws_home) g$home_team_name else g$away_team_name
    opp_name <- if (g$cws_home) g$away_team_name else g$home_team_name

    if (g$cws_home) {
      cws_runs <- ls$home_runs
      opp_runs <- ls$away_runs
      cws_hits <- sum(ls$home_hits, na.rm = TRUE)
      opp_hits <- sum(ls$away_hits, na.rm = TRUE)
      cws_err  <- sum(ls$home_errors, na.rm = TRUE)
      opp_err  <- sum(ls$away_errors, na.rm = TRUE)
    } else {
      cws_runs <- ls$away_runs
      opp_runs <- ls$home_runs
      cws_hits <- sum(ls$away_hits, na.rm = TRUE)
      opp_hits <- sum(ls$home_hits, na.rm = TRUE)
      cws_err  <- sum(ls$away_errors, na.rm = TRUE)
      opp_err  <- sum(ls$home_errors, na.rm = TRUE)
    }

    inning_cols        <- as.list(cws_runs)
    names(inning_cols) <- as.character(innings)
    cws_row <- c(Team = cws_name, inning_cols, R = g$cws_score, H = cws_hits, E = cws_err)

    inning_cols2        <- as.list(opp_runs)
    names(inning_cols2) <- as.character(innings)
    opp_row <- c(Team = opp_name, inning_cols2, R = g$opp_score, H = opp_hits, E = opp_err)

    as.data.frame(rbind(cws_row, opp_row), stringsAsFactors = FALSE)
  },
  striped = TRUE, hover = TRUE, bordered = TRUE,
  width = "100%", align = "c"
  )

  output$game_highlights <- renderUI({
    g  <- latest_game()
    bb <- latest_batted()
    p  <- latest_pitches()

    if (nrow(bb) == 0) return(NULL)

    n_barrels  <- sum(bb$barrel == TRUE, na.rm = TRUE)
    cws_barrels <- bb |>
      filter(barrel == TRUE) |>
      filter((home_team == "CWS" & inning_topbot == "Bot") |
             (away_team == "CWS" & inning_topbot == "Top")) |>
      nrow()

    avg_ev <- if ("launch_speed" %in% names(bb))
      round(mean(bb$launch_speed[!is.na(bb$launch_speed) &
                   ((bb$home_team == "CWS" & bb$inning_topbot == "Bot") |
                    (bb$away_team == "CWS" & bb$inning_topbot == "Top"))], na.rm = TRUE), 1)
    else NA

    n_pitches <- nrow(p)
    elite_velo <- if ("elite_velo" %in% names(p)) sum(p$elite_velo == TRUE, na.rm = TRUE) else NA

    fluidRow(
      column(3, value_box_card("CWS Score",     g$cws_score,       colour = result_colour(g$result))),
      column(3, value_box_card("Opp Score",      g$opp_score,       colour = "#888")),
      column(3, value_box_card("CWS Barrels",    cws_barrels,       colour = cws_accent)),
      column(3, value_box_card("CWS Avg EV",     if (!is.na(avg_ev)) paste0(avg_ev, " mph") else "N/A",
                                                  colour = cws_silver))
    )
  })

  # Spray charts
  make_spray <- function(data, title) {
    data <- data |>
      filter(!is.na(hc_x), !is.na(hc_y)) |>
      mlbam_xy_transformation() |>
      mutate(
        hit_result = case_when(
          events == "home_run" ~ "Home Run",
          events == "triple"   ~ "Triple",
          events == "double"   ~ "Double",
          events == "single"   ~ "Single",
          TRUE                 ~ "Out"
        ),
        hit_result = factor(hit_result,
          levels = c("Home Run", "Triple", "Double", "Single", "Out"))
      )

    colours <- c(
      "Home Run" = "#BF0D3E",
      "Triple"   = "#FF8C00",
      "Double"   = "#FFD700",
      "Single"   = "#4CAF50",
      "Out"      = "#555555"
    )

    ggplot(data, aes(x = hc_x_, y = hc_y_, colour = hit_result)) +
      geom_mlb_stadium(
        stadium_ids              = "generic",
        stadium_transform_coords = TRUE,
        stadium_segments         = "all",
        colour                   = "#8B7355",
        fill                     = NA,
        linewidth                = 0.5
      ) +
      geom_spraychart(
        stadium_ids              = "generic",
        stadium_transform_coords = TRUE,
        size  = 3,
        alpha = 0.85
      ) +
      scale_colour_manual(values = colours, name = NULL) +
      coord_fixed() +
      labs(title = title) +
      theme_void() +
      theme(
        plot.background  = element_rect(fill = "#111111", colour = NA),
        panel.background = element_rect(fill = "#111111", colour = NA),
        plot.title       = element_text(colour = "white", size = 13,
                                        face = "bold", hjust = 0.5,
                                        margin = margin(b = 8)),
        legend.position  = "bottom",
        legend.text      = element_text(colour = "#ccc", size = 9),
        legend.key       = element_rect(fill = NA)
      )
  }

  output$cws_spray <- renderPlot({
    bb <- latest_batted()
    g  <- latest_game()
    if (nrow(bb) == 0) return(NULL)

    cws_bb <- bb |>
      filter(
        (home_team == "CWS" & inning_topbot == "Bot") |
        (away_team == "CWS" & inning_topbot == "Top")
      )
    make_spray(cws_bb, "White Sox Batted Balls")
  }, bg = "#111111")

  output$opp_spray <- renderPlot({
    bb <- latest_batted()
    g  <- latest_game()
    if (nrow(bb) == 0) return(NULL)

    opp_bb <- bb |>
      filter(
        (home_team == "CWS" & inning_topbot == "Top") |
        (away_team == "CWS" & inning_topbot == "Bot")
      )
    make_spray(opp_bb, paste0(latest_game()$opponent, " Batted Balls"))
  }, bg = "#111111")

  # Win probability chart
  output$win_prob_chart <- renderPlot({
    p <- latest_pitches()
    g <- latest_game()
    if (nrow(p) == 0 || !"home_win_exp" %in% names(p)) return(NULL)

    p <- p |>
      filter(!is.na(home_win_exp)) |>
      mutate(
        pitch_seq  = row_number(),
        cws_win_exp = if (g$cws_home) home_win_exp else 1 - home_win_exp,
        inning_num  = as.integer(inning),
        scoring     = !is.na(delta_run_exp) & abs(delta_run_exp) >= 0.1
      )

    # Inning breaks for vlines
    inning_starts <- p |>
      group_by(inning_num, inning_topbot) |>
      summarise(pitch_seq = min(pitch_seq), .groups = "drop")

    ggplot(p, aes(x = pitch_seq, y = cws_win_exp)) +
      geom_vline(data = inning_starts, aes(xintercept = pitch_seq),
                 colour = "#333", linewidth = 0.3) +
      geom_hline(yintercept = 0.5, colour = "#555", linetype = "dashed", linewidth = 0.5) +
      geom_line(colour = cws_silver, linewidth = 1) +
      geom_area(fill = cws_accent, alpha = 0.15) +
      geom_point(data = filter(p, scoring), aes(x = pitch_seq, y = cws_win_exp),
                 colour = cws_accent, size = 2.5) +
      scale_y_continuous(labels = percent_format(), limits = c(0, 1),
                         breaks = seq(0, 1, 0.25)) +
      scale_x_continuous(expand = c(0.01, 0)) +
      labs(
        x = "Pitch sequence",
        y = "CWS Win Probability",
        caption = "Red dots = scoring plays (|delta run exp| >= 0.10)"
      ) +
      theme_minimal(base_family = "Inter") +
      theme(
        plot.background  = element_rect(fill = "#111111", colour = NA),
        panel.background = element_rect(fill = "#111111", colour = NA),
        panel.grid.major = element_line(colour = "#222"),
        panel.grid.minor = element_blank(),
        axis.text        = element_text(colour = "#888", size = 9),
        axis.title       = element_text(colour = "#aaa", size = 10),
        plot.caption     = element_text(colour = "#555", size = 8)
      )
  }, bg = "#111111")

  # Pitch speed chart
  output$pitch_speed_chart <- renderPlot({
    p <- latest_pitches()
    g <- latest_game()
    if (nrow(p) == 0 || !"release_speed" %in% names(p)) return(NULL)

    # CWS pitchers only
    cws_pitches <- p |>
      filter(
        !is.na(release_speed), !is.na(player_name),
        (home_team == "CWS" & inning_topbot == "Top") |
        (away_team == "CWS" & inning_topbot == "Bot")
      ) |>
      group_by(player_name) |>
      summarise(
        avg_velo = mean(release_speed, na.rm = TRUE),
        max_velo = max(release_speed, na.rm = TRUE),
        n_pitches = n(),
        .groups = "drop"
      ) |>
      filter(n_pitches >= 5) |>
      arrange(avg_velo) |>
      mutate(player_name = factor(player_name, levels = player_name))

    ggplot(cws_pitches, aes(y = player_name)) +
      geom_segment(aes(x = avg_velo, xend = max_velo, yend = player_name),
                   colour = "#444", linewidth = 1) +
      geom_point(aes(x = avg_velo), colour = cws_silver, size = 3.5) +
      geom_point(aes(x = max_velo), colour = cws_accent, size = 3.5) +
      geom_text(aes(x = avg_velo, label = round(avg_velo, 1)),
                colour = cws_silver, size = 3, vjust = -1) +
      geom_text(aes(x = max_velo, label = round(max_velo, 1)),
                colour = cws_accent, size = 3, vjust = -1) +
      labs(x = "Velocity (mph)", y = NULL,
           caption = "Silver = avg  |  Red = max") +
      theme_minimal(base_family = "Inter") +
      theme(
        plot.background  = element_rect(fill = "#111111", colour = NA),
        panel.background = element_rect(fill = "#111111", colour = NA),
        panel.grid.major.y = element_blank(),
        panel.grid.major.x = element_line(colour = "#222"),
        panel.grid.minor   = element_blank(),
        axis.text    = element_text(colour = "#ccc", size = 10),
        axis.title.x = element_text(colour = "#888", size = 9),
        plot.caption = element_text(colour = "#555", size = 8)
      )
  }, bg = "#111111")

  # Best/worst PAs
  output$best_worst_pa <- renderUI({
    p <- latest_pitches()
    g <- latest_game()
    if (nrow(p) == 0 || !"delta_run_exp" %in% names(p)) return(NULL)

    pa_summary <- p |>
      filter(!is.na(delta_run_exp), !is.na(events), events != "") |>
      group_by(at_bat_number, player_name, events, inning, inning_topbot) |>
      summarise(total_dre = sum(delta_run_exp, na.rm = TRUE), .groups = "drop") |>
      mutate(
        side = case_when(
          (g$cws_home & inning_topbot == "Bot") |
          (!g$cws_home & inning_topbot == "Top") ~ "CWS",
          TRUE ~ "Opp"
        )
      )

    best3 <- pa_summary |> arrange(desc(total_dre)) |> head(3)
    worst3 <- pa_summary |> arrange(total_dre) |> head(3)

    make_pa_card <- function(row, type) {
      col <- if (type == "best") "#4CAF50" else cws_accent
      inn_label <- paste0("Inn ", as.integer(row$inning),
                          if (row$inning_topbot == "Top") "T" else "B")
      div(
        style = glue("border-left: 3px solid {col}; padding: 8px 12px;
                      background: #1a1a1a; border-radius: 4px; margin-bottom: 8px;"),
        div(style = "font-size: 12px; color: #888;",
            glue("{row$side}  |  {inn_label}")),
        div(style = "font-size: 14px; font-weight: 600; color: white;",
            row$player_name),
        div(style = "font-size: 12px; color: #aaa;",
            glue("{row$events}  |  dRE: {round(row$total_dre, 3)}"))
      )
    }

    tagList(
      h6("Best PAs", style = "color: #4CAF50; margin-bottom: 8px;"),
      lapply(seq_len(nrow(best3)), function(i) make_pa_card(best3[i,], "best")),
      h6("Worst PAs", style = "color: #BF0D3E; margin-top: 16px; margin-bottom: 8px;"),
      lapply(seq_len(nrow(worst3)), function(i) make_pa_card(worst3[i,], "worst"))
    )
  })

  # -- TAB 2: Recent Recap -----------------------------------------------------

  output$recent_results_strip <- renderUI({
    games <- all_games() |> head(7)
    if (nrow(games) == 0) return(NULL)

    cards <- lapply(seq_len(nrow(games)), function(i) {
      g   <- games[i,]
      col <- result_colour(g$result)
      ha  <- if (g$cws_home) "vs" else "@"
      div(
        style = glue("display: inline-block; text-align: center; padding: 10px 14px;
                      border: 2px solid {col}; border-radius: 6px; margin-right: 8px;
                      margin-bottom: 8px; background: #1a1a1a; min-width: 90px;"),
        div(style = "font-size: 10px; color: #666;",
            format(as.Date(g$game_date), "%b %d")),
        div(style = "font-size: 11px; color: #aaa; margin: 2px 0;",
            glue("{ha} {g$opponent}")),
        div(style = glue("font-size: 16px; font-weight: 700; color: {col};"),
            glue("{g$cws_score}-{g$opp_score}")),
        div(style = glue("font-size: 11px; color: {col};"), g$result)
      )
    })

    div(style = "display: flex; flex-wrap: wrap;", cards)
  })

  # Recap text: edit recap.md in your project root to update
  output$recap_display <- renderUI({
    recap_file <- "recap.md"
    if (file.exists(recap_file)) {
      text <- paste(readLines(recap_file), collapse = "\n")
      div(
        style = "background: #1a1a1a; border-radius: 6px; padding: 20px 24px;
                 color: #ddd; line-height: 1.7; font-size: 14px;",
        HTML(markdown::markdownToHTML(text = text, fragment.only = TRUE))
      )
    } else {
      div(
        style = "background: #1a1a1a; border-radius: 6px; padding: 20px 24px;
                 color: #666; font-style: italic;",
        "No recap yet. Add a recap.md file to your app directory to display your series write-up here."
      )
    }
  })

  output$recent_batting_table <- renderDT({
    games <- all_games() |> head(5)
    if (nrow(games) == 0) return(NULL)
    recent_dates <- games$game_date

    all_batting() |>
      filter(team == "CWS", game_date %in% recent_dates) |>
      group_by(player_name) |>
      summarise(
        G   = n(),
        AB  = sum(ab),
        H   = sum(h),
        HR  = sum(hr),
        RBI = sum(rbi),
        BB  = sum(bb),
        K   = sum(k),
        AVG = round(sum(h) / pmax(sum(ab), 1), 3),
        OPS = round(mean(ops, na.rm = TRUE), 3),
        .groups = "drop"
      ) |>
      arrange(desc(OPS)) |>
      datatable(
        rownames = FALSE,
        options  = list(pageLength = 10, dom = "t", ordering = TRUE),
        class    = "display compact"
      ) |>
      formatStyle(columns = everything(),
                  backgroundColor = "#1a1a1a", color = "#ddd")
  })

  output$recent_pitching_table <- renderDT({
    games <- all_games() |> head(5)
    if (nrow(games) == 0) return(NULL)
    recent_dates <- games$game_date

    all_pitching() |>
      filter(team == "CWS", game_date %in% recent_dates) |>
      group_by(player_name) |>
      summarise(
        G   = n(),
        IP  = round(sum(ip), 1),
        H   = sum(h),
        ER  = sum(er),
        BB  = sum(bb),
        K   = sum(k),
        ERA = round(sum(er) * 9 / pmax(sum(ip), 0.1), 2),
        FIP = round(mean(fip, na.rm = TRUE), 2),
        .groups = "drop"
      ) |>
      arrange(ERA) |>
      datatable(
        rownames = FALSE,
        options  = list(pageLength = 10, dom = "t", ordering = TRUE),
        class    = "display compact"
      ) |>
      formatStyle(columns = everything(),
                  backgroundColor = "#1a1a1a", color = "#ddd")
  })

  # -- TAB 3: Season Stats -----------------------------------------------------

  output$season_record_header <- renderUI({
    games <- all_games() |> filter(game_type == "R")
    if (nrow(games) == 0) return(NULL)

    w <- sum(grepl("^W", games$result))
    l <- sum(grepl("^L", games$result))
    pct <- round(w / pmax(w + l, 1), 3)

    home_games <- games |> filter(cws_home)
    away_games <- games |> filter(!cws_home)
    hw <- sum(grepl("^W", home_games$result))
    hl <- sum(grepl("^L", home_games$result))
    aw <- sum(grepl("^W", away_games$result))
    al <- sum(grepl("^L", away_games$result))

    run_diff <- sum(games$cws_score - games$opp_score)

    fluidRow(
      column(3, value_box_card("Record",    glue("{w}-{l}"),        colour = result_colour("W"))),
      column(2, value_box_card("Win %",     fmt_pct(pct),           colour = cws_silver)),
      column(2, value_box_card("Home",      glue("{hw}-{hl}"),      colour = cws_silver)),
      column(2, value_box_card("Away",      glue("{aw}-{al}"),      colour = cws_silver)),
      column(3, value_box_card("Run Diff",  ifelse(run_diff >= 0, paste0("+", run_diff), run_diff),
                                             colour = if (run_diff >= 0) "#4CAF50" else cws_accent))
    )
  })

  season_batting_filtered <- reactive({
    df <- all_batting()
    games <- all_games()

    if (input$stat_split_filter == "home") {
      df <- df |> filter(home_game == TRUE)
    } else if (input$stat_split_filter == "away") {
      df <- df |> filter(home_game == FALSE)
    }

    if (input$stat_team_filter == "CWS") {
      df <- df |> filter(team == "CWS")
    }

    df |>
      group_by(player_name) |>
      summarise(
        G   = n(),
        PA  = sum(pa),
        AB  = sum(ab),
        H   = sum(h),
        `2B` = sum(doubles),
        `3B` = sum(triples),
        HR  = sum(hr),
        RBI = sum(rbi),
        BB  = sum(bb),
        K   = sum(k),
        SB  = sum(sb),
        AVG = round(sum(h) / pmax(sum(ab), 1), 3),
        OBP = round(mean(obp, na.rm = TRUE), 3),
        SLG = round(mean(slg, na.rm = TRUE), 3),
        OPS = round(mean(ops, na.rm = TRUE), 3),
        wOBA = round(mean(woba, na.rm = TRUE), 3),
        .groups = "drop"
      ) |>
      filter(PA >= 5) |>
      arrange(desc(OPS))
  })

  season_pitching_filtered <- reactive({
    df <- all_pitching()

    if (input$stat_split_filter == "home") {
      df <- df |> filter(home_game == TRUE)
    } else if (input$stat_split_filter == "away") {
      df <- df |> filter(home_game == FALSE)
    }

    if (input$stat_team_filter == "CWS") {
      df <- df |> filter(team == "CWS")
    }

    df |>
      group_by(player_name) |>
      summarise(
        G   = n(),
        GS  = sum(gs),
        IP  = round(sum(ip), 1),
        H   = sum(h),
        ER  = sum(er),
        HR  = sum(hr),
        BB  = sum(bb),
        K   = sum(k),
        ERA = round(sum(er) * 9 / pmax(sum(ip), 0.1), 2),
        WHIP = round((sum(h) + sum(bb)) / pmax(sum(ip), 0.1), 2),
        `K/9` = round(mean(k_per_9, na.rm = TRUE), 1),
        FIP  = round(mean(fip, na.rm = TRUE), 2),
        .groups = "drop"
      ) |>
      filter(IP >= 1) |>
      arrange(ERA)
  })

  output$season_batting_table <- renderDT({
    season_batting_filtered() |>
      datatable(
        rownames = FALSE,
        filter   = "top",
        options  = list(pageLength = 20, scrollX = TRUE, ordering = TRUE),
        class    = "display compact"
      ) |>
      formatStyle("OPS",
        background = styleColorBar(c(0, 1.2), cws_accent),
        backgroundSize = "100% 80%", backgroundRepeat = "no-repeat",
        backgroundPosition = "center") |>
      formatStyle(columns = everything(),
                  backgroundColor = "#1a1a1a", color = "#ddd")
  })

  output$season_pitching_table <- renderDT({
    season_pitching_filtered() |>
      datatable(
        rownames = FALSE,
        filter   = "top",
        options  = list(pageLength = 20, scrollX = TRUE, ordering = TRUE),
        class    = "display compact"
      ) |>
      formatStyle("ERA",
        background = styleColorBar(c(0, 10), "#4CAF50"),
        backgroundSize = "100% 80%", backgroundRepeat = "no-repeat",
        backgroundPosition = "center") |>
      formatStyle(columns = everything(),
                  backgroundColor = "#1a1a1a", color = "#ddd")
  })

  output$game_log_table <- renderDT({
    all_games() |>
      filter(game_type == "R") |>
      mutate(
        Date     = format(as.Date(game_date), "%b %d"),
        HA       = if_else(cws_home, "vs", "@"),
        Opponent = opponent,
        Score    = glue("{cws_score}-{opp_score}"),
        Result   = result,
        Innings  = if_else(total_innings > 9, paste0(total_innings, " inn"), "9"),
        Venue    = venue_name
      ) |>
      select(Date, HA, Opponent, Score, Result, Innings, Venue) |>
      datatable(
        rownames = FALSE,
        options  = list(pageLength = 30, dom = "ftp", ordering = TRUE),
        class    = "display compact"
      ) |>
      formatStyle("Result",
        color = styleEqual(
          c("W", "L"),
          c("#4CAF50", cws_accent)
        )
      ) |>
      formatStyle(columns = everything(),
                  backgroundColor = "#1a1a1a", color = "#ddd")
  })
}

shinyApp(ui, server)
