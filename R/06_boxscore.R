# 06_boxscore.R
# Broadcast-style box score with linescore, batting, and pitching lines
# Saves PNG to outputs/visuals/{date}/
#
# Depends on: DBI, RPostgres, dplyr, tidyr, gt, gtExtras, png, RCurl
# Run after: 01_extract.R, 02_transform.R, 03_load.R

library(DBI)
library(RPostgres)
library(dplyr)
library(tidyr)
library(gt)
library(gtExtras)
library(png)
library(RCurl)
library(purrr)

# ── Validate connection ────────────────────────────────────────────────────────

if (!exists("con") || !dbIsValid(con)) {
  stop("No valid database connection. Run from main.R or connect manually.")
}

# ── Query data ─────────────────────────────────────────────────────────────────

batted_balls <- dbGetQuery(con, "SELECT * FROM batted_balls")
linescore    <- dbGetQuery(con, "SELECT * FROM linescore")
games        <- dbGetQuery(con, "SELECT * FROM games")
pitches_raw  <- dbGetQuery(con, "SELECT * FROM pitches")

# ── Setup ──────────────────────────────────────────────────────────────────────

run_date <- format(Sys.Date(), "%Y-%m-%d")
out_dir  <- file.path("outputs", "visuals", run_date)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

latest_game <- games |> arrange(desc(game_date)) |> slice(1)

game_title <- paste0(
  format(as.Date(latest_game$game_date), "%B %d, %Y"), "  |  ",
  latest_game$home_team_name, " vs ", latest_game$away_team_name
)

# ── Team ID lookup ─────────────────────────────────────────────────────────────

team_id_to_espn <- c(
  "108" = "laa", "109" = "ari", "110" = "bal", "111" = "bos",
  "112" = "chc", "113" = "cin", "114" = "cle", "115" = "col",
  "116" = "det", "117" = "hou", "118" = "kc",  "119" = "lad",
  "120" = "wsh", "121" = "nym", "133" = "oak", "134" = "pit",
  "135" = "sd",  "136" = "sea", "137" = "sf",  "138" = "stl",
  "139" = "tb",  "140" = "tex", "141" = "tor", "142" = "min",
  "143" = "phi", "144" = "atl", "145" = "chw", "146" = "mia",
  "147" = "nyy", "158" = "mil"
)

get_logo_url <- function(team_id) {
  espn <- team_id_to_espn[as.character(team_id)]
  paste0("https://a.espncdn.com/i/teamlogos/mlb/500/", espn, ".png")
}

home_logo <- get_logo_url(latest_game$home_team_id)
away_logo <- get_logo_url(latest_game$away_team_id)

# ── Linescore ──────────────────────────────────────────────────────────────────

innings <- linescore |>
  filter(game_pk == latest_game$game_pk) |>
  arrange(inning)

n_innings <- max(innings$inning)

away_runs_by_inning <- innings |>
  select(inning, away_runs) |>
  pivot_wider(names_from = inning, values_from = away_runs, names_prefix = "I")

home_runs_by_inning <- innings |>
  select(inning, home_runs) |>
  pivot_wider(names_from = inning, values_from = home_runs, names_prefix = "I")

# Pull final R from games table to avoid NA from unplayed half-innings
away_R <- if (latest_game$cws_home) latest_game$opp_score else latest_game$cws_score
home_R <- if (latest_game$cws_home) latest_game$cws_score else latest_game$opp_score

away_totals <- innings |>
  summarise(
    R = away_R,
    H = sum(away_hits, na.rm = TRUE),
    E = sum(away_errors, na.rm = TRUE)
  )

home_totals <- innings |>
  summarise(
    R = home_R,
    H = sum(home_hits, na.rm = TRUE),
    E = sum(home_errors, na.rm = TRUE)
  )

linescore_df <- bind_rows(
  bind_cols(
    team  = latest_game$away_team_name,
    logo  = away_logo,
    away_runs_by_inning,
    away_totals
  ),
  bind_cols(
    team  = latest_game$home_team_name,
    logo  = home_logo,
    home_runs_by_inning,
    home_totals
  )
)

linescore_df <- linescore_df |>
  rename_with(~ gsub("^I", "", .), starts_with("I"))

inning_cols <- as.character(1:n_innings)

# ── Helpers ────────────────────────────────────────────────────────────────────

format_name <- function(n) {
  parts <- strsplit(n, ", ")[[1]]
  if (length(parts) == 2) paste(parts[2], parts[1]) else n
}

# ── Batting lines ──────────────────────────────────────────────────────────────

batting <- batted_balls |>
  filter(game_pk == latest_game$game_pk) |>
  mutate(
    team_name = case_when(
      home_team == "CWS" & inning_topbot == "Bot" ~ latest_game$home_team_name,
      away_team == "CWS" & inning_topbot == "Top" ~ latest_game$away_team_name,
      home_team == "CWS" & inning_topbot == "Top" ~ latest_game$away_team_name,
      away_team == "CWS" & inning_topbot == "Bot" ~ latest_game$home_team_name,
      TRUE ~ NA_character_
    )
  ) |>
  group_by(player_name, team_name) |>
  summarise(
    AB  = sum(!is.na(events) & !events %in% c("walk", "hit_by_pitch", "sac_fly", "sac_bunt")),
    H   = sum(events %in% c("single", "double", "triple", "home_run"), na.rm = TRUE),
    R   = sum(post_bat_score - bat_score > 0, na.rm = TRUE),
    RBI = sum(woba_value > 0 & !is.na(woba_value) & events %in% c("single","double","triple","home_run"), na.rm = TRUE),
    BB  = sum(events == "walk", na.rm = TRUE),
    K   = sum(events %in% c("strikeout", "strikeout_double_play"), na.rm = TRUE),
    HR  = sum(events == "home_run", na.rm = TRUE),
    .groups = "drop"
  ) |>
  filter(AB > 0 | BB > 0) |>
  mutate(
    AVG          = if_else(AB > 0, sub("^0", "", sprintf("%.3f", H / AB)), "-"),
    display_name = sapply(player_name, format_name)
  ) |>
  arrange(team_name, desc(AB))

# ── Pitching lines ─────────────────────────────────────────────────────────────

pitcher_ids <- batted_balls |>
  filter(game_pk == latest_game$game_pk) |>
  pull(pitcher) |>
  unique()

pitcher_names <- map_dfr(pitcher_ids, function(id) {
  tryCatch({
    p <- mlb_people(person_ids = id)
    tibble(pitcher = as.numeric(p$id[1]), player_name = p$full_name[1])
  }, error = function(e) {
    tibble(pitcher = as.numeric(id), player_name = NA_character_)
  })
})

out_events <- c(
  "field_out", "strikeout", "strikeout_double_play", "grounded_into_double_play",
  "force_out", "double_play", "triple_play", "fielders_choice_out",
  "caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home",
  "pickoff_caught_stealing_2b", "pickoff_caught_stealing_3b",
  "other_out", "sac_fly", "sac_bunt"
)

ip_by_pitcher <- pitches_raw |>
  filter(game_pk == latest_game$game_pk, !is.na(events)) |>
  mutate(is_out = events %in% out_events) |>
  group_by(pitcher) |>
  summarise(outs = sum(is_out, na.rm = TRUE), .groups = "drop") |>
  mutate(IP = floor(outs / 3) + (outs %% 3) / 10)

pitching <- batted_balls |>
  filter(game_pk == latest_game$game_pk) |>
  mutate(
    pitcher_team = case_when(
      inning_topbot == "Top" ~ home_team,
      inning_topbot == "Bot" ~ away_team
    )
  ) |>
  group_by(pitcher, pitcher_team) |>
  summarise(
    H   = sum(events %in% c("single","double","triple","home_run"), na.rm = TRUE),
    ER  = sum(events %in% c("single","double","triple","home_run"), na.rm = TRUE),
    BB  = sum(events == "walk", na.rm = TRUE),
    K   = sum(events %in% c("strikeout","strikeout_double_play"), na.rm = TRUE),
    HR  = sum(events == "home_run", na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(ip_by_pitcher |> select(pitcher, IP), by = "pitcher") |>
  left_join(pitcher_names, by = "pitcher") |>
  mutate(
    team_name    = case_when(
      pitcher_team == "CWS" ~ latest_game$home_team_name,
      TRUE                  ~ latest_game$away_team_name
    ),
    display_name = sapply(player_name, format_name)
  ) |>
  arrange(team_name)

# ── Build gt tables ────────────────────────────────────────────────────────────

tbl_linescore <- linescore_df |>
  select(logo, team, all_of(inning_cols), R, H, E) |>
  gt() |>
  gt_img_rows(columns = logo, height = 30) |>
  cols_label(
    logo = "",
    team = "",
    R = "R", H = "H", E = "E"
  ) |>
  cols_align(align = "center") |>
  cols_align(align = "left", columns = team) |>
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels()
  ) |>
  tab_style(
    style = list(
      cell_borders(sides = "left", color = "grey80", weight = px(2))
    ),
    locations = cells_body(columns = R)
  ) |>
  tab_style(
    style = cell_fill(color = "#f5f5f5"),
    locations = cells_body(rows = 2)
  ) |>
  tab_options(
    table.font.names          = "Arial",
    table.font.size           = 13,
    column_labels.font.weight = "bold",
    table.border.top.style    = "hidden",
    table.border.bottom.style = "hidden",
    heading.align             = "left"
  ) |>
  tab_header(title = md(paste0("**", game_title, "**")))

make_batting_table <- function(team_name, logo_url) {
  batting |>
    filter(team_name == !!team_name) |>
    select(display_name, AB, H, R, RBI, BB, K, HR, AVG) |>
    gt() |>
    cols_label(display_name = "Batter") |>
    cols_align(align = "center") |>
    cols_align(align = "left", columns = display_name) |>
    tab_style(
      style = cell_text(weight = "bold"),
      locations = cells_column_labels()
    ) |>
    tab_style(
      style = cell_fill(color = "#f5f5f5"),
      locations = cells_body(rows = seq(2, nrow(batting |> filter(team_name == !!team_name)), 2))
    ) |>
    tab_options(
      table.font.names          = "Arial",
      table.font.size           = 13,
      table.border.top.style    = "hidden",
      table.border.bottom.style = "hidden"
    ) |>
    tab_header(title = md(paste0(
      "<img src='", logo_url, "' style='height:28px; vertical-align:middle;'> ",
      "**", team_name, " Batting**"
    )))
}

make_pitching_table <- function(team_name, logo_url) {
  pitching |>
    filter(team_name == !!team_name) |>
    select(display_name, IP, H, ER, BB, K, HR) |>
    gt() |>
    cols_label(display_name = "Pitcher", IP = "IP") |>
    cols_align(align = "center") |>
    cols_align(align = "left", columns = display_name) |>
    tab_style(
      style = cell_text(weight = "bold"),
      locations = cells_column_labels()
    ) |>
    tab_style(
      style = cell_fill(color = "#f5f5f5"),
      locations = cells_body(rows = seq(2, nrow(pitching |> filter(team_name == !!team_name)), 2))
    ) |>
    tab_options(
      table.font.names          = "Arial",
      table.font.size           = 13,
      table.border.top.style    = "hidden",
      table.border.bottom.style = "hidden"
    ) |>
    tab_header(title = md(paste0(
      "<img src='", logo_url, "' style='height:28px; vertical-align:middle;'> ",
      "**", team_name, " Pitching**"
    )))
}

# ── Save tables ────────────────────────────────────────────────────────────────

gtsave(tbl_linescore,
       file.path(out_dir, "06a_linescore.png"))

gtsave(make_batting_table(latest_game$home_team_name, home_logo),
       file.path(out_dir, "06b_home_batting.png"))

gtsave(make_batting_table(latest_game$away_team_name, away_logo),
       file.path(out_dir, "06c_away_batting.png"))

gtsave(make_pitching_table(latest_game$home_team_name, home_logo),
       file.path(out_dir, "06d_home_pitching.png"))

gtsave(make_pitching_table(latest_game$away_team_name, away_logo),
       file.path(out_dir, "06e_away_pitching.png"))

message("Box score tables saved to: ", out_dir)
