# 08_report.R
# Generates a self-contained HTML daily newsletter report
# Uses Claude API for auto-generated prose intro sections
# Saves to outputs/reports/{date}/

library(DBI)
library(RPostgres)
library(dplyr)
library(tidyr)
library(glue)
library(htmltools)
library(httr)
library(purrr)
library(baseballr)
library(jsonlite)

# ── Connection ─────────────────────────────────────────────────────────────────

con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "baseball_statcast",
  host     = "localhost",
  port     = 6543,
  user     = "postgres.phvritbiwlcsjxqhizpt",
  password = Sys.getenv("PGPASSWORD")
)

games        <- dbGetQuery(con, "SELECT * FROM games")
batted_balls <- dbGetQuery(con, "SELECT * FROM batted_balls")
pitches      <- dbGetQuery(con, "SELECT * FROM pitches")
linescore    <- dbGetQuery(con, "SELECT * FROM linescore")

dbDisconnect(con)

# ── Setup ──────────────────────────────────────────────────────────────────────

run_date    <- format(Sys.Date(), "%Y-%m-%d")
report_dir  <- file.path("outputs", "reports", run_date)
visuals_dir <- file.path("outputs", "visuals", run_date)
dir.create(report_dir, recursive = TRUE, showWarnings = FALSE)

latest_game <- games |> arrange(desc(game_date)) |> slice(1)
game_pk     <- latest_game$game_pk

game_date_fmt <- format(as.Date(latest_game$game_date), "%B %d, %Y")
result_word   <- if_else(latest_game$result == "W", "Victory", "Defeat")
result_colour <- if_else(latest_game$result == "W", "#2DC653", "#E63946")

# ── Team logo URLs ─────────────────────────────────────────────────────────────

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
cws_logo  <- get_logo_url(145)

# ── Pitcher name lookup ────────────────────────────────────────────────────────

pitcher_ids   <- pitches |> filter(game_pk == !!game_pk) |> pull(pitcher) |> unique()
pitcher_names <- map_dfr(pitcher_ids, function(id) {
  tryCatch({
    p <- mlb_people(person_ids = id)
    tibble(pitcher = id, pitcher_name = p$full_name[1])
  }, error = function(e) tibble(pitcher = id, pitcher_name = NA_character_))
})

# ── Build game stats summaries ─────────────────────────────────────────────────

format_name <- function(n) {
  parts <- strsplit(n, ", ")[[1]]
  if (length(parts) == 2) paste(parts[2], parts[1]) else n
}

# Batting summary (RBI removed - not reliably calculable from Statcast events)
batting_summary <- batted_balls |>
  filter(game_pk == !!game_pk) |>
  mutate(
    team_side = case_when(
      home_team == "CWS" & inning_topbot == "Bot" ~ "CWS",
      away_team == "CWS" & inning_topbot == "Top" ~ "CWS",
      TRUE ~ "OPP"
    )
  ) |>
  group_by(player_name, team_side) |>
  summarise(
    AB     = sum(!is.na(events) & !events %in% c("walk","hit_by_pitch","sac_fly","sac_bunt")),
    H      = sum(events %in% c("single","double","triple","home_run"), na.rm = TRUE),
    HR     = sum(events == "home_run", na.rm = TRUE),
    BB     = sum(events == "walk", na.rm = TRUE),
    K      = sum(events %in% c("strikeout","strikeout_double_play"), na.rm = TRUE),
    avg_ev = round(mean(launch_speed, na.rm = TRUE), 1),
    .groups = "drop"
  ) |>
  filter(AB > 0 | BB > 0) |>
  mutate(
    AVG          = if_else(AB > 0, round(H / AB, 3), NA_real_),
    display_name = sapply(player_name, format_name)
  )

cws_batting <- batting_summary |> filter(team_side == "CWS") |> arrange(desc(H))
opp_batting <- batting_summary |> filter(team_side == "OPP") |> arrange(desc(H))

# Pitching summary
pitching_summary <- pitches |>
  filter(game_pk == !!game_pk) |>
  mutate(
    pitcher_team = case_when(
      inning_topbot == "Top" ~ home_team,
      inning_topbot == "Bot" ~ away_team
    )
  ) |>
  group_by(pitcher, pitcher_team) |>
  summarise(
    BF = n_distinct(at_bat_number),
    K  = sum(pitch_result == "Strike", na.rm = TRUE),
    BB = sum(events == "walk", na.rm = TRUE),
    H  = sum(events %in% c("single","double","triple","home_run"), na.rm = TRUE),
    HR = sum(events == "home_run", na.rm = TRUE),
    .groups = "drop"
  ) |>
  left_join(pitcher_names, by = "pitcher") |>
  mutate(
    team_side = if_else(pitcher_team == "CWS", "CWS", "OPP")
  )

cws_pitching <- pitching_summary |> filter(team_side == "CWS") |> arrange(desc(BF))
opp_pitching <- pitching_summary |> filter(team_side == "OPP") |> arrange(desc(BF))

# Linescore
innings <- linescore |>
  filter(game_pk == !!game_pk) |>
  arrange(inning)

# Player of the game (top CWS batter by hits)
potg <- cws_batting |>
  arrange(desc(H), desc(HR)) |>
  slice(1)

# Hard hit summary
hard_hit_rate <- batted_balls |>
  filter(game_pk == !!game_pk) |>
  summarise(
    total    = n(),
    hard_hit = sum(hard_hit, na.rm = TRUE),
    barrels  = sum(barrel, na.rm = TRUE),
    avg_ev   = round(mean(launch_speed, na.rm = TRUE), 1),
    max_ev   = round(max(launch_speed, na.rm = TRUE), 1)
  )

# Elite velo
elite_velo <- pitches |>
  filter(game_pk == !!game_pk, elite_velo == TRUE) |>
  left_join(pitcher_names, by = "pitcher") |>
  arrange(desc(release_speed))

# ── Claude API prose generator ─────────────────────────────────────────────────

claude_write <- function(prompt) {
  Sys.sleep(1)
  response <- httr::POST(
    url = "https://api.anthropic.com/v1/messages",
    httr::add_headers(
      "x-api-key"         = Sys.getenv("ANTHROPIC_API_KEY"),
      "anthropic-version" = "2023-06-01",
      "content-type"      = "application/json"
    ),
    body = jsonlite::toJSON(list(
      model      = "claude-sonnet-4-6",
      max_tokens = 400L,
      messages   = list(
        list(role = "user", content = prompt)
      )
    ), auto_unbox = TRUE),
    encode = "raw"
  )

  if (httr::http_error(response)) {
    message("API error: ", httr::status_code(response))
    message(httr::content(response, as = "text"))
    return("Analysis unavailable.")
  }

  content <- httr::content(response, as = "parsed")
  if (!is.null(content$content[[1]]$text)) {
    return(content$content[[1]]$text)
  } else {
    return("Analysis unavailable.")
  }
}

# ── Generate prose sections ────────────────────────────────────────────────────

message("Generating Claude prose sections...")

# Game summary: Claude writes intro only, facts block handles the rest
game_intro_text <- claude_write(glue(
  "Write 1-2 sentence intro for a White Sox game newsletter. Atmospheric and punchy, set the scene.
  Result: CWS {latest_game$result} {latest_game$cws_score}-{latest_game$opp_score} vs {latest_game$opponent}
  Venue: {latest_game$venue_name}
  Weather: {latest_game$temperature}F, {latest_game$wind}
  Attendance: {latest_game$attendance}
  Do not use em dashes. Sports journalism tone."
))

# Batting: Claude writes intro only
top_cws_hitters <- cws_batting |>
  slice_head(n = 3) |>
  mutate(line = glue("{display_name}: {H}-for-{AB}, {HR} HR, {BB} BB")) |>
  pull(line) |>
  paste(collapse = "; ")

batting_intro_text <- claude_write(glue(
  "Write 1-2 sentence batting intro for a White Sox game newsletter. Set up the offensive story.
  CWS scored {latest_game$cws_score} runs.
  Top performers: {top_cws_hitters}
  Average exit velocity: {hard_hit_rate$avg_ev} mph
  Hard hit rate: {hard_hit_rate$hard_hit}/{hard_hit_rate$total}
  Do not use em dashes. Be specific and analytical."
))

# Pitching: Claude writes intro only
cws_starters <- cws_pitching |>
  slice_head(n = 2) |>
  mutate(line = glue("{pitcher_name}: {BF} BF, {H} H, {K} K")) |>
  pull(line) |>
  paste(collapse = "; ")

elite_velo_text <- if (nrow(elite_velo) > 0) {
  elite_velo |>
    mutate(line = glue("{pitcher_name} {release_speed} mph")) |>
    pull(line) |>
    paste(collapse = ", ")
} else {
  "No 99+ mph pitches recorded"
}

pitching_intro_text <- claude_write(glue(
  "Write 1-2 sentence pitching intro for a White Sox game newsletter. Set up the pitching story.
  CWS allowed {latest_game$opp_score} runs.
  CWS pitchers: {cws_starters}
  Elite velocity (99+ mph): {elite_velo_text}
  Do not use em dashes. Be analytical and specific."
))

# Player of the game
potg_text <- claude_write(glue(
  "Write 2 sentences highlighting the White Sox player of the game for a newsletter.
  Player: {potg$display_name}
  Stats: {potg$H}-for-{potg$AB}, {potg$HR} HR, {potg$BB} BB
  Game result: CWS {latest_game$result} {latest_game$cws_score}-{latest_game$opp_score}
  Do not use em dashes. Be enthusiastic but grounded in the stats."
))

# Looking ahead
next_games <- tryCatch({
  schedule <- mlb_schedule(season = 2026, level_ids = "1") |>
    filter(
      as.Date(game_date) > as.Date(latest_game$game_date),
      teams_home_team_id == 145 | teams_away_team_id == 145,
      status_detailed_state != "Final"
    ) |>
    arrange(game_date) |>
    slice_head(n = 2)

  if (nrow(schedule) > 0) {
    schedule |>
      mutate(line = glue("{format(as.Date(game_date), '%b %d')} vs {teams_away_team_name}")) |>
      pull(line) |>
      paste(collapse = ", ")
  } else {
    "Schedule unavailable"
  }
}, error = function(e) "Schedule unavailable")

looking_ahead_text <- claude_write(glue(
  "Write 2 sentences looking ahead for a White Sox newsletter after a {latest_game$result}.
  Upcoming games: {next_games}
  Do not use em dashes. Be forward looking and set up anticipation for the next game."
))

message("Prose generation complete.")

# ── Image paths ────────────────────────────────────────────────────────────────

img <- function(filename) {
  path <- file.path(visuals_dir, filename)
  if (file.exists(path)) path else NULL
}

cws_spray_path  <- img("01_cws_spray_chart.png")
opp_spray_path  <- img("02_opponent_spray_chart.png")
ev_scatter_path <- img("03_exit_velo_launch_angle.png")
home_bat_path   <- img("06b_home_batting.png")
away_bat_path   <- img("06c_away_batting.png")
home_pitch_path <- img("06d_home_pitching.png")
away_pitch_path <- img("06e_away_pitching.png")

cws_zone_paths <- list.files(visuals_dir, pattern = "^07_cws_.*\\.png$", full.names = TRUE)
opp_zone_paths <- list.files(visuals_dir, pattern = "^07_opp_.*\\.png$", full.names = TRUE)

# ── HTML helpers ───────────────────────────────────────────────────────────────

img_to_base64 <- function(path) {
  if (is.null(path) || !file.exists(path)) return(NULL)
  raw <- readBin(path, "raw", file.info(path)$size)
  b64 <- jsonlite::base64_enc(raw)
  paste0("data:image/png;base64,", b64)
}

html_img <- function(path, style = "width:100%; border-radius:8px;") {
  b64 <- img_to_base64(path)
  if (is.null(b64)) return("")
  glue('<img src="{b64}" style="{style}">')
}

html_table <- function(df, caption = NULL) {
  headers  <- paste0("<th>", names(df), "</th>", collapse = "")
  rows     <- apply(df, 1, function(row) {
    cells <- paste0("<td>", row, "</td>", collapse = "")
    paste0("<tr>", cells, "</tr>")
  })
  rows_html <- paste(rows, collapse = "\n")
  cap_html  <- if (!is.null(caption)) glue("<caption>{caption}</caption>") else ""
  glue('
  <table class="box-score">
    {cap_html}
    <thead><tr>{headers}</tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
  ')
}

section <- function(title, content, id = "") {
  glue('
  <div class="section" id="{id}">
    <h2>{title}</h2>
    {content}
  </div>
  ')
}

prose_block <- function(text) {
  glue('<p class="prose">{text}</p>')
}

two_col <- function(left, right) {
  glue('
  <div class="two-col">
    <div>{left}</div>
    <div>{right}</div>
  </div>
  ')
}

image_grid <- function(paths, cols = 2) {
  if (length(paths) == 0) return("")
  imgs  <- sapply(paths, html_img)
  items <- paste0('<div class="grid-item">', imgs, '</div>', collapse = "\n")
  glue('<div class="image-grid" style="grid-template-columns: repeat({cols}, 1fr)">{items}</div>')
}

# ── CSS ────────────────────────────────────────────────────────────────────────

css <- '
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: "Georgia", serif;
    background: #f9f9f9;
    color: #1a1a1a;
    max-width: 900px;
    margin: 0 auto;
    padding: 20px;
  }
  .header {
    background: #1a1a1a;
    color: white;
    padding: 30px;
    border-radius: 12px;
    margin-bottom: 30px;
    text-align: center;
  }
  .header .date { font-size: 13px; color: #aaa; margin-bottom: 10px; letter-spacing: 2px; text-transform: uppercase; }
  .header .title { font-size: 28px; font-weight: bold; margin-bottom: 16px; }
  .scoreboard {
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 30px;
    margin: 16px 0;
  }
  .scoreboard img { height: 70px; }
  .scoreboard .score { font-size: 48px; font-weight: bold; }
  .scoreboard .team-name { font-size: 13px; color: #aaa; text-align: center; }
  .result-badge {
    display: inline-block;
    padding: 6px 18px;
    border-radius: 20px;
    font-size: 13px;
    font-weight: bold;
    letter-spacing: 1px;
    margin-top: 10px;
  }
  .venue { font-size: 12px; color: #888; margin-top: 8px; }
  .section {
    background: white;
    border-radius: 12px;
    padding: 28px;
    margin-bottom: 24px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
  }
  .section h2 {
    font-size: 18px;
    font-weight: bold;
    border-bottom: 2px solid #f0f0f0;
    padding-bottom: 10px;
    margin-bottom: 16px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #1a1a1a;
  }
  .prose {
    font-size: 15px;
    line-height: 1.75;
    color: #333;
    margin-bottom: 16px;
  }
  .two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-top: 16px;
  }
  .image-grid {
    display: grid;
    gap: 16px;
    margin-top: 16px;
  }
  .grid-item img { width: 100%; border-radius: 8px; }
  .potg-box {
    background: #f5f5f5;
    border-left: 4px solid #1a1a1a;
    padding: 16px 20px;
    border-radius: 0 8px 8px 0;
    margin-top: 12px;
  }
  .potg-name { font-size: 20px; font-weight: bold; margin-bottom: 6px; }
  .potg-prose { font-size: 14px; color: #444; line-height: 1.6; }
  .meta { font-size: 12px; color: #999; margin-top: 6px; }
  .box-score {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin-top: 12px;
  }
  .box-score caption {
    font-size: 13px;
    font-weight: bold;
    text-align: left;
    padding: 6px 0;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .box-score th {
    background: #f0f0f0;
    padding: 7px 10px;
    text-align: center;
    font-weight: bold;
    border-bottom: 2px solid #ddd;
  }
  .box-score td {
    padding: 6px 10px;
    text-align: center;
    border-bottom: 1px solid #f0f0f0;
  }
  .box-score tr:last-child td { border-bottom: none; }
  .box-score td:first-child, .box-score th:first-child { text-align: left; }
  .facts-block {
    margin-top: 16px;
    border-top: 1px solid #f0f0f0;
    padding-top: 12px;
  }
  .fact-row {
    display: flex;
    justify-content: space-between;
    padding: 6px 0;
    border-bottom: 1px solid #f8f8f8;
    font-size: 14px;
  }
  .fact-label {
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-size: 12px;
    font-weight: bold;
  }
  .fact-value {
    color: #1a1a1a;
    font-weight: 500;
  }
  .footer {
    text-align: center;
    font-size: 12px;
    color: #aaa;
    margin-top: 30px;
    padding: 20px;
  }
  @media (max-width: 600px) {
    .two-col { grid-template-columns: 1fr; }
    .scoreboard .score { font-size: 36px; }
  }
'

# ── Build HTML ─────────────────────────────────────────────────────────────────

away_score <- if (latest_game$cws_home) latest_game$opp_score else latest_game$cws_score
home_score <- if (latest_game$cws_home) latest_game$cws_score else latest_game$opp_score

header_html <- glue('
<div class="header">
  <div class="date">{game_date_fmt} &nbsp;|&nbsp; {latest_game$venue_name} &nbsp;|&nbsp; {latest_game$temperature}&deg;F, {latest_game$wind}</div>
  <div class="title">Chicago White Sox Game Report</div>
  <div class="scoreboard">
    <div>
      <img src="{away_logo}">
      <div class="team-name">{latest_game$away_team_name}</div>
    </div>
    <div class="score" style="color:{result_colour}">
      {away_score} &ndash; {home_score}
    </div>
    <div>
      <img src="{home_logo}">
      <div class="team-name">{latest_game$home_team_name}</div>
    </div>
  </div>
  <div>
    <span class="result-badge" style="background:{result_colour}; color:white;">{result_word}</span>
  </div>
  <div class="venue">Attendance: {formatC(latest_game$attendance, format="d", big.mark=",")}</div>
</div>
')

# ── Sections ───────────────────────────────────────────────────────────────────

# 1. Game summary: Claude intro + facts block
game_facts_html <- glue('
<div class="facts-block">
  <div class="fact-row"><span class="fact-label">Result</span><span class="fact-value">{result_word} {latest_game$cws_score}-{latest_game$opp_score}</span></div>
  <div class="fact-row"><span class="fact-label">Innings</span><span class="fact-value">{latest_game$total_innings}</span></div>
  <div class="fact-row"><span class="fact-label">Venue</span><span class="fact-value">{latest_game$venue_name}</span></div>
  <div class="fact-row"><span class="fact-label">Weather</span><span class="fact-value">{latest_game$temperature}F, {latest_game$wind}</span></div>
  <div class="fact-row"><span class="fact-label">Attendance</span><span class="fact-value">{formatC(latest_game$attendance, format="d", big.mark=",")}</span></div>
</div>
')

s1_game_summary <- section(
  "Game Summary",
  paste0(prose_block(game_intro_text), game_facts_html),
  "summary"
)

# 2. Box score: HTML tables from R data frames
cws_bat_table <- cws_batting |>
  select(Player = display_name, AB, H, HR, BB, K, AVG) |>
  mutate(AVG = formatC(AVG, format = "f", digits = 3))

opp_bat_table <- opp_batting |>
  select(Player = display_name, AB, H, HR, BB, K, AVG) |>
  mutate(AVG = formatC(AVG, format = "f", digits = 3))

cws_pitch_table <- cws_pitching |>
  select(Pitcher = pitcher_name, BF, H, K, BB, HR)

opp_pitch_table <- opp_pitching |>
  select(Pitcher = pitcher_name, BF, H, K, BB, HR)

s2_box_score <- section(
  "Box Score",
  paste0(
    "<h3 style='font-size:14px; margin-bottom:4px; color:#666;'>BATTING</h3>",
    two_col(
      html_table(cws_bat_table, caption = "Chicago White Sox"),
      html_table(opp_bat_table, caption = glue("{latest_game$opponent}"))
    ),
    "<h3 style='font-size:14px; margin:24px 0 4px; color:#666;'>PITCHING</h3>",
    two_col(
      html_table(cws_pitch_table, caption = "Chicago White Sox"),
      html_table(opp_pitch_table, caption = glue("{latest_game$opponent}"))
    )
  ),
  "box-score"
)

# 3. Batting analysis
s3_batting <- section(
  "Batting",
  paste0(
    prose_block(batting_intro_text),
    two_col(html_img(home_bat_path), html_img(away_bat_path))
  ),
  "batting"
)

# 4. Pitching analysis
s4_pitching <- section(
  "Pitching",
  paste0(
    prose_block(pitching_intro_text),
    two_col(html_img(home_pitch_path), html_img(away_pitch_path))
  ),
  "pitching"
)

# 5. Pitch locations
s5_strike_zone <- section(
  "Pitch Locations",
  paste0(
    "<h3 style='font-size:14px; margin-bottom:12px; color:#666;'>Chicago White Sox Pitchers</h3>",
    image_grid(cws_zone_paths, cols = 2),
    glue("<h3 style='font-size:14px; margin:20px 0 12px; color:#666;'>{latest_game$opponent} Pitchers</h3>"),
    image_grid(opp_zone_paths, cols = 2)
  ),
  "strike-zone"
)

# 6. Batted ball analysis
s6_spray <- section(
  "Batted Ball Analysis",
  paste0(
    two_col(html_img(cws_spray_path), html_img(opp_spray_path)),
    "<div style='margin-top:20px;'>",
    html_img(ev_scatter_path),
    "</div>"
  ),
  "spray"
)

# 7. Player of the game
s7_potg <- section(
  "Player of the Game",
  glue('
  <div class="potg-box">
    <div class="potg-name">{potg$display_name}</div>
    <div class="meta">{potg$H}-for-{potg$AB} &nbsp;|&nbsp; {potg$HR} HR &nbsp;|&nbsp; {potg$BB} BB</div>
    <div class="potg-prose" style="margin-top:10px;">{potg_text}</div>
  </div>
  '),
  "potg"
)

# 8. Looking ahead
s8_looking_ahead <- section(
  "Looking Ahead",
  prose_block(looking_ahead_text),
  "ahead"
)

footer_html <- glue('
<div class="footer">
  Generated by CWS Baseball Pipeline &nbsp;|&nbsp; {run_date} &nbsp;|&nbsp; Data: Baseball Savant / Statcast
</div>
')

# ── Assemble and save ──────────────────────────────────────────────────────────

full_html <- glue('
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CWS Game Report | {game_date_fmt}</title>
  <style>{css}</style>
</head>
<body>
  {header_html}
  {s1_game_summary}
  {s2_box_score}
  {s3_batting}
  {s4_pitching}
  {s5_strike_zone}
  {s6_spray}
  {s7_potg}
  {s8_looking_ahead}
  {footer_html}
</body>
</html>
')

report_path <- file.path(report_dir, paste0("cws_report_", run_date, ".html"))
writeLines(full_html, report_path)
message("Report saved to: ", report_path)
