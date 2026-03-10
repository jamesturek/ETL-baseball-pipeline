# Transform raw extracts into clean, analysis-ready tables
# Produces: games, players, batted_balls, linescore, pitches

library(dplyr)
library(lubridate)
library(janitor)

# -------------------------
# Load raw data
# -------------------------
batted_balls_raw   <- readRDS("data/raw/batted_balls_raw.rds")
game_info_raw      <- readRDS("data/raw/game_info_raw.rds")
linescore_raw      <- readRDS("data/raw/linescore_raw.rds")
batting_orders_raw <- readRDS("data/raw/batting_orders_raw.rds")
schedule_raw       <- readRDS("data/raw/schedule_raw.rds")
pitches_raw        <- if (file.exists("data/raw/pitches_raw.rds")) readRDS("data/raw/pitches_raw.rds") else NULL
batting_logs_raw   <- if (file.exists("data/raw/batting_logs_raw.rds"))  readRDS("data/raw/batting_logs_raw.rds")  else NULL
pitching_logs_raw  <- if (file.exists("data/raw/pitching_logs_raw.rds")) readRDS("data/raw/pitching_logs_raw.rds") else NULL

# -------------------------
# CWS game_pks only
# -------------------------
cws_game_pks <- schedule_raw$game_pk

if (length(cws_game_pks) == 0) {
  message("No CWS games in this date range. Skipping transform.")
  writeLines("skip", "data/raw/pipeline_status.txt")
  stop("No CWS games in date range.")
}

if (nrow(batted_balls_raw) == 0) {
  message("No Statcast data available yet. Try again later - Statcast updates daily at 3am Eastern.")
  writeLines("skip", "data/raw/pipeline_status.txt")
  stop("No Statcast data available.")
}

# -------------------------
# 1. Games table
# -------------------------
message("Transforming games table...")

final_scores <- linescore_raw |>
  group_by(game_pk) |>
  summarise(
    home_team_id   = first(home_team_id),
    home_team_name = first(home_team_name),
    away_team_id   = first(away_team_id),
    away_team_name = first(away_team_name),
    home_final_score = sum(home_runs, na.rm = TRUE),
    away_final_score = sum(away_runs, na.rm = TRUE),
    total_innings    = max(num),
    .groups = "drop"
  ) |>
  mutate(
    cws_home = home_team_id == 145,
    cws_score = if_else(cws_home, home_final_score, away_final_score),
    opp_score = if_else(cws_home, away_final_score, home_final_score),
    opponent  = if_else(cws_home, away_team_name, home_team_name),
    result    = case_when(
      cws_score > opp_score ~ "W",
      cws_score < opp_score ~ "L",
      TRUE ~ "T"
    )
  )

games <- game_info_raw |>
  inner_join(final_scores, by = "game_pk") |>
  select(
    game_pk,
    game_date,
    venue_name,
    venue_id,
    temperature,
    wind,
    attendance,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    cws_home,
    cws_score,
    opp_score,
    opponent,
    result,
    total_innings,
    game_type
  ) |>
  mutate(
    game_date   = as.Date(game_date),
    attendance  = as.integer(gsub(",", "", attendance)),
    temperature = as.integer(gsub("[^0-9]", "", temperature))
  )

message("Games rows: ", nrow(games))

# -------------------------
# 2. Players table
# -------------------------
message("Transforming players table...")

players <- batting_orders_raw |>
  select(
    player_id = id,
    full_name  = fullName,
    position   = abbreviation,
    team       = teamName,
    team_id    = teamID
  ) |>
  distinct(player_id, .keep_all = TRUE)

message("Players rows: ", nrow(players))

# -------------------------
# 3. Batted balls table
# -------------------------
message("Transforming batted balls table...")

drop_cols <- c(
  "spin_dir", "spin_rate_deprecated", "break_angle_deprecated",
  "break_length_deprecated", "tfs_deprecated", "tfs_zulu_deprecated",
  "umpire", "sv_id", "bat_speed", "swing_length", "arm_angle",
  "attack_angle", "attack_direction", "swing_path_tilt",
  "intercept_ball_minus_batter_pos_x_inches",
  "intercept_ball_minus_batter_pos_y_inches",
  "pitcher_days_since_prev_game", "batter_days_since_prev_game",
  "pitcher_days_until_next_game", "batter_days_until_next_game",
  "woba_denom", "estimated_woba_using_speedangle"
)

batted_balls <- batted_balls_raw |>
  filter(game_pk %in% cws_game_pks) |>
  select(-any_of(drop_cols)) |>
  mutate(
    spray_angle = round(
      atan((hc_x - 125.42) / (198.27 - hc_y)) * (180 / pi) * 0.75,
      1
    ),
    spray_zone = case_when(
      spray_angle < -15 ~ "pull",
      spray_angle > 15  ~ "oppo",
      TRUE              ~ "center"
    ),
    hard_hit  = launch_speed >= 95,
    barrel    = launch_speed_angle == 6,
    bb_type   = na_if(bb_type, ""),
    events    = na_if(events, ""),
    game_date = as.Date(game_date)
  ) |>
  clean_names()

message("Batted ball rows: ", nrow(batted_balls))

# -------------------------
# 4. Linescore table
# -------------------------
message("Transforming linescore table...")

linescore <- linescore_raw |>
  filter(game_pk %in% cws_game_pks) |>
  select(
    game_pk,
    inning         = num,
    inning_label   = ordinal_num,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_runs,
    home_hits,
    home_errors,
    home_left_on_base,
    away_runs,
    away_hits,
    away_errors,
    away_left_on_base
  ) |>
  clean_names()

message("Linescore rows: ", nrow(linescore))

# -------------------------
# 5. Pitches table
# -------------------------
if (!is.null(pitches_raw) && nrow(pitches_raw) > 0) {
  message("Transforming pitches table...")

  pitches <- pitches_raw |>
    filter(game_pk %in% cws_game_pks) |>
    select(-any_of(drop_cols)) |>
    mutate(
      game_date  = as.Date(game_date),
      pitch_type = na_if(pitch_type, ""),
      pitch_name = na_if(pitch_name, ""),
      description = na_if(description, ""),
      events      = na_if(events, ""),
      pitch_result = case_when(
        description %in% c("called_strike", "swinging_strike",
                            "swinging_strike_blocked", "foul_tip") ~ "Strike",
        description %in% c("ball", "blocked_ball",
                            "pitchout", "hit_by_pitch")             ~ "Ball",
        description %in% c("foul", "foul_bunt")                    ~ "Foul",
        description == "hit_into_play" & events == "home_run"       ~ "Home Run",
        description == "hit_into_play" & events %in% c(
          "single", "double", "triple")                             ~ "Hit",
        description == "hit_into_play"                              ~ "Out",
        TRUE                                                        ~ "Other"
      ),
      elite_velo = pitch_type %in% c("FF", "SI", "FC") &
                   release_speed >= 99
    ) |>
    clean_names()

  message("Pitches rows: ", nrow(pitches))
} else {
  message("No pitch data available, skipping.")
  pitches <- NULL
}

# -------------------------
# 6. Batting game logs
# -------------------------
if (!is.null(batting_logs_raw) && nrow(batting_logs_raw) > 0) {
  message("Transforming batting game logs...")
  batting_logs <- batting_logs_raw |>
    select(
      player_name = PlayerName,
      player_id   = playerid,
      team        = Team,
      game_date   = Date,
      opponent    = Opp,
      home_game   = Home,
      ab          = AB,
      pa          = PA,
      h           = H,
      doubles     = Doubles,
      triples     = Triples,
      hr          = HR,
      rbi         = RBI,
      bb          = BB,
      k           = SO,
      sb          = SB,
      avg         = AVG,
      obp         = OBP,
      slg         = SLG,
      ops         = OPS,
      woba        = wOBA,
      wrc_plus    = wRC_plus
    ) |>
    mutate(
      game_date = as.Date(game_date),
      home_game = home_game == "",
      across(c(ab, pa, h, doubles, triples, hr, rbi, bb, k, sb), as.integer),
      across(c(avg, obp, slg, ops, woba), as.numeric),
      wrc_plus  = as.integer(wrc_plus)
    ) |>
    clean_names()
  message("Batting log rows: ", nrow(batting_logs))
} else {
  message("No batting log data available, skipping.")
  batting_logs <- NULL
}

# -------------------------
# 7. Pitching game logs
# -------------------------
if (!is.null(pitching_logs_raw) && nrow(pitching_logs_raw) > 0) {
  message("Transforming pitching game logs...")
  pitching_logs <- pitching_logs_raw |>
    select(
      player_name = PlayerName,
      player_id   = playerid,
      team        = Team,
      game_date   = Date,
      opponent    = Opp,
      home_game   = Home,
      gs          = GS,
      ip          = IP,
      h           = H,
      er          = ER,
      hr          = HR,
      bb          = BB,
      k           = SO,
      era         = ERA,
      whip        = WHIP,
      k_per_9     = K_9,
      bb_per_9    = BB_9,
      fip         = FIP,
      game_score  = GameScore
    ) |>
    mutate(
      game_date = as.Date(game_date),
      home_game = home_game == "",
      gs        = as.integer(gs),
      across(c(h, er, hr, bb, k), as.integer),
      across(c(ip, era, whip, k_per_9, bb_per_9, fip, game_score), as.numeric)
    ) |>
    clean_names()
  message("Pitching log rows: ", nrow(pitching_logs))
} else {
  message("No pitching log data available, skipping.")
  pitching_logs <- NULL
}

# -------------------------
# Save processed data
# -------------------------
saveRDS(games,        "data/processed/games.rds")
saveRDS(players,      "data/processed/players.rds")
saveRDS(batted_balls, "data/processed/batted_balls.rds")
saveRDS(linescore,    "data/processed/linescore.rds")
if (!is.null(pitches))       saveRDS(pitches,       "data/processed/pitches.rds")
if (!is.null(batting_logs))  saveRDS(batting_logs,  "data/processed/batting_logs.rds")
if (!is.null(pitching_logs)) saveRDS(pitching_logs, "data/processed/pitching_logs.rds")

message("All transformed tables saved to data/processed/")
message("Transform complete.")