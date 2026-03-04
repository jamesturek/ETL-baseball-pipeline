# Transform raw extracts into clean, analysis-ready tables
# Produces: games, players, batted_balls, linescore

library(dplyr)
library(lubridate)
library(janitor)

# -------------------------
# Load raw data
# -------------------------
batted_balls_raw  <- readRDS("data/raw/batted_balls_raw.rds")
game_info_raw     <- readRDS("data/raw/game_info_raw.rds")
linescore_raw     <- readRDS("data/raw/linescore_raw.rds")
batting_orders_raw <- readRDS("data/raw/batting_orders_raw.rds")
schedule_raw      <- readRDS("data/raw/schedule_raw.rds")

# -------------------------
# CWS game_pks only
# -------------------------
cws_game_pks <- schedule_raw$game_pk

# -------------------------
# 1. Games table
# -------------------------
message("Transforming games table...")

# Aggregate linescore to get final scores per game
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
    game_date  = as.Date(game_date),
    attendance = as.integer(gsub(",", "", attendance)),
    temperature = as.integer(gsub("[^0-9]", "", temperature))
  )

message("Games rows: ", nrow(games))

# -------------------------
# 2. Players table
# -------------------------
message("Transforming players table...")

players <- batting_orders_raw |>
  select(
    player_id    = id,
    full_name    = fullName,
    position     = abbreviation,
    team         = teamName,
    team_id      = teamID
  ) |>
  distinct(player_id, .keep_all = TRUE)

message("Players rows: ", nrow(players))

# -------------------------
# 3. Batted balls table
# -------------------------
message("Transforming batted balls table...")

# Columns to drop: fully NA or deprecated
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
    # Derive spray angle from hc_x and hc_y (standard baseball convention)
    spray_angle = round(
      atan((hc_x - 125.42) / (198.27 - hc_y)) * (180 / pi) * 0.75,
      1
    ),
    # Classify spray direction
    spray_zone = case_when(
      spray_angle < -15 ~ "pull",
      spray_angle > 15  ~ "oppo",
      TRUE              ~ "center"
    ),
    # Hard hit flag (exit velo >= 95 mph)
    hard_hit = launch_speed >= 95,
    # Barrel flag (per Statcast definition)
    barrel = launch_speed_angle == 6,
    # Clean up empty strings
    bb_type  = na_if(bb_type, ""),
    events   = na_if(events, ""),
    game_date = as.Date(game_date)
  ) |>
  clean_names()

message("Batted ball rows: ", nrow(batted_balls))

# -------------------------
# 4. Linescore table (inning by inning)
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
# Save processed data
# -------------------------
saveRDS(games,        "data/processed/games.rds")
saveRDS(players,      "data/processed/players.rds")
saveRDS(batted_balls, "data/processed/batted_balls.rds")
saveRDS(linescore,    "data/processed/linescore.rds")

message("All transformed tables saved to data/processed/")
message("Transform complete.")
