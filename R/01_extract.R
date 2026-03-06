# dir.create("R")
# dir.create("data/raw", recursive = TRUE)
# dir.create("data/processed", recursive = TRUE)
# dir.create("outputs/visuals", recursive = TRUE)
# install.packages(c("baseballr", "dplyr", "lubridate", "DBI", "RPostgres", "janitor"))

# Incremental extract: CWS + opponent data for 2026 season
# Functions used: statcast_search, mlb_schedule, mlb_game_info, mlb_game_linescore, mlb_batting_orders, fg_pitcher_game_logs

library(baseballr)
library(dplyr)
library(lubridate)
library(purrr)
library(DBI)
library(RPostgres)

# -------------------------
# Database connection
# -------------------------
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "postgres",
  host     = Sys.getenv("SUPA_HOST"),
  port     = as.integer(Sys.getenv("SUPA_PORT", "5432")),
  user     = "postgres.phvritbiwlcsjxqhizpt",
  password = Sys.getenv("SUPA_PASSWORD")
)

# -------------------------
# Determine date range
# -------------------------
if (dbExistsTable(con, "batted_balls")) {
  last_date  <- dbGetQuery(con, "SELECT MAX(game_date) AS last_date FROM batted_balls")$last_date
  if (is.na(last_date)) {
    start_date <- "2026-03-03"
    message("Table exists but is empty. Starting from: ", start_date)
  } else {
    start_date <- as.character(as.Date(last_date) + 1)
    message("Incremental run: pulling from ", start_date)
  }
} else {
  start_date <- "2026-03-03"
  message("First run: pulling from ", start_date)
}

dbDisconnect(con)

end_date <- as.character(Sys.Date())
up_to_date <- as.Date(start_date) > as.Date(end_date)

if (up_to_date) {
  message("Database already up to date. No new data to pull.")
  writeLines("skip", "data/raw/pipeline_status.txt")
} else {

  message("Date range: ", start_date, " to ", end_date)

  # -------------------------
  # 1. CWS game list
  # -------------------------
  message("Pulling CWS schedule...")
  cws_schedule <- mlb_schedule(
    season    = 2026,
    level_ids = "1"
  ) |>
    filter(
      as.Date(substr(game_date, 1, 10)) >= as.Date(start_date),
      as.Date(substr(game_date, 1, 10)) <= as.Date(end_date),
      status_detailed_state == "Final",
      teams_home_team_id == 145 | teams_away_team_id == 145,
      game_type %in% c("R", "F", "D", "L", "W", "S")
    )

  game_pks <- cws_schedule$game_pk
  message("Games found: ", length(game_pks))

  # -------------------------
  # 2. Game info
  # -------------------------
  message("Pulling game info...")
  game_info_raw <- map_dfr(game_pks, function(pk) {
    Sys.sleep(1)
    tryCatch(
      mlb_game_info(game_pk = pk),
      error = function(e) {
        message("Game info failed for: ", pk)
        NULL
      }
    )
  })

  # -------------------------
  # 3. Linescore
  # -------------------------
  message("Pulling linescores...")
  linescore_raw <- map_dfr(game_pks, function(pk) {
    Sys.sleep(1)
    tryCatch(
      mlb_game_linescore(game_pk = pk),
      error = function(e) {
        message("Linescore failed for: ", pk)
        NULL
      }
    )
  })

  # -------------------------
  # 4. Batting orders
  # -------------------------
  message("Pulling batting orders...")
  batting_orders_raw <- map_dfr(game_pks, function(pk) {
    Sys.sleep(1)
    tryCatch(
      mlb_batting_orders(game_pk = pk) |> mutate(game_pk = pk),
      error = function(e) {
        message("Batting orders failed for: ", pk)
        NULL
      }
    )
  })

  # -------------------------
  # 5. Statcast batted balls
  # -------------------------
  chunk_starts <- seq(as.Date(start_date), as.Date(end_date), by = "month")
  chunk_ends   <- pmin(chunk_starts + months(1) - days(1), as.Date(end_date))
  date_chunks  <- map2(chunk_starts, chunk_ends, c)

  pull_statcast <- function(chunk, team, player_type) {
    Sys.sleep(2)
    tryCatch(
      statcast_search(
        start_date  = as.character(chunk[1]),
        end_date    = as.character(chunk[2]),
        team        = team,
        player_type = player_type
      ),
      error = function(e) {
        message("Statcast failed: ", chunk[1], " ", player_type)
        NULL
      }
    )
  }

  message("Pulling CWS batter Statcast data...")
  cws_batted <- map_dfr(date_chunks, pull_statcast, team = "CWS", player_type = "batter")

  message("Pulling opponent batter Statcast data...")
  opp_batted <- map_dfr(date_chunks, pull_statcast, team = "CWS", player_type = "pitcher")

  batted_balls_raw <- bind_rows(cws_batted, opp_batted) |>
    filter(!is.na(launch_speed), !is.na(launch_angle)) |>
    distinct(game_pk, at_bat_number, pitch_number, .keep_all = TRUE)

  message("Batted ball events extracted: ", nrow(batted_balls_raw))

  # -------------------------
  # 5b. Full pitch-by-pitch data
  # -------------------------
  message("Pulling CWS full pitch-by-pitch data...")
  cws_pitches <- map_dfr(date_chunks, pull_statcast, team = "CWS", player_type = "batter")

  message("Pulling opponent full pitch-by-pitch data...")
  opp_pitches <- map_dfr(date_chunks, pull_statcast, team = "CWS", player_type = "pitcher")

  pitches_raw <- bind_rows(cws_pitches, opp_pitches) |>
    distinct(game_pk, at_bat_number, pitch_number, .keep_all = TRUE)

  message("Total pitches extracted: ", nrow(pitches_raw))

  # -------------------------
  # 6. Batting game logs (FanGraphs)
  # -------------------------
  message("Pulling CWS batting game logs...")
  season <- as.integer(format(Sys.Date(), "%Y"))

  batting_logs_raw <- tryCatch({
    logs <- fg_batter_game_logs(
      playerid   = NULL,
      year       = season
    ) |>
      filter(Team == "CWS")
    message("Batting log rows: ", nrow(logs))
    logs
  }, error = function(e) {
    message("Batting logs failed: ", e$message)
    NULL
  })

  # -------------------------
  # 7. Pitching game logs (FanGraphs)
  # -------------------------
  message("Pulling CWS pitching game logs...")

  pitching_logs_raw <- tryCatch({
    logs <- fg_pitcher_game_logs(
      playerid   = NULL,
      year       = season
    ) |>
      filter(Team == "CWS")
    message("Pitching log rows: ", nrow(logs))
    logs
  }, error = function(e) {
    message("Pitching logs failed: ", e$message)
    NULL
  })

  # -------------------------
  # Save all raw extracts
  # -------------------------
  saveRDS(game_info_raw,      "data/raw/game_info_raw.rds")
  saveRDS(linescore_raw,      "data/raw/linescore_raw.rds")
  saveRDS(batting_orders_raw, "data/raw/batting_orders_raw.rds")
  saveRDS(batted_balls_raw,   "data/raw/batted_balls_raw.rds")
  saveRDS(cws_schedule,       "data/raw/schedule_raw.rds")
  saveRDS(pitches_raw,        "data/raw/pitches_raw.rds")
  if (!is.null(batting_logs_raw))  saveRDS(batting_logs_raw,  "data/raw/batting_logs_raw.rds")
  if (!is.null(pitching_logs_raw)) saveRDS(pitching_logs_raw, "data/raw/pitching_logs_raw.rds")

  writeLines("run", "data/raw/pipeline_status.txt")

  message("All raw extracts saved to data/raw/")
  message("Extract complete.")

} # end if/else up_to_date

