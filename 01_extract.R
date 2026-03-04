# dir.create("R")
# dir.create("data/raw", recursive = TRUE)
# dir.create("data/processed", recursive = TRUE)
# dir.create("outputs/visuals", recursive = TRUE)
# install.packages(c("baseballr", "dplyr", "lubridate", "DBI", "RPostgres", "janitor"))

# Incremental extract: CWS + opponent batted balls, box scores, player/pitcher stats
# First run pulls full 2026 season to date; subsequent runs pull new games only

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
  dbname   = "baseball_statcast",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = Sys.getenv("PGPASSWORD")
)

# -------------------------
# Determine date range
# -------------------------
if (dbExistsTable(con, "batted_balls")) {
  last_date  <- dbGetQuery(con, "SELECT MAX(game_date) AS last_date FROM batted_balls")$last_date
  start_date <- as.character(as.Date(last_date) + 1)
  message("Incremental run: pulling from ", start_date)
} else {
  start_date <- "2026-03-03"  # Cactus League start date for this pipeline
  message("First run: pulling full 2026 season from ", start_date)
}

dbDisconnect(con)

end_date <- as.character(Sys.Date() - 1) # yesterday ensures games are finalised

if (as.Date(start_date) > as.Date(end_date)) {
  message("Database already up to date. Exiting.")
  quit(save = "no")
}

# -------------------------
# 1. Statcast batted balls (both teams in CWS games)
# -------------------------
chunk_starts <- seq(as.Date(start_date), as.Date(end_date), by = "month")
chunk_ends   <- pmin(chunk_starts + months(1) - days(1), as.Date(end_date))
date_chunks  <- map2(chunk_starts, chunk_ends, c)

pull_statcast <- function(chunk, team, player_type) {
  Sys.sleep(2)
  statcast_search(
    start_date  = as.character(chunk[1]),
    end_date    = as.character(chunk[2]),
    team        = team,
    player_type = player_type
  )
}

message("Pulling CWS batter Statcast data...")
cws_batted <- map_dfr(date_chunks, pull_statcast, team = "CWS", player_type = "batter")

message("Pulling opponent batter Statcast data (games vs CWS)...")
opp_batted <- map_dfr(date_chunks, pull_statcast, team = "CWS", player_type = "pitcher")

# Combine and filter to batted ball events only
batted_balls_raw <- bind_rows(cws_batted, opp_batted) |>
  filter(!is.na(launch_speed), !is.na(launch_angle)) |>
  distinct(game_pk, at_bat_number, pitch_number, .keep_all = TRUE)

message("Batted ball events extracted: ", nrow(batted_balls_raw))

# -------------------------
# 2. CWS game list for this date range
# -------------------------
message("Pulling CWS schedule/game list...")
cws_schedule <- mlb_schedule(
  season    = 2026,
  team_id   = 145,
  level_ids = c("1", "S")  # "1" = MLB regular season, "S" = spring training
) |>
  filter(
    as.Date(date) >= as.Date(start_date),
    as.Date(date) <= as.Date(end_date),
    status_detailed_state == "Final"
  )

game_pks <- cws_schedule$game_pk

message("Games found: ", length(game_pks))

# -------------------------
# 3. Box scores for each game
# -------------------------
message("Pulling box scores...")
box_scores_raw <- map(game_pks, function(pk) {
  Sys.sleep(1)
  tryCatch(
    mlb_game_boxscore(game_pk = pk),
    error = function(e) {
      message("Box score failed for game_pk: ", pk)
      NULL
    }
  )
})

# -------------------------
# 4. Batter game stats (both teams)
# -------------------------
message("Pulling batter game logs...")
batter_stats_raw <- map_dfr(game_pks, function(pk) {
  Sys.sleep(1)
  tryCatch({
    mlb_batting_orders(game_pk = pk) |>
      mutate(game_pk = pk)
  }, error = function(e) {
    message("Batter stats failed for game_pk: ", pk)
    NULL
  })
})

# -------------------------
# 5. Pitcher game stats (both teams)
# -------------------------
message("Pulling pitcher game logs...")
pitcher_stats_raw <- map_dfr(game_pks, function(pk) {
  Sys.sleep(1)
  tryCatch({
    mlb_game_linescore(game_pk = pk) |>
      mutate(game_pk = pk)
  }, error = function(e) {
    message("Pitcher stats failed for game_pk: ", pk)
    NULL
  })
})

# -------------------------
# Save all raw extracts
# -------------------------
saveRDS(batted_balls_raw,  "data/raw/batted_balls_raw.rds")
saveRDS(cws_schedule,      "data/raw/schedule_raw.rds")
saveRDS(box_scores_raw,    "data/raw/box_scores_raw.rds")
saveRDS(batter_stats_raw,  "data/raw/batter_stats_raw.rds")
saveRDS(pitcher_stats_raw, "data/raw/pitcher_stats_raw.rds")

message("All raw extracts saved to data/raw/")