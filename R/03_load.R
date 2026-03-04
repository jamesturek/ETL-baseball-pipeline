# Load transformed tables into PostgreSQL
# Uses INSERT ON CONFLICT DO NOTHING for safe incremental loads

library(DBI)
library(RPostgres)
library(dplyr)

# -------------------------
# Load processed data
# -------------------------
games        <- readRDS("data/processed/games.rds")
players      <- readRDS("data/processed/players.rds")
batted_balls <- readRDS("data/processed/batted_balls.rds")
linescore    <- readRDS("data/processed/linescore.rds")
batting_logs  <- readRDS("data/processed/batting_logs.rds")
pitching_logs <- readRDS("data/processed/pitching_logs.rds")

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

message("Connected to PostgreSQL: baseball_statcast")

# -------------------------
# Create tables if not exists
# -------------------------
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS games (
    game_pk          BIGINT PRIMARY KEY,
    game_date        DATE,
    venue_name       TEXT,
    venue_id         INTEGER,
    temperature      INTEGER,
    wind             TEXT,
    attendance       INTEGER,
    home_team_id     INTEGER,
    home_team_name   TEXT,
    away_team_id     INTEGER,
    away_team_name   TEXT,
    cws_home         BOOLEAN,
    cws_score        INTEGER,
    opp_score        INTEGER,
    opponent         TEXT,
    result           TEXT,
    total_innings    INTEGER,
    game_type        TEXT
  )
")

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS players (
    player_id  INTEGER PRIMARY KEY,
    full_name  TEXT,
    position   TEXT,
    team       TEXT,
    team_id    INTEGER
  )
")

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS linescore (
    game_pk          BIGINT,
    inning           INTEGER,
    inning_label     TEXT,
    home_team_id     INTEGER,
    home_team_name   TEXT,
    away_team_id     INTEGER,
    away_team_name   TEXT,
    home_runs        INTEGER,
    home_hits        INTEGER,
    home_errors      INTEGER,
    home_left_on_base INTEGER,
    away_runs        INTEGER,
    away_hits        INTEGER,
    away_errors      INTEGER,
    away_left_on_base INTEGER,
    PRIMARY KEY (game_pk, inning)
  )
")

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS batted_balls (
    game_pk              BIGINT,
    at_bat_number        INTEGER,
    pitch_number         INTEGER,
    game_date            DATE,
    batter               BIGINT,
    pitcher              BIGINT,
    player_name          TEXT,
    stand                TEXT,
    p_throws             TEXT,
    home_team            TEXT,
    away_team            TEXT,
    inning               INTEGER,
    inning_topbot        TEXT,
    pitch_type           TEXT,
    pitch_name           TEXT,
    release_speed        NUMERIC,
    effective_speed      NUMERIC,
    release_spin_rate    NUMERIC,
    spin_axis            NUMERIC,
    zone                 INTEGER,
    bb_type              TEXT,
    events               TEXT,
    description          TEXT,
    launch_speed         NUMERIC,
    launch_angle         NUMERIC,
    hit_distance_sc      NUMERIC,
    hc_x                 NUMERIC,
    hc_y                 NUMERIC,
    spray_angle          NUMERIC,
    spray_zone           TEXT,
    hard_hit             BOOLEAN,
    barrel               BOOLEAN,
    estimated_ba_using_speedangle  NUMERIC,
    estimated_slg_using_speedangle NUMERIC,
    woba_value           NUMERIC,
    babip_value          INTEGER,
    iso_value            INTEGER,
    launch_speed_angle   INTEGER,
    delta_run_exp        NUMERIC,
    delta_home_win_exp   NUMERIC,
    home_win_exp         NUMERIC,
    bat_win_exp          NUMERIC,
    home_score           INTEGER,
    away_score           INTEGER,
    bat_score            INTEGER,
    fld_score            INTEGER,
    if_fielding_alignment TEXT,
    of_fielding_alignment TEXT,
    game_type            TEXT,
    PRIMARY KEY (game_pk, at_bat_number, pitch_number)
  )
")

message("Tables created (if not already existing)")

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS batting_logs (
    player_name  TEXT,
    player_id    INTEGER,
    team         TEXT,
    game_date    DATE,
    opponent     TEXT,
    home_game    BOOLEAN,
    ab           INTEGER,
    pa           INTEGER,
    h            INTEGER,
    doubles      INTEGER,
    triples      INTEGER,
    hr           INTEGER,
    rbi          INTEGER,
    bb           INTEGER,
    k            INTEGER,
    sb           INTEGER,
    avg          NUMERIC,
    obp          NUMERIC,
    slg          NUMERIC,
    ops          NUMERIC,
    woba         NUMERIC,
    wrc_plus     INTEGER,
    PRIMARY KEY (player_id, game_date)
  )
")

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS pitching_logs (
    player_name  TEXT,
    player_id    INTEGER,
    team         TEXT,
    game_date    DATE,
    opponent     TEXT,
    home_game    BOOLEAN,
    gs           INTEGER,
    ip           NUMERIC,
    h            INTEGER,
    er           INTEGER,
    hr           INTEGER,
    bb           INTEGER,
    k            INTEGER,
    era          NUMERIC,
    whip         NUMERIC,
    k_per_9      NUMERIC,
    bb_per_9     NUMERIC,
    fip          NUMERIC,
    game_score   NUMERIC,
    PRIMARY KEY (player_id, game_date)
  )
")

# -------------------------
# Helper: upsert rows safely
# -------------------------
upsert_table <- function(con, table_name, data, conflict_cols) {
  if (nrow(data) == 0) {
    message("No rows to load for: ", table_name)
    return(invisible(NULL))
  }

  # Write to a temporary table
  tmp <- paste0("tmp_", table_name)
  dbWriteTable(con, tmp, data, overwrite = TRUE, temporary = TRUE)

  # Build INSERT ... ON CONFLICT DO NOTHING
  cols        <- paste(names(data), collapse = ", ")
  conflict    <- paste(conflict_cols, collapse = ", ")
  sql <- glue::glue("
    INSERT INTO {table_name} ({cols})
    SELECT {cols} FROM {tmp}
    ON CONFLICT ({conflict}) DO NOTHING
  ")

  rows_inserted <- dbExecute(con, sql)
  message("Loaded ", rows_inserted, " new rows into: ", table_name)
}

# Install glue if needed
if (!requireNamespace("glue", quietly = TRUE)) install.packages("glue")
library(glue)
games        <- as.data.frame(games)
players      <- as.data.frame(players)
batted_balls <- as.data.frame(batted_balls)
linescore    <- as.data.frame(linescore)
games$home_team_id <- as.integer(games$home_team_id)
games$away_team_id <- as.integer(games$away_team_id)
games$home_team_id    <- as.integer(games$home_team_id)
games$away_team_id    <- as.integer(games$away_team_id)
linescore$home_team_id <- as.integer(linescore$home_team_id)
linescore$away_team_id <- as.integer(linescore$away_team_id)
batting_logs  <- as.data.frame(batting_logs)
pitching_logs <- as.data.frame(pitching_logs)

# -------------------------
# Load each table
# -------------------------
upsert_table(con, "games",        games,        "game_pk")
upsert_table(con, "players",      players,      "player_id")
upsert_table(con, "linescore",    linescore,    c("game_pk", "inning"))
upsert_table(con, "batting_logs",  batting_logs,  c("player_id", "game_date"))
upsert_table(con, "pitching_logs", pitching_logs, c("player_id", "game_date"))
# Drop and recreate batted_balls to match actual columns
dbExecute(con, "DROP TABLE IF EXISTS batted_balls")
dbWriteTable(con, "batted_balls", batted_balls, overwrite = TRUE)
dbExecute(con, "ALTER TABLE batted_balls ADD PRIMARY KEY (game_pk, at_bat_number, pitch_number)")
message("Loaded ", nrow(batted_balls), " rows into: batted_balls")

# -------------------------
# Create indexes for fast querying
# -------------------------
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bb_game_pk    ON batted_balls (game_pk)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bb_batter     ON batted_balls (batter)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bb_game_date  ON batted_balls (game_date)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bb_hard_hit   ON batted_balls (hard_hit)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bb_barrel     ON batted_balls (barrel)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_ls_game_pk    ON linescore (game_pk)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_games_date    ON games (game_date)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bat_logs_date ON batting_logs (game_date)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bat_logs_pid  ON batting_logs (player_id)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pit_logs_date ON pitching_logs (game_date)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pit_logs_pid  ON pitching_logs (player_id)")

message("Indexes created")

# -------------------------
# Quick row count verification
# -------------------------
tables <- c("games", "players", "linescore", "batted_balls", "batting_logs", "pitching_logs")
for (t in tables) {
  n <- dbGetQuery(con, paste0("SELECT COUNT(*) as n FROM ", t))$n
  message(t, ": ", n, " rows in database")
}

dbDisconnect(con)
message("Load complete. Database connection closed.")
