# Example SQL queries against the baseball_statcast PostgreSQL database
# Run individual queries interactively to explore the data

library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)

# -------------------------
# Connect
# -------------------------
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "postgres",
  host     = Sys.getenv("SUPA_HOST"),
  port     = 5432,
  user     = "postgres",
  password = Sys.getenv("SUPA_PASSWORD")
)

# -------------------------
# 1. Game results so far
# -------------------------
dbGetQuery(con, "
  SELECT
    game_date,
    opponent,
    CASE WHEN cws_home THEN 'Home' ELSE 'Away' END AS home_away,
    cws_score,
    opp_score,
    result,
    venue_name,
    temperature,
    attendance
  FROM games
  ORDER BY game_date
")

# -------------------------
# 2. Inning by inning linescore
# -------------------------
dbGetQuery(con, "
  SELECT
    g.game_date,
    g.opponent,
    l.inning_label,
    l.home_runs,
    l.away_runs
  FROM linescore l
  JOIN games g ON l.game_pk = g.game_pk
  ORDER BY g.game_date, l.inning
")

# -------------------------
# 3. CWS batters: exit velocity and launch angle summary
# -------------------------
dbGetQuery(con, "
  SELECT
    player_name,
    COUNT(*)                          AS batted_balls,
    ROUND(AVG(launch_speed)::numeric, 1)  AS avg_exit_velo,
    ROUND(MAX(launch_speed)::numeric, 1)  AS max_exit_velo,
    ROUND(AVG(launch_angle)::numeric, 1)  AS avg_launch_angle,
    SUM(CASE WHEN hard_hit THEN 1 ELSE 0 END) AS hard_hit_count,
    ROUND(
      SUM(CASE WHEN hard_hit THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1
    )                                 AS hard_hit_pct
  FROM batted_balls
  WHERE home_team = 'CWS' OR away_team = 'CWS'
    AND stand IS NOT NULL
  GROUP BY player_name
  HAVING COUNT(*) >= 2
  ORDER BY avg_exit_velo DESC
")

# -------------------------
# 4. Barrel summary
# -------------------------
dbGetQuery(con, "
  SELECT
    player_name,
    COUNT(*)          AS batted_balls,
    SUM(CASE WHEN barrel THEN 1 ELSE 0 END) AS barrels,
    ROUND(
      SUM(CASE WHEN barrel THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1
    )                 AS barrel_pct
  FROM batted_balls
  WHERE barrel IS NOT NULL
  GROUP BY player_name
  HAVING COUNT(*) >= 2
  ORDER BY barrel_pct DESC
")

# -------------------------
# 5. Batted ball type breakdown
# -------------------------
dbGetQuery(con, "
  SELECT
    bb_type,
    COUNT(*)                              AS count,
    ROUND(AVG(launch_speed)::numeric, 1)  AS avg_exit_velo,
    ROUND(AVG(launch_angle)::numeric, 1)  AS avg_launch_angle,
    ROUND(AVG(hit_distance_sc)::numeric)  AS avg_distance
  FROM batted_balls
  WHERE bb_type IS NOT NULL
  GROUP BY bb_type
  ORDER BY count DESC
")

# -------------------------
# 6. Pitch type breakdown (what pitches are being thrown against CWS)
# -------------------------
dbGetQuery(con, "
  SELECT
    pitch_name,
    COUNT(*)                              AS pitches,
    ROUND(AVG(release_speed)::numeric, 1) AS avg_velo,
    ROUND(AVG(launch_speed)::numeric, 1)  AS avg_exit_velo,
    ROUND(AVG(launch_angle)::numeric, 1)  AS avg_launch_angle
  FROM batted_balls
  WHERE pitch_name IS NOT NULL
  GROUP BY pitch_name
  ORDER BY pitches DESC
")

# -------------------------
# 7. Spray zone distribution
# -------------------------
dbGetQuery(con, "
  SELECT
    spray_zone,
    COUNT(*)                              AS count,
    ROUND(AVG(launch_speed)::numeric, 1)  AS avg_exit_velo
  FROM batted_balls
  WHERE spray_zone IS NOT NULL
  GROUP BY spray_zone
  ORDER BY count DESC
")

# -------------------------
# 8. Hard hit balls: full detail
# -------------------------
dbGetQuery(con, "
  SELECT
    game_date,
    player_name,
    pitch_name,
    launch_speed,
    launch_angle,
    hit_distance_sc,
    spray_zone,
    bb_type,
    events
  FROM batted_balls
  WHERE hard_hit = TRUE
  ORDER BY launch_speed DESC
")

# -------------------------
# Disconnect
# -------------------------
dbDisconnect(con)
