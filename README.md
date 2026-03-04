# ETL Baseball Pipeline

An incremental ETL pipeline in R that pulls Chicago White Sox Statcast and game data after every game, loads it into a PostgreSQL database, and automatically generates game visuals.

Built as a portfolio project demonstrating end-to-end data engineering with R, SQL, and geospatial visualisation.

---

## Project Structure

```
ETL-baseball-pipeline/
├── R/
│   ├── 01_extract.R       # Pull data from Baseball Savant + MLB API
│   ├── 02_transform.R     # Clean and reshape into analysis-ready tables
│   ├── 03_load.R          # Load into PostgreSQL with deduplication
│   ├── 04_queries.R       # Example SQL queries for exploration
│   └── 05_visualize.R     # Auto-generate game visuals
├── main.R                 # Orchestrates the full pipeline
├── data/
│   ├── raw/               # Raw RDS extracts
│   └── processed/         # Cleaned, typed tables
└── outputs/
    └── visuals/           # PNGs saved by game date
```

---

## Pipeline Architecture

```
Baseball Savant (Statcast)          MLB Stats API
        |                                |
   01_extract.R  <----------------------->
        |
   data/raw/*.rds
        |
   02_transform.R
        |
   data/processed/*.rds
        |
   03_load.R
        |
   PostgreSQL (baseball_statcast)
        |
   05_visualize.R
        |
   outputs/visuals/{date}/
```

The pipeline is **incremental**: on the first run it pulls all games from the start of the season. On every subsequent run it checks the most recent `game_date` already in the database and pulls only new games, so re-running never creates duplicates.

---

## Database Schema

Four tables in PostgreSQL, linked by `game_pk` and `player_id`:

### `games`
One row per game. Includes venue, weather, attendance, final score, and a CWS win/loss result flag.

| Column | Type | Description |
|---|---|---|
| game_pk | BIGINT (PK) | MLB game identifier |
| game_date | DATE | Date of game |
| venue_name | TEXT | Stadium name |
| temperature | INTEGER | Game-time temperature (F) |
| attendance | INTEGER | Paid attendance |
| cws_home | BOOLEAN | TRUE if CWS were home team |
| cws_score | INTEGER | CWS final score |
| opp_score | INTEGER | Opponent final score |
| result | TEXT | W / L / T |

### `players`
One row per player (both teams). Sourced from batting orders.

| Column | Type | Description |
|---|---|---|
| player_id | INTEGER (PK) | MLB player identifier |
| full_name | TEXT | Player full name |
| position | TEXT | Fielding position |
| team | TEXT | Team name |
| team_id | INTEGER | MLB team identifier |

### `linescore`
One row per inning per game. Inning-by-inning runs, hits, and errors for both teams.

| Column | Type | Description |
|---|---|---|
| game_pk | BIGINT (PK) | MLB game identifier |
| inning | INTEGER (PK) | Inning number |
| home_runs | INTEGER | Runs scored by home team |
| away_runs | INTEGER | Runs scored by away team |
| home_hits | INTEGER | Hits by home team |
| away_hits | INTEGER | Hits by away team |

### `batted_balls`
One row per batted ball event (both teams). Core Statcast metrics including exit velocity, launch angle, spray coordinates, and derived fields.

| Column | Type | Description |
|---|---|---|
| game_pk | BIGINT (PK) | MLB game identifier |
| at_bat_number | INTEGER (PK) | At-bat sequence number |
| pitch_number | INTEGER (PK) | Pitch sequence within at-bat |
| batter | BIGINT | Batter MLB player ID |
| pitcher | BIGINT | Pitcher MLB player ID |
| launch_speed | NUMERIC | Exit velocity (mph) |
| launch_angle | NUMERIC | Launch angle (degrees) |
| hit_distance_sc | NUMERIC | Projected hit distance (ft) |
| hc_x / hc_y | NUMERIC | Hit coordinates for spray charts |
| spray_angle | NUMERIC | Derived spray angle |
| spray_zone | TEXT | pull / center / oppo |
| hard_hit | BOOLEAN | Exit velocity >= 95 mph |
| barrel | BOOLEAN | Barrel per Statcast definition |
| bb_type | TEXT | fly_ball / ground_ball / line_drive / popup |
| estimated_ba_using_speedangle | NUMERIC | Expected batting average (xBA) |

---

## Data Sources

| Source | Method | Data |
|---|---|---|
| [Baseball Savant](https://baseballsavant.mlb.com/) | `baseballr::statcast_search()` | Statcast batted ball events |
| [MLB Stats API](https://statsapi.mlb.com/) | `baseballr::mlb_schedule()`, `mlb_game_info()`, `mlb_game_linescore()`, `mlb_batting_orders()` | Schedule, game info, linescores, batting orders |

---

## Visualisations

Five plots are auto-generated after each pipeline run and saved to `outputs/visuals/{date}/`:

| File | Description |
|---|---|
| `01_cws_spray_chart.png` | CWS batted ball locations on a baseball diamond |
| `02_opponent_spray_chart.png` | Opponent batted ball locations |
| `03_exit_velo_launch_angle.png` | Exit velocity vs launch angle scatter with barrel zone |
| `04_exit_velo_distribution.png` | Exit velocity density curves, CWS vs opponent |
| `05_hard_hit_leaderboard.png` | Hard hit rate leaderboard (min. 2 balls in play) |

Spray charts use the [`GeomMLBStadiums`](https://github.com/bdilday/GeomMLBStadiums) package. During spring training the generic stadium diagram is used; once the regular season starts, home games automatically render on Guaranteed Rate Field.

---

## Example Queries

```sql
-- CWS season record so far
SELECT result, COUNT(*) AS games
FROM games
GROUP BY result;

-- Hardest hit balls of the season
SELECT game_date, player_name, launch_speed, launch_angle,
       hit_distance_sc, events
FROM batted_balls
WHERE hard_hit = TRUE
ORDER BY launch_speed DESC
LIMIT 10;

-- Average exit velocity by pitcher faced
SELECT pitcher, AVG(launch_speed) AS avg_ev,
       COUNT(*) AS batted_balls
FROM batted_balls
GROUP BY pitcher
HAVING COUNT(*) >= 5
ORDER BY avg_ev DESC;

-- Inning-by-inning scoring across the season
SELECT g.game_date, g.opponent, l.inning_label,
       l.home_runs, l.away_runs
FROM linescore l
JOIN games g ON l.game_pk = g.game_pk
ORDER BY g.game_date, l.inning;
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| R | Core language for ETL and visualisation |
| baseballr | Statcast and MLB API data extraction |
| dplyr / lubridate / janitor | Data transformation |
| DBI / RPostgres | PostgreSQL connection and loading |
| ggplot2 / ggrepel | Visualisation |
| GeomMLBStadiums | Baseball diamond geometry for spray charts |
| PostgreSQL 18 | Database storage |

---

## Setup

**Requirements:** R 4.1+, PostgreSQL 18, Rtools 4.5

**1. Clone the repo and open the RStudio/Positron project**

**2. Install R packages**
```r
install.packages(c("baseballr", "dplyr", "lubridate", "janitor",
                   "DBI", "RPostgres", "ggplot2", "ggrepel", "glue"))
devtools::install_github("bdilday/GeomMLBStadiums")
```

**3. Create the PostgreSQL database**
```bash
psql -U postgres -c "CREATE DATABASE baseball_statcast;"
```

**4. Add your PostgreSQL password to `.Renviron`**
```
PGPASSWORD=yourpasswordhere
```

**5. Run the pipeline**
```r
source("main.R")
```

On the first run the full season to date is pulled. After that, run `source("main.R")` after any CWS game to incrementally update the database and regenerate visuals.

---

## Automated Scheduling

The pipeline can be scheduled via Windows Task Scheduler to run overnight automatically after game days. See `main.R` for the full orchestration logic.

---

## Author

James Turek | [jamesturek.github.io](https://jamesturek.github.io)

MSc Geographic Data Science, London School of Economics
