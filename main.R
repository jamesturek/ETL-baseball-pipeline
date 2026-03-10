message("========================================")
message("  CWS Baseball ETL Pipeline")
message("  Run time: ", Sys.time())
message("========================================")

start_time <- Sys.time()

# -------------------------
# Database connection
# -------------------------
library(DBI)
library(RPostgres)

message("\n--- CONNECTING TO DATABASE ---")
con <- dbConnect(
  RPostgres::Postgres(),
  host     = "aws-1-us-east-1.pooler.supabase.com",
  port     = 6543,
  dbname   = "postgres",
  user     = "postgres.phvritbiwlcsjxqhizpt",
  password = Sys.getenv("SUPA_PASSWORD")
)
message("Connected to database.")

# -------------------------
# Step 1: Extract
# -------------------------
message("\n--- STEP 1: EXTRACT ---")
tryCatch(
  source("R/01_extract.R"),
  error = function(e) {
    message("EXTRACT FAILED: ", e$message)
    dbDisconnect(con)
    stop(e)
  }
)

pipeline_status <- readLines("data/raw/pipeline_status.txt")

if (pipeline_status == "skip") {

  message("\nNo new games since last run. Skipping transform, load, and visualise.")

} else {

  # -------------------------
  # Step 2: Transform
  # -------------------------
  message("\n--- STEP 2: TRANSFORM ---")
  tryCatch(
    source("R/02_transform.R"),
    error = function(e) {
      message("TRANSFORM FAILED: ", e$message)
      dbDisconnect(con)
      stop(e)
    }
  )

  # -------------------------
  # Step 3: Load
  # -------------------------
  message("\n--- STEP 3: LOAD ---")
  tryCatch(
    source("R/03_load.R"),
    error = function(e) {
      message("LOAD FAILED: ", e$message)
      dbDisconnect(con)
      stop(e)
    }
  )

  # -------------------------
  # Step 4: Visualise
  # -------------------------
  message("\n--- STEP 4: VISUALISE ---")

  run_date <- format(Sys.Date(), "%Y-%m-%d")
  out_dir  <- file.path("outputs", "visuals", run_date)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  message("Saving visuals to: ", out_dir)

  tryCatch(
    source("R/05_visualize.R"),
    error = function(e) {
      message("VISUALISE (spray charts) FAILED: ", e$message)
      dbDisconnect(con)
      stop(e)
    }
  )

  tryCatch(
    source("R/06_boxscore.R"),
    error = function(e) {
      message("VISUALISE (box score) FAILED: ", e$message)
      dbDisconnect(con)
      stop(e)
    }
  )

  tryCatch(
    source("R/07_strike_zone.R"),
    error = function(e) {
      message("VISUALISE (strike zone) FAILED: ", e$message)
      dbDisconnect(con)
      stop(e)
    }
  )

  message("All visuals complete.")

} # end if new data

# -------------------------
# Done
# -------------------------
dbDisconnect(con)
message("Database connection closed.")

elapsed <- round(difftime(Sys.time(), start_time, units = "mins"), 1)
message("\n========================================")
message("  Pipeline complete in ", elapsed, " mins")
message("  ", Sys.time())
message("========================================")