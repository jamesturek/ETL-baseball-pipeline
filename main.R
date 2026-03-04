message("========================================")
message("  CWS Baseball ETL Pipeline")
message("  Run time: ", Sys.time())
message("========================================")

start_time <- Sys.time()

# -------------------------
# Step 1: Extract
# -------------------------
message("\n--- STEP 1: EXTRACT ---")
tryCatch(
  source("R/01_extract.R"),
  error = function(e) {
    message("EXTRACT FAILED: ", e$message)
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
      stop(e)
    }
  )

  # -------------------------
  # Step 4: Visualise
  # -------------------------
  message("\n--- STEP 4: VISUALISE ---")
  tryCatch(
    source("R/05_visualize.R"),
    error = function(e) {
      message("VISUALISE FAILED: ", e$message)
      stop(e)
    }
  )

} # end if new data

# -------------------------
# Done
# -------------------------
elapsed <- round(difftime(Sys.time(), start_time, units = "mins"), 1)
message("\n========================================")
message("  Pipeline complete in ", elapsed, " mins")
message("  ", Sys.time())
message("========================================")

source("main.R")
