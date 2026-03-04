# Game visuals for casual fans — clear, storytelling-focused
# Saves PNGs to outputs/visuals/{date}/

library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(GeomMLBStadiums)

# -------------------------
# Connect
# -------------------------
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "baseball_statcast",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = Sys.getenv("PGPASSWORD")
)

batted_balls <- dbGetQuery(con, "SELECT * FROM batted_balls")
games        <- dbGetQuery(con, "SELECT * FROM games")
dbDisconnect(con)

# -------------------------
# Setup
# -------------------------
run_date <- format(Sys.Date(), "%Y-%m-%d")
out_dir  <- file.path("outputs", "visuals", run_date)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
message("Saving visuals to: ", out_dir)

save_plot <- function(plot, filename, width = 10, height = 8) {
  path <- file.path(out_dir, filename)
  ggsave(path, plot, width = width, height = height, dpi = 150, bg = "white")
  message("Saved: ", filename)
}

latest_game <- games |> arrange(desc(game_date)) |> slice(1)

game_title <- paste0(
  "Chicago White Sox ", latest_game$cws_score,
  "  vs  ",
  latest_game$opponent, " ", latest_game$opp_score,
  "   |   ", format(as.Date(latest_game$game_date), "%B %d, %Y")
)

result_label <- if (latest_game$result == "W") "WIN" else if (latest_game$result == "L") "LOSS" else "TIE"

stadium_id <- if (latest_game$cws_home && latest_game$game_type == "R") {
  "white_soxs"
} else {
  "generic"
}

# -------------------------
# 1. CWS spray chart
# -------------------------
message("Generating CWS spray chart...")

cws_batted <- batted_balls |>
  filter(
    !is.na(hc_x), !is.na(hc_y),
    home_team == "CWS" & inning_topbot == "Bot" |
    away_team == "CWS" & inning_topbot == "Top"
  ) |>
  mutate(
    hit_result = case_when(
      events == "single"                  ~ "Single",
      events == "double"                  ~ "Double",
      events == "triple"                  ~ "Triple",
      events == "home_run"                ~ "Home Run",
      grepl("out|error|fielders", events) ~ "Out",
      TRUE                                ~ "Other"
    ),
    label = if_else(
      events %in% c("home_run", "triple", "double"),
      sub(",.*", "", player_name),
      NA_character_
    )
  )

p1 <- ggplot(cws_batted, aes(x = hc_x, y = hc_y, colour = hit_result)) +
  geom_mlb_stadium(
    stadium_ids              = stadium_id,
    stadium_segments         = "all",
    colour                   = "grey60",
    size                     = 0.5
  ) +
  geom_spraychart(
    stadium_ids              = stadium_id,
    size                     = 4,
    alpha                    = 0.85
  ) +
  geom_label_repel(
    aes(label = label),
    na.rm         = TRUE,
    size          = 3.5,
    fontface      = "bold",
    box.padding   = 0.4,
    colour        = "grey20",
    fill          = "white",
    label.size    = 0.3,
    show.legend   = FALSE
  ) +
  scale_colour_manual(values = c(
    "Single"   = "#2196F3",
    "Double"   = "#4CAF50",
    "Triple"   = "#FF9800",
    "Home Run" = "#F44336",
    "Out"      = "grey70",
    "Other"    = "grey85"
  )) +
  coord_fixed() +
  scale_y_reverse() +
  theme_void(base_size = 13) +
  theme(
    legend.position  = "bottom",
    legend.title     = element_blank(),
    legend.text      = element_text(size = 11),
    plot.title       = element_text(face = "bold", size = 15, hjust = 0.5),
    plot.subtitle    = element_text(size = 11, hjust = 0.5, colour = "grey40"),
    plot.caption     = element_text(size = 9, colour = "grey60", hjust = 0.5),
    plot.background  = element_rect(fill = "white", colour = NA),
    plot.margin      = margin(10, 10, 10, 10)
  ) +
  labs(
    title    = "Where Did the White Sox Hit the Ball?",
    subtitle = game_title,
    caption  = "Each dot = one batted ball. Labelled dots = extra base hits."
  )

save_plot(p1, "01_cws_spray_chart.png")

# -------------------------
# 2. Opponent spray chart
# -------------------------
message("Generating opponent spray chart...")

opp_batted <- batted_balls |>
  filter(
    !is.na(hc_x), !is.na(hc_y),
    home_team == "CWS" & inning_topbot == "Top" |
    away_team == "CWS" & inning_topbot == "Bot"
  ) |>
  mutate(
    hit_result = case_when(
      events == "single"                  ~ "Single",
      events == "double"                  ~ "Double",
      events == "triple"                  ~ "Triple",
      events == "home_run"                ~ "Home Run",
      grepl("out|error|fielders", events) ~ "Out",
      TRUE                                ~ "Other"
    ),
    label = if_else(
      events %in% c("home_run", "triple", "double"),
      sub(",.*", "", player_name),
      NA_character_
    )
  )

p2 <- ggplot(opp_batted, aes(x = hc_x, y = hc_y, colour = hit_result)) +
  geom_mlb_stadium(
    stadium_ids              = stadium_id,
    stadium_segments         = "all",
    colour                   = "grey60",
    size                     = 0.5
  ) +
  geom_spraychart(
    stadium_ids              = stadium_id,
    size                     = 4,
    alpha                    = 0.85
  ) +
  geom_label_repel(
    aes(label = label),
    na.rm         = TRUE,
    size          = 3.5,
    fontface      = "bold",
    box.padding   = 0.4,
    colour        = "grey20",
    fill          = "white",
    label.size    = 0.3,
    show.legend   = FALSE
  ) +
  scale_colour_manual(values = c(
    "Single"   = "#2196F3",
    "Double"   = "#4CAF50",
    "Triple"   = "#FF9800",
    "Home Run" = "#F44336",
    "Out"      = "grey70",
    "Other"    = "grey85"
  )) +
  coord_fixed() +
  scale_y_reverse() +
  theme_void(base_size = 13) +
  theme(
    legend.position  = "bottom",
    legend.title     = element_blank(),
    legend.text      = element_text(size = 11),
    plot.title       = element_text(face = "bold", size = 15, hjust = 0.5),
    plot.subtitle    = element_text(size = 11, hjust = 0.5, colour = "grey40"),
    plot.caption     = element_text(size = 9, colour = "grey60", hjust = 0.5),
    plot.background  = element_rect(fill = "white", colour = NA),
    plot.margin      = margin(10, 10, 10, 10)
  ) +
  labs(
    title    = paste0("Where Did ", latest_game$opponent, " Hit the Ball?"),
    subtitle = game_title,
    caption  = "Each dot = one batted ball. Labelled dots = extra base hits."
  )

save_plot(p2, "02_opponent_spray_chart.png")

# -------------------------
# 3. Exit velo vs launch angle
# -------------------------
message("Generating exit velo vs launch angle scatter...")

# Quadrant annotations for casual readers
quadrant_labels <- data.frame(
  x     = c(-35,  5,  17, 55),
  y     = c(105, 68, 113, 68),
  label = c("Hard\nGround Balls", "Weak\nContact", "Barrel\nZone", "High\nFly Balls")
)

p3 <- batted_balls |>
  filter(!is.na(bb_type)) |>
  mutate(
    bb_type   = tools::toTitleCase(gsub("_", " ", bb_type)),
    team_side = case_when(
      home_team == "CWS" & inning_topbot == "Bot" ~ "CWS Batter",
      away_team == "CWS" & inning_topbot == "Top" ~ "CWS Batter",
      TRUE ~ "Opponent Batter"
    )
  ) |>
  ggplot(aes(x = launch_angle, y = launch_speed, colour = bb_type, shape = team_side)) +
  annotate("rect", xmin = 10, xmax = 50, ymin = 95, ymax = Inf,
           fill = "#FFF9C4", alpha = 0.5) +
  geom_point(alpha = 0.75, size = 3) +
  geom_vline(xintercept = c(10, 50), linetype = "dashed",
             colour = "grey70", linewidth = 0.4) +
  geom_hline(yintercept = 95, linetype = "dashed",
             colour = "grey70", linewidth = 0.4) +
  geom_text(
    data     = quadrant_labels,
    aes(x = x, y = y, label = label),
    inherit.aes = FALSE,
    size        = 3.2,
    colour      = "grey50",
    lineheight  = 0.9,
    fontface    = "italic"
  ) +
  scale_colour_manual(values = c(
    "Fly Ball"    = "#F44336",
    "Ground Ball" = "#4CAF50",
    "Line Drive"  = "#2196F3",
    "Popup"       = "#FF9800"
  )) +
  scale_shape_manual(values = c("CWS Batter" = 16, "Opponent Batter" = 17)) +
  scale_x_continuous(breaks = seq(-60, 90, 20)) +
  scale_y_continuous(breaks = seq(60, 120, 10)) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position  = "bottom",
    legend.title     = element_blank(),
    legend.text      = element_text(size = 10),
    plot.title       = element_text(face = "bold", size = 15),
    plot.subtitle    = element_text(size = 11, colour = "grey40"),
    plot.caption     = element_text(size = 9, colour = "grey60"),
    panel.grid.minor = element_blank(),
    plot.background  = element_rect(fill = "white", colour = NA)
  ) +
  labs(
    title    = "How Hard and at What Angle Did Batters Hit the Ball?",
    subtitle = game_title,
    caption  = "Yellow zone = barrels (hardest, best-angle hits). Circles = CWS batters, triangles = opponents.",
    x        = "Launch Angle (degrees, negative = into the ground)",
    y        = "Exit Velocity (mph)"
  )

save_plot(p3, "03_exit_velo_launch_angle.png")

# -------------------------
# 4. Exit velo distribution
# -------------------------
message("Generating exit velo distribution...")

ev_data <- batted_balls |>
  filter(!is.na(launch_speed)) |>
  mutate(
    team_side = case_when(
      home_team == "CWS" & inning_topbot == "Bot" ~ "White Sox",
      away_team == "CWS" & inning_topbot == "Top" ~ "White Sox",
      TRUE ~ latest_game$opponent
    )
  )

ev_means <- ev_data |>
  group_by(team_side) |>
  summarise(mean_ev = round(mean(launch_speed), 1), .groups = "drop")

p4 <- ggplot(ev_data, aes(x = launch_speed, fill = team_side, colour = team_side)) +
  geom_density(alpha = 0.35, linewidth = 0.9) +
  geom_vline(
    data      = ev_means,
    aes(xintercept = mean_ev, colour = team_side),
    linetype  = "dashed",
    linewidth = 1
  ) +
  geom_label(
    data        = ev_means,
    aes(x = mean_ev, y = 0.028, label = paste0(team_side, "\navg: ", mean_ev, " mph"),
        colour = team_side),
    inherit.aes = FALSE,
    size        = 3.8,
    fontface    = "bold",
    fill        = "white",
    label.size  = 0.3,
    label.padding = unit(0.3, "lines")
  ) +
  scale_fill_manual(values   = c("White Sox" = "#27251F", setNames("#AAAAAA", latest_game$opponent))) +
  scale_colour_manual(values = c("White Sox" = "#27251F", setNames("#888888", latest_game$opponent))) +
  annotate("text", x = 95, y = 0.025, label = "Hard hit\nzone (95+ mph)",
           size = 3.2, colour = "grey50", fontface = "italic") +
  geom_vline(xintercept = 95, linetype = "dotted", colour = "grey60", linewidth = 0.6) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position  = "none",
    plot.title       = element_text(face = "bold", size = 15),
    plot.subtitle    = element_text(size = 11, colour = "grey40"),
    plot.caption     = element_text(size = 9, colour = "grey60"),
    panel.grid.minor = element_blank(),
    axis.title.y     = element_blank(),
    axis.text.y      = element_blank(),
    plot.background  = element_rect(fill = "white", colour = NA)
  ) +
  labs(
    title    = "How Hard Did Each Team Hit the Ball?",
    subtitle = game_title,
    caption  = "Each curve shows the spread of exit velocities. Further right = harder contact.",
    x        = "Exit Velocity (mph)"
  )

save_plot(p4, "04_exit_velo_distribution.png")

# -------------------------
# 5. Hard hit leaderboard
# -------------------------
message("Generating hard hit rate leaderboard...")

cws_roster <- batting_orders <- dbGetQuery(
  dbConnect(
    RPostgres::Postgres(),
    dbname = "baseball_statcast", host = "localhost",
    port = 5432, user = "postgres", password = Sys.getenv("PGPASSWORD")
  ),
  "SELECT full_name, team_id FROM players WHERE team_id = 145"
)

con2 <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "baseball_statcast",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = Sys.getenv("PGPASSWORD")
)
cws_players <- dbGetQuery(con2, "SELECT full_name FROM players WHERE team_id = 145")
dbDisconnect(con2)

cws_name_list <- cws_players$full_name

leaderboard <- batted_balls |>
  filter(!is.na(launch_speed)) |>
  group_by(player_name) |>
  summarise(
    batted_balls_n = n(),
    hard_hit_count = sum(hard_hit, na.rm = TRUE),
    hard_hit_pct   = hard_hit_count / batted_balls_n * 100,
    avg_ev         = round(mean(launch_speed, na.rm = TRUE), 1),
    .groups        = "drop"
  ) |>
  filter(batted_balls_n >= 2) |>
  arrange(desc(hard_hit_pct)) |>
  slice_head(n = 15) |>
  mutate(
    # Convert "Last, First" to "First Last" for readability
    display_name = sapply(player_name, function(n) {
      parts <- strsplit(n, ", ")[[1]]
      if (length(parts) == 2) paste(parts[2], parts[1]) else n
    }),
    display_name = reorder(display_name, hard_hit_pct),
    team_side    = if_else(
      sapply(player_name, function(n) {
        parts <- strsplit(n, ", ")[[1]]
        full  <- if (length(parts) == 2) paste(parts[2], parts[1]) else n
        full %in% cws_name_list
      }),
      "White Sox", latest_game$opponent
    )
  )

p5 <- ggplot(leaderboard,
             aes(x = hard_hit_pct, y = display_name, fill = team_side)) +
  geom_col(alpha = 0.9, width = 0.7) +
  geom_text(
    aes(label = paste0(round(hard_hit_pct), "% (", hard_hit_count, "/", batted_balls_n, ")")),
    hjust  = -0.1,
    size   = 3.5,
    colour = "grey30"
  ) +
  scale_fill_manual(values = c(
    "White Sox"             = "#27251F",
    setNames("#C4CED4", latest_game$opponent)
  )) +
  scale_x_continuous(
    limits = c(0, 105),
    expand = expansion(mult = c(0, 0))
  ) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position    = "bottom",
    legend.title       = element_blank(),
    legend.text        = element_text(size = 11),
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(size = 11, colour = "grey40"),
    plot.caption       = element_text(size = 9, colour = "grey60"),
    panel.grid.minor   = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.text.y        = element_text(size = 11),
    plot.background    = element_rect(fill = "white", colour = NA)
  ) +
  labs(
    title    = "Who Made the Hardest Contact?",
    subtitle = game_title,
    caption  = "Hard hit = exit velocity 95 mph or above. Minimum 2 balls in play.",
    x        = "Hard Hit Rate (%)",
    y        = NULL
  )

save_plot(p5, "05_hard_hit_leaderboard.png")

message("All visuals saved to: ", out_dir)
message("Visualise complete.")
