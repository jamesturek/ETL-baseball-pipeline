# 05_visualize.R
# Game visuals for casual fans — clear, storytelling-focused
# Saves PNGs to outputs/visuals/{date}/
#
# Depends on: DBI, RPostgres, dplyr, ggplot2, ggrepel, GeomMLBStadiums
# Run after: 01_extract.R, 02_transform.R, 03_load.R

library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(GeomMLBStadiums)
library(ggimage)
library(stringr)
# ── Connection ─────────────────────────────────────────────────────────────────

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
cws_players  <- dbGetQuery(con, "SELECT full_name FROM players WHERE team_id = 145")
pitches_raw  <- dbGetQuery(con, "SELECT * FROM pitches")

dbDisconnect(con)

# ── Setup ──────────────────────────────────────────────────────────────────────

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

stadium_id <- if (latest_game$cws_home && latest_game$game_type == "R") {
  "white_soxs"
} else {
  "generic"
}

cws_name_list <- cws_players$full_name

# ── Helpers ────────────────────────────────────────────────────────────────────

field_layers <- function(stadium) {
  outfield <- GeomMLBStadiums::MLBStadiumsPathData |>
    filter(team == stadium, segment == "outfield_outer") |>
    rename(hc_x = x, hc_y = y) |>
    mlbam_xy_transformation() |>
    rename(x = hc_x_, y = hc_y_)

  infield <- GeomMLBStadiums::MLBStadiumsPathData |>
    filter(team == stadium, segment == "infield_outer") |>
    rename(hc_x = x, hc_y = y) |>
    mlbam_xy_transformation() |>
    rename(x = hc_x_, y = hc_y_)

  list(
    geom_polygon(data = outfield, aes(x = x, y = y),
                 fill = "#2d6a2d", colour = NA, inherit.aes = FALSE),
    geom_polygon(data = infield, aes(x = x, y = y),
                 fill = "#b5784a", colour = NA, inherit.aes = FALSE),
    geom_mlb_stadium(
      stadium_ids              = stadium,
      stadium_segments         = "all",
      stadium_transform_coords = TRUE,
      colour                   = "#1a1a1a",
      linewidth                = 0.4
    )
  )
}

hit_colours <- scale_colour_manual(
  breaks = c("Single", "Double", "Triple", "Home Run", "Out"),
  values = c(
    "Single"   = "#2196F3",
    "Double"   = "#00C853",
    "Triple"   = "#FF9800",
    "Home Run" = "#F44336",
    "Out"      = "#B0BEC5"
  )
)

theme_spray <- function() {
  theme_void(base_size = 13) +
    theme(
      legend.position  = "bottom",
      legend.title     = element_blank(),
      legend.text      = element_text(size = 11),
      plot.title       = element_text(face = "bold", size = 15, hjust = 0.5),
      plot.subtitle    = element_text(size = 11, hjust = 0.5, colour = "grey40"),
      plot.caption     = element_text(size = 9, colour = "grey60", hjust = 0.5),
      plot.background  = element_rect(fill = "white", colour = NA),
      plot.margin      = margin(10, 40, 40, 10)
    )
}

hit_labels <- function() {
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
  )
}

add_hit_result <- function(df) {
  df |>
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
}

library(mlbplotR)
library(png)
library(RCurl)
library(ggimage)

team_id_to_espn <- c(
  "108" = "laa", "109" = "ari", "110" = "bal", "111" = "bos",
  "112" = "chc", "113" = "cin", "114" = "cle", "115" = "col",
  "116" = "det", "117" = "hou", "118" = "kc",  "119" = "lad",
  "120" = "wsh", "121" = "nym", "133" = "oak", "134" = "pit",
  "135" = "sd",  "136" = "sea", "137" = "sf",  "138" = "stl",
  "139" = "tb",  "140" = "tex", "141" = "tor", "142" = "min",
  "143" = "phi", "144" = "atl", "145" = "chw", "146" = "mia",
  "147" = "nyy", "158" = "mil"
)

logo_watermark <- function(team_id) {
  espn <- team_id_to_espn[as.character(team_id)]
  url  <- paste0("https://a.espncdn.com/i/teamlogos/mlb/500/", espn, ".png")
  annotation_custom(
    grob = grid::rasterGrob(
      png::readPNG(RCurl::getURLContent(url)),
      interpolate = TRUE,
      width       = unit(1.2, "inches"),
      height      = unit(1.2, "inches")
    ),
    xmin = Inf, xmax = Inf,
    ymin = -Inf, ymax = -Inf
  )
}

# ── Infield markers ────────────────────────────────────────────────────────────

bases <- tibble(
  label = c("Home", "1B",  "2B",  "3B"),
  hc_x  = c(125,    153,   124,    97),
  hc_y  = c(208,    172,   135,   172)
) |>
  mlbam_xy_transformation() |>
  select(label, hc_x = hc_x_, hc_y = hc_y_)

diamond_path <- bases[c(1, 2, 3, 4), ]

infield_markers <- list(
  geom_polygon(
    data        = diamond_path,
    aes(x = hc_x, y = hc_y),
    inherit.aes = FALSE,
    fill        = NA,
    colour      = "white",
    linewidth   = 0.6,
    linetype    = "dashed"
  ),
  geom_point(
    data        = bases |> filter(label != "Home"),
    aes(x = hc_x, y = hc_y),
    inherit.aes = FALSE,
    shape       = 22,
    size        = 5,
    fill        = "white",
    colour      = "white"
  ),
  geom_point(
    data        = bases |> filter(label == "Home"),
    aes(x = hc_x, y = hc_y),
    inherit.aes = FALSE,
    shape       = 23,
    size        = 3.5,
    fill        = "white",
    colour      = "white"
  )
)

# ── 1. CWS spray chart ─────────────────────────────────────────────────────────

message("Generating CWS spray chart...")

cws_batted <- batted_balls |>
  filter(
    !is.na(hc_x), !is.na(hc_y),
    home_team == "CWS" & inning_topbot == "Bot" |
    away_team == "CWS" & inning_topbot == "Top"
  ) |>
  add_hit_result() |>
  filter(hit_result != "Other") |>
  mlbam_xy_transformation() |>
  select(-hc_x, -hc_y) |>
  rename(hc_x = hc_x_, hc_y = hc_y_)

p1 <- ggplot(cws_batted, aes(x = hc_x, y = hc_y, colour = hit_result)) +
  field_layers(stadium_id) +
  infield_markers +
  geom_spraychart(
  stadium_ids              = stadium_id,
  stadium_transform_coords = TRUE,
  size                     = 4.8,
  alpha                    = 1,
  colour                   = "black"
) +
geom_spraychart(
  stadium_ids              = stadium_id,
  stadium_transform_coords = TRUE,
  size                     = 4,
  alpha                    = 0.85
) +
  hit_labels() +
  hit_colours +
  logo_watermark(145) +
  coord_fixed(clip = "off") +
  theme_spray() +
  labs(
    title    = "Where Did the White Sox Hit the Ball?",
    subtitle = game_title,
    caption  = "Each dot = one batted ball. Labelled dots = extra base hits."
  )

save_plot(p1, "01_cws_spray_chart.png")

# ── 2. Opponent spray chart ────────────────────────────────────────────────────

message("Generating opponent spray chart...")

opp_batted <- batted_balls |>
  filter(
    !is.na(hc_x), !is.na(hc_y),
    home_team == "CWS" & inning_topbot == "Top" |
    away_team == "CWS" & inning_topbot == "Bot"
  ) |>
  add_hit_result() |>
  filter(hit_result != "Other") |>
  mlbam_xy_transformation() |>
  select(-hc_x, -hc_y) |>
  rename(hc_x = hc_x_, hc_y = hc_y_)

opp_id <- if (latest_game$cws_home) latest_game$away_team_id else latest_game$home_team_id

p2 <- ggplot(opp_batted, aes(x = hc_x, y = hc_y, colour = hit_result)) +
  field_layers(stadium_id) +
  infield_markers +
  geom_spraychart(
  stadium_ids              = stadium_id,
  stadium_transform_coords = TRUE,
  size                     = 4.8,
  alpha                    = 1,
  colour                   = "black"
) +
geom_spraychart(
  stadium_ids              = stadium_id,
  stadium_transform_coords = TRUE,
  size                     = 4,
  alpha                    = 0.85
) +
  hit_labels() +
  hit_colours +
  logo_watermark(opp_id) +
  coord_fixed(clip = "off") +
  theme_spray() +
  labs(
    title    = paste0("Where Did ", latest_game$opponent, " Hit the Ball?"),
    subtitle = game_title,
    caption  = "Each dot = one batted ball. Labelled dots = extra base hits."
  )

save_plot(p2, "02_opponent_spray_chart.png")

# ── 3. Exit velo vs launch angle ───────────────────────────────────────────────

message("Generating exit velo vs launch angle scatter...")

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
      TRUE                                        ~ "Opponent Batter"
    )
  ) |>
  ggplot(aes(x = launch_angle, y = launch_speed, colour = bb_type, shape = team_side)) +
  annotate("rect",
           xmin = 10, xmax = 50, ymin = 95, ymax = Inf,
           fill = "#FFF9C4", alpha = 0.5) +
  geom_point(alpha = 0.75, size = 3) +
  geom_vline(xintercept = c(10, 50),
             linetype = "dashed", colour = "grey70", linewidth = 0.4) +
  geom_hline(yintercept = 95,
             linetype = "dashed", colour = "grey70", linewidth = 0.4) +
  geom_text(
    data        = quadrant_labels,
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

# ── 4. Exit velo distribution ──────────────────────────────────────────────────

message("Generating exit velo distribution...")

ev_data <- batted_balls |>
  filter(!is.na(launch_speed)) |>
  mutate(
    team_side = case_when(
      home_team == "CWS" & inning_topbot == "Bot" ~ "White Sox",
      away_team == "CWS" & inning_topbot == "Top" ~ "White Sox",
      TRUE                                        ~ latest_game$opponent
    )
  )

ev_means <- ev_data |>
  group_by(team_side) |>
  summarise(mean_ev = round(mean(launch_speed), 1), .groups = "drop") |>
  arrange(mean_ev) |>
  mutate(label_y = c(0.021, 0.028))

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
    aes(x = mean_ev, y = label_y,
        label  = paste0(team_side, "\navg: ", mean_ev, " mph"),
        colour = team_side),
    inherit.aes   = FALSE,
    size          = 3.8,
    fontface      = "bold",
    fill          = "white",
    label.size    = 0.3,
    label.padding = unit(0.3, "lines")
  ) +
  geom_vline(xintercept = 95,
             linetype = "dotted", colour = "grey60", linewidth = 0.6) +
  annotate("text", x = 95, y = 0.025,
           label    = "Hard hit\nzone (95+ mph)",
           size     = 3.2,
           colour   = "grey50",
           fontface = "italic",
           hjust    = -0.1) +
  scale_fill_manual(values   = c("White Sox" = "#27251F",
                                 setNames("#AAAAAA", latest_game$opponent))) +
  scale_colour_manual(values = c("White Sox" = "#27251F",
                                 setNames("#888888", latest_game$opponent))) +
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

# ── 5. Hard hit leaderboard ────────────────────────────────────────────────────

message("Generating hard hit rate leaderboard...")

format_name <- function(n) {
  parts <- strsplit(n, ", ")[[1]]
  if (length(parts) == 2) paste(parts[2], parts[1]) else n
}

leaderboard <- batted_balls |>
  filter(!is.na(launch_speed)) |>
  group_by(player_name, batter) |>
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
    display_name  = sapply(player_name, format_name),
    display_name  = reorder(display_name, hard_hit_pct),
    team_side     = if_else(
      sapply(player_name, format_name) %in% cws_name_list,
      "White Sox",
      latest_game$opponent
    ),
    headshot_url  = paste0(
      "https://img.mlbstatic.com/mlb-photos/image/upload/",
      "d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/",
      batter, "/headshot/67/current"
    )
  )

p5 <- ggplot(leaderboard,
             aes(x = hard_hit_pct, y = display_name, fill = team_side)) +
  geom_col(alpha = 0.9, width = 0.7) +
  geom_text(
    aes(label = paste0(round(hard_hit_pct), "% (",
                       hard_hit_count, "/", batted_balls_n, ")")),
    hjust  = -0.1,
    size   = 3.5,
    colour = "grey30"
  ) +
  geom_image(
    aes(x = -8, image = headshot_url),
    size   = 0.034,
    asp    = 1.25
  ) +
  scale_fill_manual(values = c(
    "White Sox" = "#27251F",
    setNames("#C4CED4", latest_game$opponent)
  )) +
  scale_x_continuous(
    limits = c(-18, 105),
    expand = expansion(mult = c(0, 0)),
    breaks = c(0, 25, 50, 75, 100),
    labels = c(0, 25, 50, 75, 100)
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
    axis.text.y        = element_text(size = 11, margin = margin(r = 30)),
    plot.background    = element_rect(fill = "white", colour = NA),
    plot.margin        = margin(10, 20, 10, 20)
  ) +
  labs(
    title    = "Who Made the Hardest Contact?",
    subtitle = game_title,
    caption  = "Hard hit = exit velocity 95 mph or above. Minimum 2 balls in play.",
    x        = "Hard Hit Rate (%)",
    y        = NULL
  )

save_plot(p5, "05_hard_hit_leaderboard.png", width = 11, height = 9)

# ── 6. Win probability chart ───────────────────────────────────────────────────

message("Generating win probability chart...")

wp_data <- pitches_raw |>
  arrange(inning, inning_topbot, at_bat_number, pitch_number) |>
  mutate(
    cws_win_exp = if_else(home_team == "CWS", home_win_exp, 1 - home_win_exp),
    pitch_seq   = row_number()
  )

# Only annotate plays where the lead changes hands
scoring_plays <- wp_data |>
  filter(
    !is.na(events),
    events %in% c("single", "double", "triple", "home_run",
                  "sac_fly", "fielders_choice", "grounded_into_double_play"),
    abs(delta_run_exp) >= 0.15,
    cws_win_exp >= 0.05
  ) |>
  mutate(
    # Determine if this play helped CWS or the opponent
    cws_batting = (home_team == "CWS" & inning_topbot == "Bot") |
                  (away_team == "CWS" & inning_topbot == "Top"),
    play_favour = if_else(
      (cws_batting & delta_run_exp > 0) | (!cws_batting & delta_run_exp < 0),
      "CWS", "Opponent"
    ),
    label = case_when(
      events == "home_run"                  ~ "HR",
      events == "triple"                    ~ "3B",
      events == "double"                    ~ "2B",
      events == "single"                    ~ "1B",
      events == "sac_fly"                   ~ "SF",
      events == "fielders_choice"           ~ "FC",
      events == "grounded_into_double_play" ~ "GDP",
      TRUE                                  ~ str_to_title(events)
    )
  )

# First pitch of each half-inning for x-axis labels
inning_breaks <- wp_data |>
  group_by(inning, inning_topbot) |>
  slice_min(pitch_seq, n = 1) |>
  ungroup() |>
  mutate(inning_label = if_else(inning_topbot == "Top",
                                paste0("T", inning),
                                paste0("B", inning)))

# Two ribbon layers: green above 50%, red below 50%
ribbon_above <- wp_data |>
  mutate(x = pitch_seq, ymax = pmax(cws_win_exp, 0.5), ymin = 0.5)

ribbon_below <- wp_data |>
  mutate(x = pitch_seq, ymin = pmin(cws_win_exp, 0.5), ymax = 0.5)

p6 <- ggplot(wp_data, aes(x = pitch_seq, y = cws_win_exp)) +
  # Inning dividers
  geom_vline(
    data      = inning_breaks,
    aes(xintercept = pitch_seq),
    colour    = "grey88",
    linewidth = 0.3,
    linetype  = "dashed"
  ) +
  # Green ribbon above 50%
  geom_ribbon(
    data = ribbon_above,
    aes(x = x, ymin = ymin, ymax = ymax),
    fill  = "#2e7d32",
    alpha = 0.2,
    inherit.aes = FALSE
  ) +
  geom_ribbon(
    data = ribbon_below,
    aes(x = x, ymin = ymin, ymax = ymax),
    fill  = "#c0392b",
    alpha = 0.2,
    inherit.aes = FALSE
  ) +
  # 50% reference line
  geom_hline(yintercept = 0.5, colour = "grey50", linewidth = 0.4, linetype = "dotted") +
  # Win probability line
  geom_line(colour = "#1a1a1a", linewidth = 0.9) +
  # Lead change dots
  geom_point(
    data        = scoring_plays,
    aes(x = pitch_seq, y = cws_win_exp, colour = play_favour),
    size        = 3.5,
    show.legend = TRUE
  ) +
  # Lead change labels
  ggrepel::geom_label_repel(
    data          = scoring_plays,
    aes(x = pitch_seq, y = cws_win_exp, label = label, colour = play_favour),
    size          = 3.2,
    fontface      = "bold",
    fill          = "white",
    label.size    = 0.25,
    label.padding = unit(0.2, "lines"),
    min.segment.length = 0.2,
    box.padding   = 0.4,
    max.overlaps  = Inf,
    show.legend   = FALSE
  ) +
  scale_colour_manual(
    values = c("CWS" = "#2e7d32", "Opponent" = "#c0392b"),
    name   = NULL
  ) +
  scale_x_continuous(
    breaks = inning_breaks$pitch_seq,
    labels = inning_breaks$inning_label
  ) +
  scale_y_continuous(
    limits = c(0, 1),
    labels = scales::percent_format(accuracy = 1)
  ) +
  labs(
    title    = paste0("Win Probability: CWS vs ", latest_game$opponent),
    subtitle = game_title,
    x        = "Inning",
    y        = "CWS Win Probability",
    caption  = "HR = home run, 2B/3B = double/triple, 1B = single, SF = sac fly, FC = fielders choice, GDP = grounded into double play.\nGreen = CWS scoring play, red = opponent (|delta run exp| >= 0.15, garbage time excluded). Data: Baseball Savant."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(size = 11, colour = "grey40"),
    plot.caption       = element_text(size = 9, colour = "grey60"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    axis.text.x        = element_text(size = 8, colour = "grey50"),
    plot.background    = element_rect(fill = "white", colour = NA)
  )

save_plot(p6, "06_win_probability.png")


# ── 7. Pitch speed by pitcher ──────────────────────────────────────────────────

message("Generating pitch speed by pitcher...")

pitch_speed_data <- pitches_raw |>
  filter(!is.na(release_speed), !is.na(pitch_name)) |>
  left_join(pitcher_names, by = "pitcher") |>
  mutate(
    team_side = case_when(
      home_team == "CWS" & inning_topbot == "Bot" ~ latest_game$opponent,
      home_team == "CWS" & inning_topbot == "Top" ~ "CWS",
      away_team == "CWS" & inning_topbot == "Top" ~ latest_game$opponent,
      away_team == "CWS" & inning_topbot == "Bot" ~ "CWS"
    )
  ) |>
  group_by(pitcher_name, team_side, pitch_name) |>
  summarise(
    avg_speed = round(mean(release_speed), 1),
    n_pitches = n(),
    .groups   = "drop"
  ) |>
  filter(n_pitches >= 3) |>
  left_join(
    # fastball velo for ordering
    pitches_raw |>
      left_join(pitcher_names, by = "pitcher") |>
      filter(pitch_type %in% c("FF", "SI")) |>
      group_by(pitcher_name) |>
      summarise(fb_velo = mean(release_speed, na.rm = TRUE), .groups = "drop"),
    by = "pitcher_name"
  ) |>
  mutate(pitcher_name = reorder(pitcher_name, fb_velo))

p7 <- ggplot(pitch_speed_data,
             aes(x = avg_speed, y = pitcher_name, colour = pitch_name, size = n_pitches)) +
  geom_point(alpha = 0.85) +
  ggrepel::geom_text_repel(
    aes(label = paste0(avg_speed, " mph")),
    size            = 3.0,
    colour          = "grey15",
    fontface        = "bold",
    show.legend     = FALSE,
    box.padding     = 0.35,
    point.padding   = 0.3,
    min.segment.length = 0.3,
    max.overlaps    = Inf
  ) +
  facet_wrap(~ team_side, scales = "free_y", ncol = 1) +
  scale_size_continuous(range = c(3, 8), name = "Pitches thrown") +
  scale_colour_manual(
    name   = "Pitch type",
    values = c(
      "4-Seam Fastball" = "#E63946",
      "Sinker"          = "#F4A261",
      "Cutter"          = "#E76F51",
      "Changeup"        = "#2A9D8F",
      "Curveball"       = "#457B9D",
      "Slider"          = "#9B5DE5",
      "Sweeper"         = "#F72585",
      "Knuckle Curve"   = "#3A86FF",
      "Splitter"        = "#06D6A0"
    )
  ) +
  scale_x_continuous(
    limits = c(70, 100),
    breaks = seq(70, 100, 5),
    labels = paste0(seq(70, 100, 5), " mph")
  ) +
  labs(
    title    = "Average Pitch Speed by Pitcher and Pitch Type",
    subtitle = game_title,
    x        = NULL,
    y        = NULL,
    caption  = "Dot size = number of pitches thrown. Minimum 3 pitches per type shown. Data: Baseball Savant."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title       = element_text(face = "bold", size = 15),
    plot.subtitle    = element_text(size = 11, colour = "grey40"),
    plot.caption     = element_text(size = 9, colour = "grey60"),
    panel.grid.minor = element_blank(),
    panel.grid.major.y = element_line(colour = "grey92"),
    strip.text       = element_text(face = "bold", size = 12),
    legend.position  = "bottom",
    plot.background  = element_rect(fill = "white", colour = NA)
  )

save_plot(p7, "07_pitch_speed.png", width = 11, height = 10)


# ── 8. Best and worst plate appearances ────────────────────────────────────────

message("Generating best and worst plate appearances...")

pa_data <- pitches_raw |>
  filter(!is.na(events), !is.na(delta_run_exp)) |>
  mutate(
    cws_win_exp = if_else(home_team == "CWS", home_win_exp, 1 - home_win_exp),
    cws_batting = (home_team == "CWS" & inning_topbot == "Bot") |
                  (away_team == "CWS" & inning_topbot == "Top"),
    # Positive delta_run_exp for batting team = good for batter
    cws_delta = if_else(cws_batting, delta_run_exp, -delta_run_exp),
    batter_name = sapply(player_name, format_name),
    inning_label = paste0(if_else(inning_topbot == "Top", "T", "B"), inning),
    event_label = case_when(
      events == "home_run" ~ "HR",
      events == "triple"   ~ "3B",
      events == "double"   ~ "2B",
      events == "single"   ~ "1B",
      events == "strikeout" ~ "K",
      events == "hit_by_pitch" ~ "HBP",
      events == "walk"      ~ "BB",
      events == "sac_fly"   ~ "SF",
      events == "grounded_into_double_play" ~ "GDP",
      grepl("out", events)  ~ "Out",
      TRUE                  ~ str_to_title(events)
    )
  )

best_pa <- pa_data |>
  arrange(desc(cws_delta)) |>
  slice_head(n = 5) |>
  mutate(category = "Best White Sox Plate Appearances")

worst_pa <- pa_data |>
  arrange(cws_delta) |>
  slice_head(n = 5) |>
  mutate(category = "Most Damaging Opponent Plate Appearances")

top_pa <- bind_rows(best_pa, worst_pa) |>
  mutate(
    label      = paste0(batter_name, " (", event_label, ", ", inning_label, ")"),
    label      = reorder(label, cws_delta),
    bar_colour = if_else(cws_delta > 0, "positive", "negative")
  )

p8 <- ggplot(top_pa, aes(x = cws_delta, y = label, fill = bar_colour)) +
  geom_col(width = 0.65, alpha = 0.9) +
  geom_vline(xintercept = 0, colour = "grey40", linewidth = 0.5) +
  geom_text(
    aes(
      label = paste0(if_else(cws_delta > 0, "+", ""), round(cws_delta, 2)),
      hjust = if_else(cws_delta > 0, -0.15, 1.15)
    ),
    size     = 3.5,
    fontface = "bold",
    colour   = "grey20"
  ) +
  facet_wrap(~ category, scales = "free_y", ncol = 1) +
  scale_fill_manual(values = c("positive" = "#2e7d32", "negative" = "#c0392b")) +
  scale_x_continuous(
    limits = c(-2.5, 2.8),
    breaks = seq(-2.5, 2.5, 0.5),
    labels = function(x) ifelse(x > 0, paste0("+", x), x)
  ) +
  labs(
    title    = "Best and Worst Plate Appearances for CWS",
    subtitle = game_title,
    x        = "Run Expectancy Added (CWS perspective)",
    y        = NULL,
    caption  = "Positive = helped CWS, negative = hurt CWS. Based on change in run expectancy. Data: Baseball Savant."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(size = 11, colour = "grey40"),
    plot.caption       = element_text(size = 9, colour = "grey60"),
    panel.grid.major.y = element_blank(),
    panel.grid.minor   = element_blank(),
    strip.text         = element_text(face = "bold", size = 12),
    legend.position    = "none",
    plot.background    = element_rect(fill = "white", colour = NA),
    plot.margin        = margin(10, 30, 10, 10)
  )

save_plot(p8, "08_best_worst_pa.png", width = 11, height = 8)


# ── Done ───────────────────────────────────────────────────────────────────────

message("All visuals saved to: ", out_dir)
message("Visualise complete.")