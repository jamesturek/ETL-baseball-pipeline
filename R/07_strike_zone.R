# 07_strike_zone.R
# Strike zone heatmap plots per pitcher, CWS and opponent
# Saves PNGs to outputs/visuals/{date}/
#
# Depends on: DBI, RPostgres, dplyr, ggplot2, ggrepel, purrr, baseballr
# Run after: 01_extract.R, 02_transform.R, 03_load.R

library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(purrr)
library(baseballr)

# ── Connection ─────────────────────────────────────────────────────────────────

con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "baseball_statcast",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = Sys.getenv("PGPASSWORD")
)

pitches <- dbGetQuery(con, "SELECT * FROM pitches")
games   <- dbGetQuery(con, "SELECT * FROM games")

dbDisconnect(con)

# ── Setup ──────────────────────────────────────────────────────────────────────

run_date <- format(Sys.Date(), "%Y-%m-%d")
out_dir  <- file.path("outputs", "visuals", run_date)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

latest_game <- games |> arrange(desc(game_date)) |> slice(1)

game_title <- paste0(
  "Chicago White Sox ", latest_game$cws_score,
  "  vs  ",
  latest_game$opponent, " ", latest_game$opp_score,
  "   |   ", format(as.Date(latest_game$game_date), "%B %d, %Y")
)

save_plot <- function(plot, filename, width = 10, height = 10) {
  path <- file.path(out_dir, filename)
  ggsave(path, plot, width = width, height = height, dpi = 150, bg = "white")
  message("Saved: ", filename)
}

# ── Pitcher name lookup ────────────────────────────────────────────────────────

pitcher_ids <- pitches |> pull(pitcher) |> unique()

pitcher_names <- map_dfr(pitcher_ids, function(id) {
  tryCatch({
    p <- mlb_people(person_ids = id)
    tibble(pitcher = id, pitcher_name = p$full_name[1])
  }, error = function(e) {
    tibble(pitcher = id, pitcher_name = NA_character_)
  })
})

pitches <- pitches |>
  left_join(pitcher_names, by = "pitcher")

# ── Pitch type colours ─────────────────────────────────────────────────────────

pitch_colours <- scale_colour_manual(
  name   = "Pitch Type",
  values = c(
    "4-Seam Fastball" = "#E63946",
    "Sinker"          = "#FF6B6B",
    "Cutter"          = "#FF9F1C",
    "Slider"          = "#2196F3",
    "Sweeper"         = "#9B5DE5",
    "Curveball"       = "#00B4D8",
    "Slurve"          = "#7B2FBE",
    "Changeup"        = "#2DC653",
    "Split-Finger"    = "#0A7B3E"
  )
)

# ── Strike zone box ────────────────────────────────────────────────────────────

strike_zone <- annotate(
  "rect",
  xmin = -0.83, xmax = 0.83,
  ymin = 1.5,   ymax = 3.5,
  fill      = NA,
  colour    = "black",
  linewidth = 0.8
)

# ── Theme ──────────────────────────────────────────────────────────────────────

theme_zone <- function() {
  theme_minimal(base_size = 13) +
    theme(
      legend.position     = "bottom",
      legend.box          = "vertical",
      legend.spacing.y    = unit(0.2, "cm"),
      legend.key.size     = unit(0.5, "cm"),
      legend.margin       = margin(t = 5),
      legend.title        = element_text(face = "bold", size = 10),
      legend.text         = element_text(size = 10),
      plot.title          = element_text(face = "bold", size = 15, hjust = 0.5),
      plot.subtitle       = element_text(size = 11, hjust = 0.5, colour = "grey40"),
      plot.caption        = element_text(size = 9, colour = "grey60", hjust = 0.5),
      panel.grid.minor    = element_blank(),
      panel.grid.major    = element_line(colour = "grey90"),
      plot.background     = element_rect(fill = "white", colour = NA),
      axis.title          = element_text(size = 11)
    )
}

# ── Build plot function ────────────────────────────────────────────────────────

make_zone_plot <- function(team_side, team_label, filename_prefix) {

  if (team_side == "CWS") {
    plot_pitches <- pitches |>
      filter(
        home_team == "CWS" & inning_topbot == "Top" |
        away_team == "CWS" & inning_topbot == "Bot"
      )
  } else {
    plot_pitches <- pitches |>
      filter(
        home_team == "CWS" & inning_topbot == "Bot" |
        away_team == "CWS" & inning_topbot == "Top"
      )
  }

  plot_pitches <- plot_pitches |>
    filter(!is.na(plate_x), !is.na(plate_z), !is.na(pitch_name))

  pitcher_list <- unique(plot_pitches$pitcher_name)

  walk(pitcher_list, function(pname) {

    pitcher_pitches <- plot_pitches |>
      filter(pitcher_name == pname)

    n_pitches <- nrow(pitcher_pitches)

    pitcher_pitches <- pitcher_pitches |>
      filter(pitch_result %in% c("Ball", "Strike"))

    elite <- pitcher_pitches |>
      filter(elite_velo == TRUE) |>
      mutate(velo_label = paste0(round(release_speed, 1), " mph"))

    safe_name <- gsub("[^a-zA-Z0-9]", "_", pname)
    filename  <- paste0(filename_prefix, "_", safe_name, ".png")

    p <- ggplot(pitcher_pitches, aes(x = plate_x, y = plate_z)) +
      annotate("rect",
               xmin = -2.5, xmax = 2.5, ymin = 0, ymax = 5,
               fill = "grey97", colour = NA) +
      stat_density_2d(
        aes(fill = after_stat(level)),
        geom    = "polygon",
        contour = TRUE,
        bins    = 8,
        alpha   = 0.45
      ) +
      scale_fill_gradient(
        low   = "#ffffcc",
        high  = "#d73027",
        name  = "Density",
        guide = "none"
      ) +
      strike_zone +
      # Black outline layer
      geom_point(
        aes(shape = pitch_result),
        colour = "black",
        size   = 5.5,
        alpha  = 0.9,
        stroke = 1.2
      ) +
      # Coloured layer on top
      geom_point(
        aes(colour = pitch_name, shape = pitch_result),
        size   = 3.8,
        alpha  = 0.95,
        stroke = 0.8
      ) +
      # Elite velo labels
      geom_label_repel(
        data        = elite,
        aes(label   = paste0(round(release_speed, 1), " mph")),
        size        = 3.2,
        fontface    = "bold",
        colour      = "black",
        fill        = "white",
        label.size  = 0.3,
        box.padding = 0.5,
        show.legend = FALSE
      ) +
      scale_shape_manual(
        name   = "Result",
        values = c(
          "Ball"   = 1,
          "Strike" = 4
        )
      ) +
      pitch_colours +
      guides(
        colour = guide_legend(
          order        = 1,
          nrow         = 2,
          title        = "Pitch Type",
          title.theme  = element_text(face = "bold", size = 10),
          override.aes = list(size = 4, shape = 16)
        ),
        shape = guide_legend(
          order        = 2,
          nrow         = 1,
          title        = "Result",
          title.theme  = element_text(face = "bold", size = 10),
          override.aes = list(colour = "black", size = 4)
        )
      ) +
      scale_x_reverse() +
      coord_fixed(xlim = c(2.5, -2.5), ylim = c(0, 5)) +
      theme_zone() +
      labs(
        title    = pname,
        subtitle = paste0(team_label, "  |  ", game_title),
        caption  = paste0(n_pitches, " pitches. Rectangle = strike zone. Labels = 99+ mph fastballs."),
        x        = "Horizontal Position (ft, pitcher's perspective: glove side left)",
        y        = "Height (ft)"
      )

    save_plot(p, filename)
  })
}

# ── Generate plots ─────────────────────────────────────────────────────────────

message("Generating CWS pitcher strike zone plots...")
make_zone_plot("CWS", "Chicago White Sox", "07_cws")

message("Generating opponent pitcher strike zone plots...")
make_zone_plot("OPP", latest_game$opponent, "07_opp")

message("Strike zone plots complete.")
