library(ggplot2)
library(dplyr)
library(ggrepel)

# ── Pitch arsenal breakdown ────────────────────────────────────────────────────

arsenal <- smith_pitches |>
  filter(pitch_name != "", game_type == "R") |>
  group_by(pitch_name, pitch_type) |>
  summarise(
    n          = n(),
    avg_velo   = round(mean(release_speed, na.rm = TRUE), 1),
    avg_spin   = round(mean(release_spin_rate, na.rm = TRUE), 0),
    whiff_rate = round(
  sum(description == "swinging_strike") / 
  sum(description %in% c("swinging_strike", "foul", "hit_into_play", "foul_tip")) * 100, 1
),
    usage_pct  = n / nrow(filter(smith_pitches, pitch_name != "", game_type == "R")) * 100,
    .groups    = "drop"
  ) |>
  filter(n >= 10, usage_pct >= 5)  # drop rarely used pitches

pitch_colours_vec <- c(
  "4-Seam Fastball" = "#E63946",
  "Sinker"          = "#F4A261",
  "Cutter"          = "#457B9D",
  "Changeup"        = "#2A9D8F",
  "Sweeper"         = "#F72585"
)

p_arsenal <- ggplot(arsenal, aes(x = avg_velo, y = whiff_rate, 
                                  colour = pitch_name, size = usage_pct)) +
  geom_point(alpha = 0.9) +
  ggrepel::geom_text_repel(
    aes(label = paste0(pitch_name, "\n", avg_velo, " mph | ", round(usage_pct, 1), "% usage")),
    size            = 3.5,
    fontface        = "bold",
    show.legend     = FALSE,
    box.padding     = 0.5,
    point.padding   = 0.3,
    colour          = "grey20"
  ) +
  scale_colour_manual(values = pitch_colours_vec) +
  scale_size_continuous(range = c(4, 14)) +
  scale_y_continuous(labels = function(x) paste0(x, "%")) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title       = element_text(face = "bold", size = 15),
    plot.subtitle    = element_text(size = 11, colour = "grey40"),
    plot.caption     = element_text(size = 9, colour = "grey60"),
    panel.grid.minor = element_blank(),
    legend.position = "bottom",
    plot.background  = element_rect(fill = "white", colour = NA)
  ) +
  labs(
    title    = "Shane Smith 2025 Pitch Arsenal",
    subtitle = "Velocity vs. Whiff Rate | Dot size = usage %",
    x        = "Average Velocity (mph)",
    y        = "Whiff Rate (%)",
    caption  = "Regular season 2025. Data: Baseball Savant."
  )

ggsave("outputs/visuals/smith_arsenal.png", p_arsenal, 
       width = 10, height = 7, dpi = 150, bg = "white")
message("Saved: smith_arsenal.png")

# ── Strike zone heatmap ────────────────────────────────────────────────────────

sz_data <- smith_pitches |>
  filter(pitch_name != "", game_type == "R", 
         !is.na(plate_x), !is.na(plate_z)) |>
  filter(pitch_name %in% c("Sweeper", "Sinker", "4-Seam Fastball", "Cutter"))

# Strike zone box
sz_box <- data.frame(
  xmin = -0.83, xmax = 0.83,
  ymin = 1.5,   ymax = 3.5
)

p_heatmap <- ggplot(sz_data, aes(x = plate_x, y = plate_z)) +
  stat_density_2d(
    aes(fill = after_stat(density)),
    geom     = "raster",
    contour  = FALSE,
    n        = 100
  ) +
  scale_fill_gradientn(
    colours = c("white", "#FFF9C4", "#FF9800", "#F44336", "#7B0000"),
    name    = "Density"
  ) +
  geom_rect(
    data        = sz_box,
    aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
    inherit.aes = FALSE,
    fill        = NA,
    colour      = "black",
    linewidth   = 0.8
  ) +
  facet_wrap(~ pitch_name, ncol = 2, scales = "fixed") +
  coord_fixed(xlim = c(-2, 2), ylim = c(0.5, 5)) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(size = 11, colour = "grey40"),
    plot.caption       = element_text(size = 9, colour = "grey60"),
    panel.grid         = element_blank(),
    strip.text         = element_text(face = "bold", size = 12),
    strip.background   = element_rect(fill = "#27251F", colour = NA),
    strip.text.x       = element_text(colour = "white"),
    legend.position    = "right",
    plot.background    = element_rect(fill = "white", colour = NA)
  ) +
  labs(
    title    = "Shane Smith 2025 Strike Zone Location by Pitch Type",
    subtitle = "Pitcher's perspective | Darker = higher pitch density",
    x        = "Horizontal Location (ft)",
    y        = "Vertical Location (ft)",
    caption  = "Regular season 2025. Strike zone box = approximate rulebook zone. Data: Baseball Savant."
  )

ggsave("outputs/visuals/smith_heatmap.png", p_heatmap,
       width = 10, height = 9, dpi = 150, bg = "white")
message("Saved: smith_heatmap.png")


# ── CWS 2026 Top 100 Prospect Report ──────────────────────────────────────────
library(ggplot2)
library(dplyr)
library(tidyr)
library(patchwork)

# ── Data ───────────────────────────────────────────────────────────────────────

prospects <- list(
  list(
    name     = "Braden Montgomery",
    pos      = "OF",
    age      = 22,
    rank_pip = 36,
    rank_ba  = 73,
    rank_law = 30,
    eta      = "2026",
    level    = "Double-A",
    blurb    = "Switch-hitting outfielder with a 70-grade arm and plus athleticism.\nPosted 133 wRC+ in his Double-A debut. Contact rate (sub-70%)\nremains the key question, but power ceiling is immense.",
    stats    = data.frame(
      level  = c("Low-A", "High-A", "Double-A"),
      wrc    = c(150, 134, 133),
      hr     = c(5, 7, 5),
      bb_pct = c(12.1, 13.4, 12.8),
      k_pct  = c(24.1, 26.3, 28.7)
    ),
    tools = data.frame(
      tool  = c("Hit", "Power", "Speed", "Field", "Arm"),
      grade = c(50, 60, 55, 55, 70)
    ),
    colour = "#27251F"
  ),
  list(
    name     = "Noah Schultz",
    pos      = "LHP",
    age      = 22,
    rank_pip = 49,
    rank_ba  = 26,
    rank_law = 95,
    eta      = "2026",
    level    = "Triple-A",
    blurb    = "6-foot-10 lefty with a 70-grade slider and deceptive low arm slot\nreminiscent of Chris Sale. Knee tendinitis limited 2025, but\ntop-of-rotation ceiling remains intact if healthy.",
    stats    = data.frame(
      level  = c("Double-A\n2024", "Double-A\n2025", "Triple-A\n2025"),
      era    = c(2.31, 4.21, 5.10),
      whip   = c(1.08, 1.61, 1.74),
      k9     = c(11.2, 9.8, 8.4),
      bb_pct = c(7.1, 13.2, 14.4)
    ),
    tools = data.frame(
      tool  = c("Fastball", "Slider", "Changeup", "Command", "Deception"),
      grade = c(65, 70, 45, 45, 65)
    ),
    colour = "#C4162A"
  ),
  list(
    name     = "Hagen Smith",
    pos      = "LHP",
    age      = 22,
    rank_pip = 72,
    rank_ba  = 92,
    rank_law = 58,
    eta      = "2026/27",
    level    = "Double-A",
    blurb    = "2024 first-round pick out of Arkansas. Sat 94-96 mph in the AFL\nwith multiple slider shapes. Strong late-season momentum after\na slow start. High-upside starter with closer fallback.",
    stats    = data.frame(
      level  = c("Low-A", "High-A", "Double-A", "AFL"),
      era    = c(3.10, 3.75, 4.20, 2.80),
      whip   = c(1.21, 1.35, 1.48, 1.10),
      k9     = c(12.1, 11.4, 10.8, 13.2),
      bb_pct = c(10.2, 11.8, 12.4, 9.5)
    ),
    tools = data.frame(
      tool  = c("Fastball", "Slider", "Changeup", "Command", "Deception"),
      grade = c(60, 65, 50, 50, 55)
    ),
    colour = "#004687"
  )
)

prospects[[1]]$stats$level <- factor(
  prospects[[1]]$stats$level,
  levels = c("Low-A", "High-A", "Double-A")
)

prospects[[3]]$stats$level <- factor(
  prospects[[3]]$stats$level,
  levels = c("Low-A", "High-A", "Double-A", "AFL")
)

make_card <- function(p) {
  
  is_pitcher <- p$pos %in% c("LHP", "RHP")
  
  p_tools <- ggplot(p$tools, aes(x = reorder(tool, grade), y = grade, fill = grade)) +
    geom_col(width = 0.6) +
    geom_text(aes(label = grade), hjust = -0.3, size = 3.5, fontface = "bold") +
    scale_fill_gradientn(
      colours = c("#d73027", "#fee08b", "#1a9850"),
      limits  = c(20, 80)
    ) +
    scale_y_continuous(limits = c(0, 85)) +
    coord_flip() +
    theme_minimal(base_size = 11) +
    theme(
      legend.position  = "none",
      panel.grid       = element_blank(),
      axis.text.x      = element_blank(),
      axis.title       = element_blank(),
      plot.title       = element_text(face = "bold", size = 10, colour = "grey30"),
      plot.background  = element_rect(fill = "white", colour = NA)
    ) +
    labs(title = "Tool Grades (20-80 scale)")

  if (!is_pitcher) {
    p_stats <- ggplot(p$stats, aes(x = level, y = wrc, group = 1)) +
      geom_col(fill = p$colour, alpha = 0.85, width = 0.5) +
      geom_text(aes(label = wrc), vjust = -0.4, size = 3.5, fontface = "bold") +
      geom_hline(yintercept = 100, linetype = "dashed", colour = "grey50", linewidth = 0.5) +
      annotate("text", x = 0.6, y = 104, label = "MLB avg", size = 3, colour = "grey50", hjust = 0) +
      scale_y_continuous(limits = c(0, 175)) +
      scale_x_discrete(limits = levels(p$stats$level)) +
      theme_minimal(base_size = 11) +
      theme(
        panel.grid.major.x = element_blank(),
        panel.grid.minor   = element_blank(),
        axis.title.x       = element_blank(),
        plot.title         = element_text(face = "bold", size = 10, colour = "grey30"),
        plot.background    = element_rect(fill = "white", colour = NA)
      ) +
      labs(title = "wRC+ by Level", y = "wRC+")
  } else {
    p_stats <- ggplot(p$stats, aes(x = level)) +
      geom_col(aes(y = k9), fill = p$colour, alpha = 0.85, width = 0.5) +
      geom_text(aes(y = k9, label = round(k9, 1)), vjust = -0.4, size = 3.5, fontface = "bold") +
      geom_line(aes(y = bb_pct * 1.0, group = 1), colour = "#E63946", linewidth = 1) +
geom_point(aes(y = bb_pct * 1.0), colour = "#E63946", size = 2.5) +
scale_y_continuous(
  limits   = c(0, 16),
  name     = "K/9",
  sec.axis = sec_axis(~ . / 1.0, name = "BB% (red line)")
) +
      theme_minimal(base_size = 11) +
      theme(
        panel.grid.major.x  = element_blank(),
        panel.grid.minor    = element_blank(),
        axis.title.x        = element_blank(),
        axis.title.y.right  = element_text(colour = "#E63946", size = 9),
        plot.title          = element_text(face = "bold", size = 10, colour = "grey30"),
        plot.background     = element_rect(fill = "white", colour = NA)
      ) +
      labs(title = "K/9 by Level (BB% in red)")
  }

  ranks <- data.frame(
    outlet = c("MLB Pipeline", "Baseball America", "Keith Law"),
    rank   = c(p$rank_pip, p$rank_ba, p$rank_law)
  )

  p_ranks <- ggplot(ranks, aes(x = reorder(outlet, -rank), y = 1)) +
    geom_text(
      aes(label = paste0(outlet, "   #", rank)),
      hjust = 0.5, size = 4, colour = p$colour, fontface = "bold"
    ) +
    ylim(0.5, 1.5) +
    coord_flip() +
    theme_void() +
    theme(
      plot.title      = element_text(face = "bold", size = 10, colour = "grey30",
                                     margin = margin(b = 6)),
      plot.background = element_rect(fill = "white", colour = NA)
    ) +
    labs(title = "National Rankings")

  header <- ggplot() +
    annotate("rect", xmin = 0, xmax = 1, ymin = 0.55, ymax = 1,
             fill = p$colour, colour = NA) +
    annotate("text", x = 0.05, y = 0.84,
             label = p$name,
             hjust = 0, vjust = 0.5,
             size = 6, fontface = "bold", colour = "white") +
    annotate("text", x = 0.05, y = 0.645,
             label = paste0(p$pos, "  |  Age ", p$age, "  |  ", p$level, "  |  ETA: ", p$eta),
             hjust = 0, vjust = 0.5,
             size = 3.5, colour = "white") +
    annotate("text", x = 0.05, y = 0.42,
             label = p$blurb,
             hjust = 0, vjust = 1,
             size = 3.2, colour = "grey20", lineheight = 1.4) +
    xlim(0, 1) + ylim(0, 1) +
    theme_void() +
    theme(plot.background = element_rect(fill = "white", colour = NA))

  (header / (p_tools | p_stats | p_ranks)) +
    plot_layout(heights = c(0.8, 1.4)) +
    plot_annotation(
      theme = theme(plot.background = element_rect(fill = "white", colour = "#e0e0e0", linewidth = 1))
    )
}

cards <- lapply(prospects, make_card)

final <- wrap_plots(cards, ncol = 1) +
  plot_annotation(
    title    = "Chicago White Sox: 2026 Top 100 Prospects",
    subtitle = "Three prospects ranked in MLB Pipeline's national Top 100, all expected to debut in 2026",
    caption  = "Sources: MLB Pipeline, Baseball America, The Athletic (Keith Law). Stats: MiLB 2025 regular season + AFL.",
    theme    = theme(
      plot.title      = element_text(face = "bold", size = 17, margin = margin(b = 4)),
      plot.subtitle   = element_text(size = 12, colour = "grey40", margin = margin(b = 10)),
      plot.caption    = element_text(size = 9, colour = "grey60"),
      plot.background = element_rect(fill = "white", colour = NA)
    )
  )

ggsave("outputs/visuals/cwx_top100_prospects.png", final,
       width = 13, height = 18, dpi = 150, bg = "white")
message("Saved: cwx_top100_prospects.png")
