# -------------------------------------------------------------------
# 02a_schema_scan_help.R - helper utilities to read and summarize
# schema scan outputs for the Myanmar MHMI RCT
# Author: Dominik Schwarzkopf
# Date: 2025-08-25
#
# What this file provides:
# 1) load_schema_tables() - loads all schema CSVs into a list
# 2) preview_var(name) - label, missingness, examples across waves
# 3) search_vars(keyword) - search names and labels across waves
# 4) valmap(name) - value label map for a variable by wave
# 5) pretty_candidates(family) - tidy candidate table for a family
# 6) family_summary() - count and missingness summary by family and wave
# 7) build_candidates_template() - editable CSV for final selections
# 8) export_pretty_candidates() - writes pretty candidate CSVs to docs/
#
# Nothing runs on source. Call the functions from the console or Rmd.
# -------------------------------------------------------------------

# ---- setup and small utilities -------------------------------------
suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr)
  library(stringr); library(glue); library(fs)
})

# Use project paths and ensure schema folder exists
source("code/00_setup.R")
SCHEMA_DIR <- fs::path(PATH_OUT_TBL, "schema_scan")

# Safe CSV reader
.read_csv_safe <- function(path) {
  if (fs::file_exists(path)) readr::read_csv(path, show_col_types = FALSE) else NULL
}

# ---- 1) Load all schema tables into a list --------------------------
# Returns a named list:
# $vv         schema_variables.csv
# $labels     tibble with wave, var, label
# $vlabels    tibble with wave, var, value, value_label
# $blocks     schema_repeated_blocks.csv
# $candidates tibble with family, wave, var, label, and metrics
load_schema_tables <- function() {
  if (!fs::dir_exists(SCHEMA_DIR)) {
    stop("Schema directory not found at: ", SCHEMA_DIR,
         ". Run 02_schema_scan.R first.")
  }
  vv <- .read_csv_safe(fs::path(SCHEMA_DIR, "schema_variables.csv"))
  
  labs <- bind_rows(
    .read_csv_safe(fs::path(SCHEMA_DIR, "labels_baseline.csv")),
    .read_csv_safe(fs::path(SCHEMA_DIR, "labels_endline.csv")),
    .read_csv_safe(fs::path(SCHEMA_DIR, "labels_midline.csv"))
  )
  
  vlabels <- bind_rows(
    .read_csv_safe(fs::path(SCHEMA_DIR, "value_labels_baseline.csv")),
    .read_csv_safe(fs::path(SCHEMA_DIR, "value_labels_endline.csv")),
    .read_csv_safe(fs::path(SCHEMA_DIR, "value_labels_midline.csv"))
  )
  
  blocks <- .read_csv_safe(fs::path(SCHEMA_DIR, "schema_repeated_blocks.csv"))
  
  fams <- c("utilization","finance","maternal","child","covid")
  cand <- lapply(fams, function(f) {
    p <- fs::path(SCHEMA_DIR, glue("schema_candidates_{f}.csv"))
    df <- .read_csv_safe(p)
    if (!is.null(df) && nrow(df)) df <- mutate(df, family = f, .before = 1)
    df
  })
  candidates <- bind_rows(cand)
  
  list(vv = vv, labels = labs, vlabels = vlabels, blocks = blocks, candidates = candidates)
}

# ---- 2) Preview a variable across waves -----------------------------
# Shows name, label, type, missingness, uniques, examples
preview_var <- function(varname) {
  sch <- load_schema_tables()
  vv <- sch$vv
  if (is.null(vv)) stop("schema_variables.csv not found.")
  out <- vv %>%
    filter(name == varname) %>%
    select(wave, name, label, type, n_missing, pct_missing, n_unique, examples) %>%
    arrange(wave)
  if (nrow(out) == 0) message("Not found: ", varname)
  out
}

# ---- 3) Search names and labels for a keyword -----------------------
search_vars <- function(keyword) {
  sch <- load_schema_tables()
  vv <- sch$vv
  if (is.null(vv)) stop("schema_variables.csv not found.")
  kw <- tolower(keyword)
  vv %>%
    filter(str_detect(tolower(name), kw) | str_detect(tolower(label), kw)) %>%
    arrange(wave, name)
}

# ---- 4) Value label map per variable per wave -----------------------
valmap <- function(varname) {
  sch <- load_schema_tables()
  vl <- sch$vlabels
  if (is.null(vl) || nrow(vl) == 0) {
    message("No value labels found.")
    return(invisible(NULL))
  }
  vl %>% filter(var == varname) %>% arrange(wave, value)
}

# ---- 5) Pretty candidates for a given family ------------------------
# Returns a tidy table with the most relevant columns and sorting
pretty_candidates <- function(family = c("utilization","finance","maternal","child","covid")) {
  family <- match.arg(family)
  sch <- load_schema_tables()
  cand <- sch$candidates
  if (is.null(cand) || nrow(cand) == 0) stop("No candidate files found in schema_scan.")
  cand %>%
    filter(family == !!family) %>%
    select(family, wave, var, label, type, pct_missing, n_unique, examples) %>%
    arrange(wave, pct_missing, var)
}

# ---- 6) Summary by family and wave ----------------------------------
# Quick counts and missingness stats
family_summary <- function() {
  sch <- load_schema_tables()
  cand <- sch$candidates
  if (is.null(cand) || nrow(cand) == 0) stop("No candidate files found in schema_scan.")
  cand %>%
    group_by(family, wave) %>%
    summarise(
      n_vars = n(),
      pct_missing_median = median(pct_missing, na.rm = TRUE),
      pct_missing_p90    = quantile(pct_missing, 0.9, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(family, wave)
}

# ---- 7) Build editable selections template in docs/ -----------------
# Produces docs/analysis_selections_template.csv with blank columns
build_candidates_template <- function() {
  sch <- load_schema_tables()
  cand <- sch$candidates
  if (is.null(cand) || nrow(cand) == 0) {
    message("No candidate files found. Did 02_schema_scan.R run?")
    return(invisible(NULL))
  }
  tpl <- cand %>%
    select(family, wave, var, label, type, pct_missing, n_unique, examples) %>%
    mutate(
      level      = "",   # hh or pregnancy or child
      indicator  = "",   # e.g., hh_tele_any_30d
      window     = "",   # 30d or 12m or most_recent
      coding     = "",   # binary any or none, sum, amount, etc.
      recode     = "",   # e.g., 1->1, 2->0, else NA
      notes      = ""    # remarks
    )
  dir.create("docs", showWarnings = FALSE)
  out_path <- here::here("docs", "analysis_selections_template.csv")
  readr::write_csv(tpl, out_path, na = "")
  message("Wrote ", out_path)
  invisible(tpl)
}

# ---- 8) Export pretty candidates for all families -------------------
# Writes CSVs into docs/ so you can review in one place
export_pretty_candidates <- function() {
  fams <- c("utilization","finance","maternal","child","covid")
  dir.create("docs", showWarnings = FALSE)
  out <- vector("list", length(fams)); names(out) <- fams
  for (f in fams) {
    tbl <- try(pretty_candidates(f), silent = TRUE)
    if (!inherits(tbl, "try-error") && !is.null(tbl) && nrow(tbl)) {
      p <- here::here("docs", glue("candidates_pretty_{f}.csv"))
      readr::write_csv(tbl, p, na = "")
      message("Wrote ", p)
      out[[f]] <- tbl
    }
  }
  invisible(out)
}

# ---- Optional helpers: roster stems quick view ----------------------
# Returns top roster stems by wave to help decide which to pivot
roster_quicklook <- function(top_n = 30) {
  sch <- load_schema_tables()
  b <- sch$blocks
  if (is.null(b) || nrow(b) == 0) {
    message("No roster stems detected in schema_repeated_blocks.csv")
    return(invisible(NULL))
  }
  b %>%
    group_by(wave) %>%
    arrange(wave, desc(n_indices)) %>%
    slice_head(n = top_n) %>%
    ungroup()
}

# -------------------------------------------------------------------
# Usage examples (run in console after source):
#
# source("code/02a_schema_scan_help.R")
# preview_var("group")
# search_vars("tele")
# valmap("group")
# pretty_candidates("utilization")
# family_summary()
# build_candidates_template()
# export_pretty_candidates()
# roster_quicklook(20)
# -------------------------------------------------------------------
