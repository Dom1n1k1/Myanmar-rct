# -------------------------------------------------------------------
# 00_setup.R — project setup for Myanmar MHMI RCT
# Author: Dominik Schwarzkopf
# Date: 2025-08-28
#
# Purpose:
# 1) Load packages
# 2) Load .Renviron so DATA_DIR is available
# 3) Define folder structure for /data (off-repo) and /out (in-repo)
# 4) Create missing folders
# 5) Set options and themes
# 6) Provide helpers for saving, logging, snapshotting
# 7) Append a log block and refresh snapshot
# -------------------------------------------------------------------

# ---- 0) Options ----------------------------------------------------
set.seed(20250821)

# ---- 1) Packages ---------------------------------------------------
pkgs <- c(
  "broom","car","corrr","dplyr","estimatr","fixest","forcats","fs",
  "ggeffects","ggplot2","glue","haven","here","janitor","kableExtra","knitr",
  "labelled","lubridate","lmtest","modelsummary","patchwork","purrr","readxl",
  "sandwich","stringr","texreg","tidyr","tidyverse"
)
to_install <- setdiff(pkgs, rownames(installed.packages()))
if (length(to_install)) install.packages(to_install, quiet = TRUE)
invisible(lapply(pkgs, library, character.only = TRUE))

suppressWarnings(try(here::i_am("code/00_setup.R"), silent = TRUE))

# ---- 2) Load .Renviron ---------------------------------------------
if (file.exists(".Renviron")) readRenviron(".Renviron")
DATA_DIR <- Sys.getenv("DATA_DIR", unset = NA)
if (is.na(DATA_DIR) || !fs::dir_exists(DATA_DIR)) {
  stop("DATA_DIR not set or folder missing. Edit .Renviron and restart R.")
}

# ---- 3) Define paths -----------------------------------------------
PATH_DATA <- list(
  `00_data_original`         = fs::path(DATA_DIR, "00_data_original"),
  `01_waves_raw`             = fs::path(DATA_DIR, "01_waves_raw"),
  `02_schema_scan`           = fs::path(DATA_DIR, "02_schema_scan"),
  `03_keys_and_clusters`     = fs::path(DATA_DIR, "03_keys_and_clusters"),
  `04_household_raw`         = fs::path(DATA_DIR, "04_household_raw"),
  `05_individual_raw`        = fs::path(DATA_DIR, "05_individual_raw"),
  `06_household_clean`       = fs::path(DATA_DIR, "06_household_clean"),
  `07_individual_clean`      = fs::path(DATA_DIR, "07_individual_clean"),
  `08_individual_aggregated` = fs::path(DATA_DIR, "08_individual_aggregated"),
  `09_household_analysis`    = fs::path(DATA_DIR, "09_household_analysis"),
  `10_individual_analysis`   = fs::path(DATA_DIR, "10_individual_analysis")
)

PATH_OUT_ROOT <- here::here("out")
PATH_OUT <- list(
  `data_prep` = fs::path(PATH_OUT_ROOT, "data_prep"),
  `tables`    = fs::path(PATH_OUT_ROOT, "tables"),
  `figures`   = fs::path(PATH_OUT_ROOT, "figures")
)
PATH_OUT_DP <- list(
  `01_data_build`            = fs::path(PATH_OUT$data_prep, "01_data_build"),
  `02_schema_scan`           = fs::path(PATH_OUT$data_prep, "02_schema_scan"),
  `03_keys_and_clusters`     = fs::path(PATH_OUT$data_prep, "03_keys_and_clusters"),
  `04_household_raw`         = fs::path(PATH_OUT$data_prep, "04_household_raw"),
  `05_individual_raw`        = fs::path(PATH_OUT$data_prep, "05_individual_raw"),
  `06_household_clean`       = fs::path(PATH_OUT$data_prep, "06_household_clean"),
  `07_individual_clean`      = fs::path(PATH_OUT$data_prep, "07_individual_clean"),
  `08_individual_aggregated` = fs::path(PATH_OUT$data_prep, "08_individual_aggregated"),
  `09_household_analysis`    = fs::path(PATH_OUT$data_prep, "09_household_analysis"),
  `10_individual_analysis`   = fs::path(PATH_OUT$data_prep, "10_individual_analysis")
)
PATH_OUT_TBL <- list(
  `balance_tables` = fs::path(PATH_OUT$tables, "balance_tables"),
  `main_results`   = fs::path(PATH_OUT$tables, "main_results"),
  `implementation` = fs::path(PATH_OUT$tables, "implementation"),
  `heterogeneity`  = fs::path(PATH_OUT$tables, "heterogeneity"),
  `appendix`       = fs::path(PATH_OUT$tables, "appendix")
)
PATH_OUT_FIG <- list(
  `descriptives`   = fs::path(PATH_OUT$figures, "descriptives"),
  `main_results`   = fs::path(PATH_OUT$figures, "main_results"),
  `appendix`       = fs::path(PATH_OUT$figures, "appendix")
)

PATH_CODE <- here::here("code")
PATH_DOCS <- here::here("docs")
PATH_DOCS_APPENDIX  <- fs::path(PATH_DOCS, "appendix")
PATH_DOCS_FLOWLOG   <- fs::path(PATH_DOCS, "data_flow_log.md")
PATH_DOCS_STRUCTURE <- fs::path(PATH_DOCS, "repo_structure_snapshot.md")
PATH_SPECS <- here::here("specs")

# ---- 4) Create folders ---------------------------------------------
invisible(fs::dir_create(unlist(PATH_DATA, use.names = FALSE)))
invisible(fs::dir_create(unlist(PATH_OUT, use.names = FALSE)))
invisible(fs::dir_create(unlist(PATH_OUT_DP, use.names = FALSE)))
invisible(fs::dir_create(unlist(PATH_OUT_TBL, use.names = FALSE)))
invisible(fs::dir_create(unlist(PATH_OUT_FIG, use.names = FALSE)))
invisible(fs::dir_create(c(PATH_CODE, PATH_DOCS, PATH_DOCS_APPENDIX, PATH_SPECS)))

if (!fs::file_exists(PATH_DOCS_FLOWLOG)) {
  writeLines("# Data Flow Log\n\nThis file tracks created files and outputs step by step.\n", PATH_DOCS_FLOWLOG)
}
if (!fs::file_exists(PATH_DOCS_STRUCTURE)) {
  writeLines("# Repo Structure Snapshot\n\nThis file captures a static overview of repo and data directories.\n", PATH_DOCS_STRUCTURE)
}

# ---- 5) Global options ---------------------------------------------
options(dplyr.summarise.inform = FALSE, scipen = 999)
knitr::opts_chunk$set(echo = TRUE, message = FALSE, warning = FALSE, fig.path = "out/figures/")
ggplot2::theme_set(ggplot2::theme_minimal(base_size = 11))

# ---- 6) Helpers ----------------------------------------------------
write_table <- function(df, fname, out_dir = PATH_OUT$tables, stamp = TRUE) {
  stopifnot(is.data.frame(df))
  base  <- if (stamp) glue::glue("{Sys.Date()}_{fname}") else fname
  fpath <- fs::path(out_dir, base)
  readr::write_csv(df, fpath, na = "")
  return(as.character(fpath))
}
save_rds_step <- function(obj, fname, data_dir) {
  stopifnot(fs::dir_exists(data_dir))
  fpath <- fs::path(data_dir, fname)
  saveRDS(obj, fpath)
  return(as.character(fpath))
}
assert_cols <- function(df, cols) {
  miss <- setdiff(cols, names(df))
  if (length(miss)) stop("Missing columns: ", paste(miss, collapse = ", "))
  invisible(TRUE)
}

.rel_out <- function(p) {
  root <- normalizePath(PATH_OUT_ROOT, winslash = "/", mustWork = FALSE)
  x    <- normalizePath(p, winslash = "/", mustWork = FALSE)
  rel  <- sub(paste0("^", root), "", x)
  if (!startsWith(rel, "/")) rel <- paste0("/", rel)
  rel
}
.rel_data <- function(p) {
  root <- normalizePath(DATA_DIR, winslash = "/", mustWork = FALSE)
  x    <- normalizePath(p, winslash = "/", mustWork = FALSE)
  rel  <- sub(paste0("^", root), "", x)
  if (!startsWith(rel, "/")) rel <- paste0("/", rel)
  rel
}

append_log <- function(step, code_files = NULL, docs_files = NULL, out_files = NULL, data_files = NULL) {
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M")
  lines <- c(glue::glue("## {step}  "),
             glue::glue("Timestamp: {ts}"))
  if (!is.null(code_files)) for (f in code_files) lines <- c(lines, glue::glue("**/code**: {f}"))
  if (!is.null(docs_files)) for (f in docs_files) lines <- c(lines, glue::glue("**/docs**: {f}"))
  if (!is.null(out_files))  for (f in out_files) lines <- c(lines, glue::glue("**/out**: {.rel_out(f)}"))
  if (!is.null(data_files)) for (f in data_files) lines <- c(lines, glue::glue("**/data**: {.rel_data(f)}"))
  lines <- c(lines, "", "---", "")
  cat(paste0(lines, collapse = "\n"), file = PATH_DOCS_FLOWLOG, append = TRUE)
}

# New helper for listing folders + files (with empty fallback)
.list_folder <- function(path, header) {
  block <- c(glue::glue("{header}"))
  if (fs::dir_exists(path)) {
    files <- list.files(path, recursive = FALSE, full.names = FALSE)
    if (length(files)) {
      block <- c(block, paste0("• ", sort(files)))
    } else {
      block <- c(block, "• (empty)")
    }
  } else {
    block <- c(block, "• (missing directory)")
  }
  block
}

write_structure_snapshot <- function() {
  ts <- format(Sys.Date(), "%Y-%m-%d")
  block <- c(glue::glue("# Repo Structure Snapshot ({ts})"),
             "## /code",
             paste0("• ", sort(basename(list.files(PATH_CODE, recursive = FALSE)))),
             "",
             "## /docs",
             paste0("• ", sort(basename(list.files(PATH_DOCS, recursive = FALSE)))),
             "",
             "## /data (off-repo on SwitchDrive)")
  
  for (nm in names(PATH_DATA)) {
    block <- c(block, .list_folder(PATH_DATA[[nm]], glue::glue("### /data/{nm}")))
  }
  
  block <- c(block, "",
             "## /out (in-repo)",
             "### /out/tables")
  for (nm in names(PATH_OUT_TBL)) {
    block <- c(block, .list_folder(PATH_OUT_TBL[[nm]], glue::glue("#### /out/tables/{nm}")))
  }
  block <- c(block, "### /out/figures")
  for (nm in names(PATH_OUT_FIG)) {
    block <- c(block, .list_folder(PATH_OUT_FIG[[nm]], glue::glue("#### /out/figures/{nm}")))
  }
  block <- c(block, "### /out/data_prep")
  for (nm in names(PATH_OUT_DP)) {
    block <- c(block, .list_folder(PATH_OUT_DP[[nm]], glue::glue("#### /out/data_prep/{nm}")))
  }
  
  writeLines(block, PATH_DOCS_STRUCTURE)
}

path_summary <- function() {
  list(DATA_DIR = as.character(DATA_DIR),
       DATA_subdirs = PATH_DATA,
       OUT_data_prep = PATH_OUT_DP,
       OUT_tables = PATH_OUT_TBL,
       OUT_figures = PATH_OUT_FIG)
}

# ---- 7) Constants & specs ------------------------------------------
NA_CODES <- c(97L, 98L, 99L)
SPEC_WAVE_MAP   <- fs::path(PATH_SPECS, "wave_map.csv")
SPEC_DICT       <- fs::path(PATH_SPECS, "indicator_dictionary.csv")
SPEC_DERIV_YAML <- fs::path(PATH_SPECS, "derivations_spec.yaml")
SPEC_CODEBOOK_XLSX <- fs::path(PATH_SPECS, "codebook_MHMI_baseline_endline.xlsx")

# ---- Final message -------------------------------------------------
message("Setup OK. DATA_DIR = '", DATA_DIR, "'.")

try(write_structure_snapshot(), silent = TRUE)

append_log(
  step = "00_setup.R",
  code_files = "code/00_setup.R",
  docs_files = c("docs/data_flow_log.md", "docs/repo_structure_snapshot.md")
)
write_structure_snapshot()
