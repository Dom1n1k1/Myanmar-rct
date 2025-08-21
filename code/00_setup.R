# -------------------------------------------------------------------
# 00_setup.R — project setup for Myanmar MHMI RCT
# Author: Dominik Schwarzkopf
# Date: 2025-08-21
#
# What this script does:
# 1) Loads or installs required packages (alphabetical; each with a short purpose note)
# 2) Loads the project .Renviron so DATA_DIR is available
# 3) Defines canonical paths for data (on SwitchDrive) and outputs (in repo)
# 4) Creates missing folders, including SwitchDrive data folders
# 5) Sets knitr defaults, seed, ggplot theme
# 6) Provides small helper functions for saving tables, clean data, and checks
# -------------------------------------------------------------------

# ---- 0) Options useful across the project --------------------------
set.seed(20250821)

# ---- 1) Packages (alphabetical) -----------------------------------
# broom        : Tidy model outputs (turn models into data frames)
# car          : Companion to Applied Regression (ANOVA, VIF, etc.)
# corrr        : Tidy correlation matrices and visualizations
# dplyr        : Data manipulation
# estimatr     : Fast regressions with robust/HC/cluster SEs
# fixest       : Econometrics (fast FE/IV, cluster-robust vcov)
# forcats      : Tidy factor utilities
# fs           : File system utilities (paths, dirs)
# ggeffects    : Marginal effects from models for ggplot
# ggplot2      : Grammar of graphics plotting
# glue         : String interpolation
# haven        : Read Stata/SPSS/SAS files
# here         : Project-rooted paths
# janitor      : Clean data frames, names, simple tabulations
# kableExtra   : Pretty tables in LaTeX/HTML
# knitr        : Knitting options and chunk control
# labelled     : Variable labels helpers (esp. survey data)
# lubridate    : Dates/times
# lmtest       : Diagnostic tests for models (coeftest, etc.)
# modelsummary : Publication-ready model tables
# patchwork    : Combine ggplots
# purrr        : Functional programming (map, etc.)
# readxl       : Read Excel files
# sandwich     : Robust/cluster-robust covariance matrices
# stringr      : String handling
# texreg       : Export regression tables
# tidyr        : Reshape/tidying tools
# tidyverse    : Meta-package (dplyr, ggplot2, readr, tidyr, etc.)
# -------------------------------------------------------------------

pkgs <- c(
  "broom","car","corrr","dplyr","estimatr","fixest","forcats","fs",
  "ggeffects","ggplot2","glue","haven","here","janitor","kableExtra","knitr",
  "labelled","lubridate","lmtest","modelsummary","patchwork","purrr","readxl",
  "sandwich","stringr","texreg","tidyr","tidyverse"
)

to_install <- setdiff(pkgs, rownames(installed.packages()))
if (length(to_install)) install.packages(to_install, quiet = TRUE)
invisible(lapply(pkgs, library, character.only = TRUE))

# Lock the project root for {here}
suppressWarnings(try(here::i_am("code/00_setup.R"), silent = TRUE))

# ---- 2) Load .Renviron so DATA_DIR is available --------------------
# In the project .Renviron, set:
# DATA_DIR="C:/Users/schwdo/switchdrive/Poverty Action Lab Myanmar Impact Evaluation/Data"
if (file.exists(".Renviron")) readRenviron(".Renviron")

DATA_DIR <- Sys.getenv("DATA_DIR", unset = NA)
if (is.na(DATA_DIR) || !fs::dir_exists(DATA_DIR)) {
  stop("DATA_DIR is not set or the folder does not exist. Edit .Renviron in the project root and restart R.")
}

# ---- 3) Define paths (data off-repo; outputs in repo) --------------
PATH_RAW    <- fs::path(DATA_DIR, "data_raw")    # SwitchDrive - not in Git
PATH_CLEAN  <- fs::path(DATA_DIR, "data_clean")  # SwitchDrive - not in Git
PATH_OUT_TBL <- here::here("out", "tables")      # in repo
PATH_OUT_FIG <- here::here("out", "figures")     # in repo
PATH_CODE    <- here::here("code")
PATH_DOCS    <- here::here("docs", "appendix")

# ---- 4) Create missing folders ------------------------------------
# Create SwitchDrive data folders if missing (safe, avoids early stops)
fs::dir_create(PATH_RAW)
fs::dir_create(PATH_CLEAN)
# Create repo-side folders
invisible(fs::dir_create(c(PATH_OUT_TBL, PATH_OUT_FIG, PATH_CODE, PATH_DOCS)))

# ---- 5) Global options, knitr defaults, ggplot theme ---------------
options(dplyr.summarise.inform = FALSE, scipen = 999)
knitr::opts_chunk$set(
  echo = TRUE,
  message = FALSE,
  warning = FALSE,
  fig.path = "out/figures/"
)
ggplot2::theme_set(ggplot2::theme_minimal(base_size = 11))

# ---- 6) Small helpers ----------------------------------------------
# Write a CSV table to out/tables with an optional date prefix
write_table <- function(df, fname, stamp = TRUE) {
  stopifnot(is.data.frame(df))
  base <- if (stamp) glue::glue("{Sys.Date()}_{fname}") else fname
  fpath <- fs::path(PATH_OUT_TBL, base)
  readr::write_csv(df, fpath, na = "")
  return(as.character(fpath))
}

# Save an R object to SwitchDrive clean data folder
save_clean <- function(obj, fname) {
  fpath <- fs::path(PATH_CLEAN, fname)
  saveRDS(obj, fpath)
  return(as.character(fpath))
}

# Assert that required columns exist in a data frame
assert_cols <- function(df, cols) {
  miss <- setdiff(cols, names(df))
  if (length(miss)) stop("Missing columns: ", paste(miss, collapse = ", "))
  invisible(TRUE)
}

# Quick summary printer for sanity checks
path_summary <- function() {
  list(
    DATA_DIR     = as.character(DATA_DIR),
    PATH_RAW     = as.character(PATH_RAW),
    PATH_CLEAN   = as.character(PATH_CLEAN),
    PATH_OUT_TBL = as.character(PATH_OUT_TBL),
    PATH_OUT_FIG = as.character(PATH_OUT_FIG)
  )
}

# One-line confirmation
message(glue::glue("Setup OK. DATA_DIR = '{DATA_DIR}'. Outputs -> {PATH_OUT_TBL} and {PATH_OUT_FIG}."))
