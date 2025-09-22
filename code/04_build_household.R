# -------------------------------------------------------------------
# 04_build_household.R — extract HH-level vars per wave, exclude rosters + preg
# Author: Dominik Schwarzkopf
# Date: 2025-09-22
#
# What this script does (read before editing)
# - Loads setup and per-wave .rds from Step 1 plus keys.rds from Step 3
# - Excludes (a) roster stems (with and without numeric index) and
#   (b) pregnancy-module variables (pattern *_preg and maternal candidates)
# - For each wave, keeps only variables that actually appear in that wave
#   after exclusions; attaches keys (hh_id, cluster_id, strata, treat, wave)
#   and drops completely empty columns
# - Saves per-wave HH raw files that contain only wave-specific HH variables
#   (no all-NA columns from other waves)
# - Harmonizes types ONLY to build the stacked long file (does not overwrite
#   the per-wave files)
# - Writes a variable catalog, stems and maternal exclusions used, an
#   empty-columns log, and the type-harmonization summary
# - Appends outputs to docs/data_flow_log.md and refreshes the repo snapshot
# -------------------------------------------------------------------

# ---- 0) Setup ------------------------------------------------------
# Date: 2025-08-28
# Purpose: load paths, helpers, and packages
source("code/00_setup.R")
suppressPackageStartupMessages({
  library(dplyr); library(tidyr); library(stringr); library(purrr)
  library(readr); library(tibble); library(fs); library(glue); library(labelled)
})

IN_DIR_WAVES <- PATH_DATA$`01_waves_raw`
IN_DIR_KEYS  <- PATH_DATA$`03_keys_and_clusters`
OUT_DIR_DP   <- PATH_OUT_DP$`04_household_raw`
DATA_OUT     <- PATH_DATA$`04_household_raw`
fs::dir_create(OUT_DIR_DP, recurse = TRUE)
fs::dir_create(DATA_OUT,  recurse = TRUE)

.read_wave <- function(fname) {
  p <- fs::path(IN_DIR_WAVES, fname)
  if (fs::file_exists(p)) readRDS(p) else NULL
}
.write_out <- function(df, name) {
  stopifnot(is.data.frame(df))
  path <- fs::path(OUT_DIR_DP, name)
  readr::write_csv(df, path, na = "")
  as.character(path)
}
.save_wave <- function(obj, name) save_rds_step(obj, name, DATA_OUT)

# ---- 1) Load inputs ------------------------------------------------
# Date: 2025-09-23
# Purpose: read wave files from Step 1 and keys from Step 3
bl   <- .read_wave("01_baseline_raw.rds")
el   <- .read_wave("01_endline_raw.rds")
ml   <- .read_wave("01_midline_raw.rds")
keys_path <- fs::path(IN_DIR_KEYS, "keys.rds")
if (!fs::file_exists(keys_path)) stop("keys.rds not found. Run 03_keys_and_clusters.R first.")
keys <- readRDS(keys_path)

if (all(sapply(list(bl, el, ml), is.null))) stop("No per-wave .rds found. Run 01_data_build.R first.")

# ---- 2) Exclusion sources -----------------------------------------
# Date: 2025-09-23
# Purpose: set up what to exclude from HH extracts

# 2a) Roster stems (prefer Step 2)
blocks_csv <- fs::path(PATH_OUT_DP$`02_schema_scan`, "schema_repeated_blocks.csv")
roster_stems <- character(0)
if (fs::file_exists(blocks_csv)) {
  blk <- suppressMessages(readr::read_csv(blocks_csv, show_col_types = FALSE))
  if (nrow(blk) && "stem" %in% names(blk)) roster_stems <- sort(unique(blk$stem))
}
# Fallback: scan baseline names for suffix digits with or without underscore
if (!length(roster_stems) && !is.null(bl)) {
  m <- stringr::str_match(names(bl), "^(.*?)(?:_|)([0-9]+)$")
  roster_stems <- sort(unique(stats::na.omit(m[,2])))
}
message("Roster stems detected: ", ifelse(length(roster_stems), paste(utils::head(roster_stems, 20), collapse = ", "), "(none)"))

# Regex that drops stem_1, stem2, etc.
drop_regex_stem_index <- if (length(roster_stems)) {
  paste0("^(", paste(stringr::str_replace_all(roster_stems, "([\\W_])", "\\\\\\1"), collapse = "|"), ")(?:_|)?\\d+$")
} else NULL
# Also drop exact stem names (some stems already contain digits)
drop_regex_stem_exact <- if (length(roster_stems)) {
  paste0("^(", paste(stringr::str_replace_all(roster_stems, "([\\W_])", "\\\\\\1"), collapse = "|"), ")$")
} else NULL

# 2b) Pregnancy module by name pattern
# Examples: c9_insu_preg, d1_preg, i4_preg_claim
drop_regex_preg_suffix <- "(?i)_preg($|_)"

# 2c) Pregnancy module from Step 2 candidates (maternal family), if present
# Source of truth: out/data_prep/02_schema_scan/schema_candidates_maternal.csv
cand_maternal_csv <- fs::path(PATH_OUT_DP$`02_schema_scan`, "schema_candidates_maternal.csv")
maternal_vars <- character(0)
if (fs::file_exists(cand_maternal_csv)) {
  cm <- suppressMessages(readr::read_csv(cand_maternal_csv, show_col_types = FALSE))
  if (nrow(cm) && "var" %in% names(cm)) maternal_vars <- sort(unique(cm$var))
}

# Helper: detect empty columns
# Purpose: robust all-empty detector across classes (labelled, factor, dates, etc.)
detect_empty_columns <- function(df) {
  if (is.null(df)) return(character(0))
  is_empty_vec <- function(x) {
    if (inherits(x, "haven_labelled")) x <- haven::zap_labels(x)
    if (is.factor(x))  x <- as.character(x)
    if (is.logical(x)) x <- as.character(x)
    if (is.character(x)) {
      all(trimws(x) == "" | is.na(x))
    } else {
      all(is.na(x))
    }
  }
  names(df)[vapply(df, is_empty_vec, logical(1))]
}

# ---- 3) Extract HH-level vars per wave -----------------------------
# Date: 2025-09-23
# Purpose: exclude roster + pregnancy, keep HH vars, attach keys; keys are source of truth for treat/strata
extract_hh_wave <- function(df, wave_label) {
  if (is.null(df)) return(NULL)
  if (!"hh_id" %in% names(df)) stop("hh_id missing in wave: ", wave_label)
  
  vars <- names(df)
  
  # build exclusion index
  drop_idx <- rep(FALSE, length(vars))
  if (!is.null(drop_regex_stem_index)) drop_idx <- drop_idx | stringr::str_detect(vars, drop_regex_stem_index)
  if (!is.null(drop_regex_stem_exact)) drop_idx <- drop_idx | stringr::str_detect(vars, drop_regex_stem_exact)
  drop_idx <- drop_idx | stringr::str_detect(vars, drop_regex_preg_suffix)
  if (length(maternal_vars)) drop_idx <- drop_idx | vars %in% maternal_vars
  
  # compile keep set, explicitly removing wave-side treat/strata to avoid join collisions
  keep_vars <- setdiff(vars[!drop_idx], c("treat","strata"))
  keep_always <- intersect(c("hh_id","ind_id","group_raw"), keep_vars)
  keep_vars <- union(keep_vars, keep_always)
  
  hh_small <- df |>
    dplyr::select(dplyr::all_of(unique(keep_vars))) |>
    dplyr::distinct() |>
    dplyr::mutate(wave = wave_label) |>
    dplyr::select(dplyr::everything())
  
  joined <- hh_small |>
    dplyr::left_join(
      keys |>
        dplyr::select(dplyr::all_of(c("hh_id","cluster_id","strata","wave","treat"))),
      by = c("hh_id","wave")
    ) |>
    dplyr::select(-dplyr::any_of(c("treat.x","treat.y","strata.x","strata.y")))
  
  # drop completely empty variables and log them (per-wave only)
  empties <- detect_empty_columns(joined)
  if (length(empties)) {
    message(wave_label, ": dropping ", length(empties), " completely empty column(s).")
    joined <- dplyr::select(joined, -dplyr::any_of(empties))
    # Append to the local accumulator defined above
    empties_log <<- dplyr::bind_rows(
      empties_log,
      tibble::tibble(wave = wave_label, column = empties)
    )
  }
  
  # relocate identifiers
  out <- joined |>
    dplyr::relocate(dplyr::any_of(c("hh_id","cluster_id","strata","wave","treat"))) |>
    dplyr::select(dplyr::everything())
  
  out
}

# empties log container (local, not .GlobalEnv)
empties_log <- tibble::tibble(wave = character(), column = character())

hh_bl_pw <- extract_hh_wave(bl, "baseline")
hh_el_pw <- extract_hh_wave(el, "endline")
hh_ml_pw <- extract_hh_wave(ml, "midline")

# ---- 4) Save per-wave files (BEFORE any harmonization) --------------
# Date: 2025-09-23
# Purpose: ensure per-wave files contain only wave-available HH vars
created_data <- character(0)
created_out  <- character(0)

if (!is.null(hh_bl_pw)) created_data <- c(created_data, .save_wave(hh_bl_pw, "baseline_household_raw.rds"))
if (!is.null(hh_el_pw)) created_data <- c(created_data, .save_wave(hh_el_pw, "endline_household_raw.rds"))
if (!is.null(hh_ml_pw)) created_data <- c(created_data, .save_wave(hh_ml_pw, "midline_household_raw.rds"))

# ---- 5) Harmonize types ONLY for stacked long ----------------------
# Date: 2025-09-23
# Purpose: resolve cross-wave type conflicts so bind_rows works; do not overwrite per-wave objects
harmonize_waves <- function(waves_named) {
  waves <- purrr::compact(waves_named)
  if (!length(waves)) return(list(waves = waves_named, summary = tibble::tibble()))
  
  # union of column names
  all_vars <- Reduce(union, lapply(waves, names))
  
  # types per wave/var
  type_long <- purrr::imap_dfr(waves, function(df, wname) {
    tibble::tibble(
      wave  = wname,
      var   = names(df),
      class = vapply(df, function(x) class(x)[1], character(1))
    )
  })
  
  is_numericish_class <- function(cls) cls %in% c("numeric","integer","double","logical","Date","POSIXct","haven_labelled")
  decide_target <- function(v) {
    classes <- type_long$class[type_long$var == v]
    if (all(is_numericish_class(classes))) "numeric" else "character"
  }
  
  target <- tibble::tibble(
    var = all_vars,
    target_type = vapply(all_vars, decide_target, character(1))
  )
  
  cast_one <- function(df) {
    miss <- setdiff(all_vars, names(df))
    if (length(miss)) df[miss] <- NA
    for (v in all_vars) {
      ttype <- target$target_type[target$var == v]
      x <- df[[v]]
      if (ttype == "character") {
        if (!is.character(x)) {
          if (inherits(x, "haven_labelled")) x <- as.vector(x)
          if (is.factor(x)) x <- as.character(x)
          x <- as.character(x)
        }
      } else {
        if (!is.numeric(x)) {
          if (inherits(x, "haven_labelled")) x <- as.vector(x)
          if (is.factor(x)) x <- as.character(x)
          suppressWarnings(x <- as.numeric(x))
          if (exists("NA_CODES", inherits = FALSE) && length(NA_CODES)) {
            x[x %in% as.numeric(NA_CODES)] <- NA_real_
          }
        }
      }
      df[[v]] <- x
    }
    df
  }
  
  waves_cast <- purrr::map(waves_named, ~ if (is.null(.x)) NULL else cast_one(.x))
  
  summary <- type_long |>
    dplyr::group_by(var) |>
    dplyr::summarise(
      waves_present = paste(sort(unique(wave)), collapse = ","),
      n_types = dplyr::n_distinct(class),
      classes = paste(sort(unique(class)), collapse = ","),
      target_type = unique(target$target_type[target$var == var]),
      .groups = "drop"
    ) |>
    dplyr::filter(n_types > 1)
  
  list(waves = waves_cast, summary = summary)
}

harm <- harmonize_waves(list(baseline = hh_bl_pw, endline = hh_el_pw, midline = hh_ml_pw))
hh_bl_h <- harm$waves$baseline
hh_el_h <- harm$waves$endline
hh_ml_h <- harm$waves$midline
harm_summary <- harm$summary

if (!is.null(harm_summary) && nrow(harm_summary) > 0) {
  created_out <- c(created_out, .write_out(harm_summary, "type_harmonization_summary.csv"))
}

hh_raw_long <- dplyr::bind_rows(hh_bl_h, hh_el_h, hh_ml_h) |>
  dplyr::select(dplyr::everything())
if (nrow(hh_raw_long) > 0) {
  created_data <- c(created_data, .save_wave(hh_raw_long, "hh_raw_long.rds"))
}

# ---- 6) Variable catalog + exclusion logs --------------------------
# Date: 2025-09-23
# Purpose: catalog per wave; log stems/patterns and empty columns dropped
ex_val <- function(x, n = 5) {
  v <- unique(x[!is.na(x)])
  if (!length(v)) return("")
  paste(utils::head(v, n), collapse = " | ")
}
vardict_one <- function(df, wave) {
  if (is.null(df)) return(NULL)
  labs <- labelled::var_label(df)
  labv <- purrr::map_chr(labs, ~ ifelse(length(.x), as.character(.x), ""))
  tibble::tibble(
    wave        = wave,
    var         = names(df),
    label       = unname(labv),
    class       = vapply(df, function(x) class(x)[1], character(1)),
    n_missing   = vapply(df, function(x) sum(is.na(x)), integer(1)),
    pct_missing = round(vapply(df, function(x) mean(is.na(x)), numeric(1)) * 100, 1),
    n_unique    = vapply(df, function(x) dplyr::n_distinct(x, na.rm = TRUE), integer(1)),
    examples    = vapply(df, ex_val, character(1))
  ) |>
    dplyr::arrange(var) |>
    dplyr::select(dplyr::everything())
}

vardict <- dplyr::bind_rows(
  vardict_one(hh_bl_pw, "baseline"),
  vardict_one(hh_el_pw, "endline"),
  vardict_one(hh_ml_pw, "midline")
)
created_out <- c(created_out, .write_out(if (is.null(vardict)) tibble::tibble() else vardict, "hh_raw_variables_catalog.csv"))

# Stems used
created_out <- c(created_out, .write_out(tibble::tibble(stem = roster_stems), "roster_stems_used.csv"))
# Maternal candidates used (if any)
if (length(maternal_vars)) {
  created_out <- c(created_out, .write_out(tibble::tibble(var = maternal_vars), "maternal_candidates_excluded.csv"))
}

# Empty columns dropped per wave (already accumulated locally)
created_out <- c(created_out, .write_out(empties_log, "empty_columns_dropped_hh.csv"))


# ---- 7) Log and snapshot -------------------------------------------
append_log(
  step       = "04_build_household.R",
  code_files = "code/04_build_household.R",
  out_files  = created_out,
  data_files = created_data
)
write_structure_snapshot()

message("Step 04 complete: per-wave HH files saved (no wave-absent columns). Long file built with harmonization. Logs in /out/data_prep/04_household_raw/.")
