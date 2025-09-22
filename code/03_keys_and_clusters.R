# -------------------------------------------------------------------
# 03_keys_and_clusters.R — define cluster_id/strata and crosswave keys
# Author: Dominik Schwarzkopf
# Date: 2025-09-16
#
# What this script does
# 1) Load per-wave raw .rds from DATA_DIR/01_waves_raw (Step 1 outputs)
# 2) Build authoritative baseline mapping by hh_id:
#      cluster_id <- uni_cluster_id (fallback a2_ward)
#      strata     <- a1_tsp
#      treat      <- cluster-level baseline treatment
# 3) Merge that mapping onto endline and midline by hh_id, with optional fill
# 4) Write diagnostics to /out/data_prep/03_keys_and_clusters/
# 5) Save /data/03_keys_and_clusters/keys.rds
# 6) Append outputs to docs/data_flow_log.md and refresh repo_structure_snapshot.md
# -------------------------------------------------------------------

# ---- 0) Setup ------------------------------------------------------
# What this section does:
# - Load project setup (paths, helpers)
# - Load required packages quietly
# - Define IO helpers used below
# Date: 2025-09-16

source("code/00_setup.R")
suppressPackageStartupMessages({
  library(dplyr); library(readr); library(stringr); library(tidyr); library(purrr); library(tibble); library(fs); library(glue)
})

IN_DIR   <- PATH_DATA$`01_waves_raw`
OUT_DIR  <- PATH_OUT_DP$`03_keys_and_clusters`
DATA_OUT <- PATH_DATA$`03_keys_and_clusters`
fs::dir_create(OUT_DIR, recurse = TRUE)
fs::dir_create(DATA_OUT, recurse = TRUE)

.write_out <- function(df, name) {
  stopifnot(is.data.frame(df))
  path <- fs::path(OUT_DIR, name)
  readr::write_csv(df, path, na = "")
  as.character(path)
}

.read_wave_or_null <- function(fname) {
  p <- fs::path(IN_DIR, fname)
  if (fs::file_exists(p)) readRDS(p) else NULL
}

# ---- 1) Read waves -------------------------------------------------
# What this section does:
# - Read baseline, endline, midline .rds created by 01_data_build.R
# - Stop if baseline is missing (cannot define clusters without it)
# Date: 2025-09-16

bl <- .read_wave_or_null("01_baseline_raw.rds")
el <- .read_wave_or_null("01_endline_raw.rds")
ml <- .read_wave_or_null("01_midline_raw.rds")

if (is.null(bl)) stop("Baseline is required to define clusters and treatment. Run 01_data_build.R first.")

# ---- 2) Authoritative baseline mapping by hh_id --------------------
# What this section does:
# - Pick cluster_id from uni_cluster_id, fallback a2_ward
# - Pick strata from a1_tsp
# - Keep baseline treat at the household level (may contain NA)
# - Collapse to cluster-level treat using a strict rule:
#     * if all non-missing are 1 -> 1
#     * if all non-missing are 0 -> 0
#     * mixed or no non-missing -> NA and flagged in conflicts
# Date: 2025-09-16

derive_ids_baseline <- function(df) {
  nm <- names(df)
  if (!"hh_id" %in% nm) stop("hh_id missing in baseline.")
  cluster_vec <- if ("uni_cluster_id" %in% nm) as.character(df[["uni_cluster_id"]])
  else if ("a2_ward" %in% nm)   as.character(df[["a2_ward"]])
  else                          rep(NA_character_, nrow(df))
  strata_vec  <- if ("a1_tsp" %in% nm) as.character(df[["a1_tsp"]]) else NA_character_
  tibble::tibble(
    hh_id      = as.character(df[["hh_id"]]),
    cluster_id = cluster_vec,
    strata     = strata_vec,
    treat_raw  = if ("treat" %in% nm) as.integer(df[["treat"]]) else NA_integer_
  ) |>
    dplyr::distinct() |>
    dplyr::select(dplyr::everything())
}

bl_ids <- derive_ids_baseline(bl)

clust_treat <- bl_ids |>
  dplyr::group_by(cluster_id) |>
  dplyr::summarise(
    n_hh = dplyr::n(),
    n1   = sum(treat_raw %in% 1L, na.rm = TRUE),
    n0   = sum(treat_raw %in% 0L, na.rm = TRUE),
    n_na = sum(is.na(treat_raw)),
    treat = dplyr::case_when(
      n1 > 0L & n0 == 0L ~ 1L,
      n0 > 0L & n1 == 0L ~ 0L,
      n1 == 0L & n0 == 0L ~ NA_integer_,
      TRUE ~ NA_integer_
    ),
    conflict = n1 > 0L & n0 > 0L,
    .groups = "drop"
  ) |>
  dplyr::select(dplyr::everything())

conflicts <- clust_treat |>
  dplyr::filter(conflict | is.na(treat)) |>
  dplyr::select(dplyr::everything())

created_out <- character(0)
if (nrow(conflicts) > 0) {
  created_out <- c(created_out, .write_out(conflicts, "keys_treatment_conflicts.csv"))
  warning(nrow(conflicts), " clusters have conflicting or missing baseline treatment. See out/data_prep/03_keys_and_clusters/keys_treatment_conflicts.csv")
}

bl_keys <- bl_ids |>
  dplyr::select(dplyr::all_of(c("hh_id","cluster_id","strata"))) |>
  dplyr::left_join(clust_treat |> dplyr::select(dplyr::all_of(c("cluster_id","treat"))), by = "cluster_id") |>
  dplyr::mutate(wave = "baseline") |>
  dplyr::distinct() |>
  dplyr::select(dplyr::all_of(c("hh_id","cluster_id","strata","wave","treat")))

# ---- 3) Merge mapping to EL/ML with optional fill ------------------
# What this section does:
# - For each non-baseline wave, take distinct hh_id and left-join baseline keys
# - Optionally fill cluster_id from a2_ward if baseline join was NA
# - Tag the wave and keep tidy columns
# Date: 2025-09-16

inherit_keys <- function(df, wave_label) {
  if (is.null(df)) return(NULL)
  nm <- names(df)
  res <- tibble::tibble(hh_id = if ("hh_id" %in% nm) as.character(df[["hh_id"]]) else NA_character_) |>
    dplyr::distinct() |>
    dplyr::left_join(
      bl_keys |> dplyr::select(dplyr::all_of(c("hh_id","cluster_id","strata","treat"))),
      by = "hh_id"
    )
  if ("a2_ward" %in% nm) {
    res <- res |>
      dplyr::mutate(cluster_id = ifelse(is.na(cluster_id), as.character(df[["a2_ward"]]), cluster_id)) |>
      dplyr::select(dplyr::everything())
  }
  res |>
    dplyr::mutate(wave = wave_label) |>
    dplyr::select(dplyr::all_of(c("hh_id","cluster_id","strata","wave","treat")))
}

el_keys <- inherit_keys(el, "endline")
ml_keys <- inherit_keys(ml, "midline")

keys_all <- dplyr::bind_rows(bl_keys, el_keys, ml_keys) |>
  dplyr::distinct() |>
  dplyr::select(dplyr::everything())

# ---- 4) Diagnostics ------------------------------------------------
# What this section does:
# - New HHs in EL/ML not found at baseline
# - Multiple non-missing treat values per cluster after inheritance
# - Summary counts by wave
# Date: 2025-09-16

new_hh <- dplyr::bind_rows(
  if (!is.null(el_keys)) el_keys |> dplyr::filter(is.na(treat)) else NULL,
  if (!is.null(ml_keys)) ml_keys |> dplyr::filter(is.na(treat)) else NULL
)
if (!is.null(new_hh) && nrow(new_hh) > 0) {
  created_out <- c(created_out, .write_out(new_hh, "keys_new_households_postbaseline.csv"))
  message(nrow(new_hh), " HHs in endline/midline not found at baseline. See out/data_prep/03_keys_and_clusters/keys_new_households_postbaseline.csv")
}

multi_treat <- keys_all |>
  dplyr::filter(!is.na(cluster_id)) |>
  dplyr::group_by(cluster_id) |>
  dplyr::summarise(n_treat_vals = dplyr::n_distinct(treat, na.rm = TRUE), .groups = "drop") |>
  dplyr::filter(n_treat_vals > 1) |>
  dplyr::select(dplyr::everything())
if (nrow(multi_treat) > 0) {
  created_out <- c(created_out, .write_out(multi_treat, "keys_clusters_multiple_treat_values.csv"))
  warning("Some clusters have multiple non-missing treat values after inheritance.")
}

keys_summary <- keys_all |>
  dplyr::group_by(wave) |>
  dplyr::summarise(
    n_rows     = dplyr::n(),
    n_hh       = dplyr::n_distinct(hh_id, na.rm = TRUE),
    n_clusters = dplyr::n_distinct(cluster_id, na.rm = TRUE),
    treat_na   = sum(is.na(treat)),
    .groups = "drop"
  ) |>
  dplyr::select(dplyr::everything())
created_out <- c(created_out, .write_out(keys_summary, "keys_summary.csv"))

# Record which source columns were picked on baseline
picked_vars <- tibble::tibble(
  wave = "baseline",
  picked_cluster_var = if ("uni_cluster_id" %in% names(bl)) "uni_cluster_id" else if ("a2_ward" %in% names(bl)) "a2_ward" else "(none)",
  picked_strata_var  = if ("a1_tsp" %in% names(bl)) "a1_tsp" else "(none)"
)
created_out <- c(created_out, .write_out(picked_vars, "keys_variable_picks.csv"))

# ---- 5) Save keys --------------------------------------------------
# What this section does:
# - Save keys.rds to /data/03_keys_and_clusters/
# - Guard against empty result
# Date: 2025-09-16

if (nrow(keys_all) == 0) stop("No keys constructed.")
keys_path <- fs::path(DATA_OUT, "keys.rds")
saveRDS(keys_all, keys_path)

# ---- 6) Log and snapshot -------------------------------------------
# What this section does:
# - Append outputs to /docs/data_flow_log.md
# - Refresh /docs/repo_structure_snapshot.md
# Date: 2025-09-16

append_log(
  step = "03_keys_and_clusters.R",
  code_files = "code/03_keys_and_clusters.R",
  out_files  = created_out,
  data_files = keys_path
)
write_structure_snapshot()

message("Step 03 complete: keys saved to /data/03_keys_and_clusters/keys.rds; diagnostics in /out/data_prep/03_keys_and_clusters/")
