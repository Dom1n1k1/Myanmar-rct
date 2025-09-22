# -------------------------------------------------------------------
# 02_schema_scan.R — scan waves for schema, labels, blocks, candidates
# Author: Dominik Schwarzkopf
# Date: 2025-09-16
#
# What this script does:
# 1) Load setup and read per-wave .rds from DATA_DIR/01_waves_raw
# 2) Write variable dictionary per wave (types, missingness, uniques, examples)
# 3) Dump variable labels and value labels per wave
# 4) Detect repeated wide blocks (name_1, name_2, ... or name1, name2)
# 5) Detect multi-select candidates (space-separated numeric codes)
# 6) Search names and labels for key outcome families and write candidate lists
# 7) Append outputs to /docs/data_flow_log.md and refresh repo snapshot
# -------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(purrr)
  library(readr); library(tibble); library(labelled); library(fs)
  library(glue)
})

# ---- 1) Load setup -------------------------------------------------
source("code/00_setup.R")

# ---- 2) IO paths ---------------------------------------------------
IN_DIR  <- PATH_DATA$`01_waves_raw`
OUT_DIR <- PATH_OUT_DP$`02_schema_scan`
fs::dir_create(OUT_DIR, recurse = TRUE)

# Helper to write CSVs consistently
write_here <- function(df, fname) {
  stopifnot(is.data.frame(df))
  fpath <- fs::path(OUT_DIR, fname)
  readr::write_csv(df, fpath, na = "")
  return(as.character(fpath))
}

# ---- 3) Read waves produced by Step 1 ------------------------------
read_wave_or_null <- function(fname) {
  p <- fs::path(IN_DIR, fname)
  if (fs::file_exists(p)) readRDS(p) else NULL
}

waves <- list(
  baseline = read_wave_or_null("01_baseline_raw.rds"),
  endline  = read_wave_or_null("01_endline_raw.rds"),
  midline  = read_wave_or_null("01_midline_raw.rds")
)

# Early guard: still produce empty artifacts if nothing to scan
created_out <- character(0)

if (length(compact(waves)) == 0) {
  empty <- tibble()
  created_out <- c(
    write_here(empty, "schema_variables.csv"),
    write_here(empty, "labels_baseline.csv"),
    write_here(empty, "labels_endline.csv"),
    write_here(empty, "labels_midline.csv"),
    write_here(empty, "value_labels_baseline.csv"),
    write_here(empty, "value_labels_endline.csv"),
    write_here(empty, "value_labels_midline.csv"),
    write_here(empty, "schema_repeated_blocks.csv"),
    write_here(empty, "schema_multiselect_candidates.csv"),
    write_here(empty, "schema_candidates_utilization.csv"),
    write_here(empty, "schema_candidates_finance.csv"),
    write_here(empty, "schema_candidates_maternal.csv"),
    write_here(empty, "schema_candidates_child.csv"),
    write_here(empty, "schema_candidates_covid.csv")
  )
  append_log(
    step = "02_schema_scan.R",
    code_files = "code/02_schema_scan.R",
    out_files = created_out
  )
  write_structure_snapshot()
  quit(save = "no")
}

# ---- 4) Small helpers ----------------------------------------------
ex_val <- function(x, n = 5) {
  v <- unique(x[!is.na(x)])
  if (length(v) == 0) return("")
  paste(utils::head(v, n), collapse = " | ")
}

# Variable dictionary per wave
vardict_one <- function(df, wave) {
  if (is.null(df)) return(NULL)
  labs <- labelled::var_label(df)
  labv <- purrr::map_chr(labs, ~ ifelse(length(.x), as.character(.x), ""))
  tibble(
    wave        = wave,
    name        = names(df),
    label       = unname(labv),
    type        = vapply(df, function(x) class(x)[1], character(1)),
    n_missing   = vapply(df, function(x) sum(is.na(x)), integer(1)),
    pct_missing = round(vapply(df, function(x) mean(is.na(x)), numeric(1)) * 100, 1),
    n_unique    = vapply(df, function(x) dplyr::n_distinct(x, na.rm = TRUE), integer(1)),
    examples    = vapply(df, ex_val, character(1))
  )
}

# Multi-select detection
detect_multiselect <- function(df, wave) {
  if (is.null(df)) return(NULL)
  is_char <- vapply(df, is.character, logical(1))
  if (!any(is_char)) return(NULL)
  chr_df <- df[is_char]
  nm <- names(chr_df)
  pat_multi  <- "^\\s*\\d+(\\s+\\d+)+\\s*$"
  pat_single <- "^\\s*\\d+\\s*$"
  res <- purrr::map2_chr(chr_df, nm, function(x, v) {
    x <- x[!is.na(x)]
    if (length(x) == 0) return("none")
    share_multi  <- mean(grepl(pat_multi, x))
    share_single <- mean(grepl(pat_single, x))
    if (is.nan(share_multi))  share_multi  <- 0
    if (is.nan(share_single)) share_single <- 0
    if (share_multi >= 0.30) return("multi_select")
    if (share_single >= 0.80) return("single_code")
    "none"
  })
  tibble(
    wave = wave,
    var = names(res)[res %in% c("multi_select","single_code")],
    detected = unname(res[res %in% c("multi_select","single_code")])
  )
}

# Dump variable labels and value labels
dump_varlabels <- function(df, wave) {
  if (is.null(df)) return(invisible(NULL))
  labs <- labelled::var_label(df)
  tibble(
    wave  = wave,
    var   = names(df),
    label = purrr::map_chr(labs, ~ ifelse(length(.x), as.character(.x), ""))
  )
}

dump_vallabels <- function(df, wave) {
  if (is.null(df)) return(invisible(NULL))
  purrr::imap_dfr(df, function(x, nm) {
    vl <- labelled::val_labels(x)
    if (length(vl)) {
      tibble(
        wave  = wave,
        var   = nm,
        value = suppressWarnings(as.numeric(vl)),
        value_label = names(vl)
      )
    } else NULL
  })
}

# Repeated block detection
blockscan_one <- function(df, wave) {
  if (is.null(df)) return(NULL)
  nm <- names(df)
  m  <- stringr::str_match(nm, "^(.*?)(?:_|)([0-9]+)$")
  ok <- !is.na(m[,1]) & !is.na(m[,3])
  if (!any(ok)) return(NULL)
  stems <- m[ok,2]
  idx   <- suppressWarnings(as.integer(m[ok,3]))
  tibble(stem = stems, idx = idx, var = nm[ok]) |>
    dplyr::group_by(stem) |>
    dplyr::summarise(
      n_indices   = dplyr::n_distinct(idx),
      min_idx     = min(idx, na.rm = TRUE),
      max_idx     = max(idx, na.rm = TRUE),
      sample_vars = paste(utils::head(sort(unique(var)), 10), collapse = ", "),
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(n_indices)) |>
    dplyr::mutate(wave = wave, .before = 1) |>
    dplyr::select(dplyr::everything())
}

# Targeted search patterns
pat <- list(
  utilization = "(tele|telemed|hotline|call|consult|gp|outpatient|clinic|visit|primary)",
  finance     = "(oop|out[-_ ]?of[-_ ]?pocket|expense|expenditure|cost|paid|pay|spent|bill|fee)",
  maternal    = "(anc|prenatal|preg|pregnancy|delivery|birth|postnatal|pnc)",
  child       = "(child|under7|u_?7|immuni|vaccine|vaccin|dose|card)",
  covid       = "(covid|corona|sars|vax|vaccin)"
)

find_candidates <- function(df, wave, regex_pat, vardict_ref) {
  if (is.null(df)) return(NULL)
  labs <- labelled::var_label(df)
  labv <- purrr::map_chr(labs, ~ ifelse(length(.x), as.character(.x), ""))
  nm   <- names(df)
  rr   <- stringr::regex(regex_pat, ignore_case = TRUE)
  hit  <- stringr::str_detect(nm, rr) | stringr::str_detect(labv, rr)
  if (!any(hit)) return(NULL)
  tibble(
    wave  = wave,
    var   = nm[hit],
    label = labv[hit]
  ) |>
    dplyr::left_join(
      vardict_ref |> dplyr::select(dplyr::all_of(c("wave","name","type","n_missing","pct_missing","n_unique","examples"))),
      by = c("wave" = "wave", "var" = "name")
    ) |>
    dplyr::arrange(var) |>
    dplyr::select(dplyr::everything())
}

# ---- 5) Build artifacts --------------------------------------------
vardict <- dplyr::bind_rows(
  vardict_one(waves$baseline, "baseline"),
  vardict_one(waves$endline,  "endline"),
  vardict_one(waves$midline,  "midline")
)

if (is.null(vardict) || nrow(vardict) == 0) {
  created_out <- c(created_out, write_here(tibble(), "schema_variables.csv"))
} else {
  created_out <- c(created_out, write_here(vardict, "schema_variables.csv"))
}

# Multi-select candidates
ms_hits <- dplyr::bind_rows(
  detect_multiselect(waves$baseline, "baseline"),
  detect_multiselect(waves$endline,  "endline"),
  detect_multiselect(waves$midline,  "midline")
)
created_out <- c(created_out, write_here(if (is.null(ms_hits)) tibble() else ms_hits, "schema_multiselect_candidates.csv"))

# Labels per wave
lab_bl <- dump_varlabels(waves$baseline, "baseline"); created_out <- c(created_out, write_here(if (is.null(lab_bl)) tibble() else lab_bl, "labels_baseline.csv"))
lab_el <- dump_varlabels(waves$endline,  "endline");  created_out <- c(created_out, write_here(if (is.null(lab_el)) tibble() else lab_el, "labels_endline.csv"))
lab_ml <- dump_varlabels(waves$midline,  "midline");  created_out <- c(created_out, write_here(if (is.null(lab_ml)) tibble() else lab_ml, "labels_midline.csv"))

# Value labels per wave
vabl_bl <- dump_vallabels(waves$baseline, "baseline"); created_out <- c(created_out, write_here(if (is.null(vabl_bl)) tibble() else vabl_bl, "value_labels_baseline.csv"))
vabl_el <- dump_vallabels(waves$endline,  "endline");  created_out <- c(created_out, write_here(if (is.null(vabl_el)) tibble() else vabl_el, "value_labels_endline.csv"))
vabl_ml <- dump_vallabels(waves$midline,  "midline");  created_out <- c(created_out, write_here(if (is.null(vabl_ml)) tibble() else vabl_ml, "value_labels_midline.csv"))

# Repeated block guess
blocks <- dplyr::bind_rows(
  blockscan_one(waves$baseline, "baseline"),
  blockscan_one(waves$endline,  "endline"),
  blockscan_one(waves$midline,  "midline")
)
created_out <- c(created_out, write_here(if (is.null(blocks)) tibble() else blocks, "schema_repeated_blocks.csv"))

# Candidate searches
write_family <- function(regex_pat, fam_name) {
  out <- dplyr::bind_rows(
    find_candidates(waves$baseline, "baseline", regex_pat, vardict),
    find_candidates(waves$endline,  "endline",  regex_pat, vardict),
    find_candidates(waves$midline,  "midline",  regex_pat, vardict)
  )
  created_out <<- c(created_out, write_here(if (is.null(out)) tibble() else out, glue("schema_candidates_{fam_name}.csv")))
}

purrr::iwalk(pat, write_family)

message("Schema scan complete. See: ", OUT_DIR)

# ---- 6) Log and snapshot -------------------------------------------
append_log(
  step = "02_schema_scan.R",
  code_files = "code/02_schema_scan.R",
  out_files = created_out
)
write_structure_snapshot()
