# -------------------------------------------------------------------
# 01_data_build.R - import raw survey files, clean IDs, merge panel
# Author: Dominik Schwarzkopf
# Date: 2025-08-21
#
# What this script does:
# 1) Loads project setup (packages, paths) from 00_setup.R
# 2) Reads baseline, endline, midline from DATA_DIR/data_raw using exact filenames
#    - .data files are tried as Stata first, else read as delimited text
# 3) Cleans names and standardizes key identifiers:
#    - hh_id, ind_id (optional), treat, strata
# 4) Validates uniqueness and removes exact duplicates
# 5) Builds a baseline-endline panel at chosen UNIT (household or individual)
# 6) Saves clean waves to DATA_DIR/data_clean and a summary CSV to out/tables
# -------------------------------------------------------------------

# ---- 1) Load setup -------------------------------------------------
source("code/00_setup.R")

# ---- 2) User choices -----------------------------------------------
# Set your primary unit of analysis
UNIT <- "household"          # or "individual"

# We have exact filenames
USE_EXACT_FILENAMES <- TRUE

# Exact filenames relative to PATH_RAW on SwitchDrive
F_BASELINE <- "MHMI_baseline_anon.data"
F_ENDLINE  <- "mhmi_endline_anon.data"
F_MIDLINE  <- "MHMI_midline_anon.dta"
F_MERGED   <- "MHMI_baseline_endline_anon_merged.dta"  # optional cross-check only

# ---- 3) Helpers ----------------------------------------------------

# Read any supported format, including .data
read_any <- function(path) {
  ext <- tolower(fs::path_ext(path))
  if (ext %in% c("dta"))  return(haven::read_dta(path))
  if (ext %in% c("sav"))  return(haven::read_sav(path))
  if (ext %in% c("csv"))  return(readr::read_csv(path, show_col_types = FALSE))
  if (ext %in% c("tsv"))  return(readr::read_tsv(path, show_col_types = FALSE))
  if (ext %in% c("xlsx","xls")) return(readxl::read_excel(path))
  
  if (ext %in% c("data")) {
    # 1) Try Stata via haven
    out <- try(haven::read_dta(path), silent = TRUE)
    if (!inherits(out, "try-error")) {
      message("Read as Stata (via haven): ", basename(path))
      return(out)
    }
    # 2) Fallback to delimited text with basic delimiter sniffing
    message("Falling back to delimited text for ", basename(path))
    first <- try(readr::read_lines(path, n_max = 1), silent = TRUE)
    if (inherits(first, "try-error")) {
      stop("Cannot read .data as text. Open the file to confirm its format or rename to .dta if it is Stata.")
    }
    delim <- if (grepl("\t", first)) "\t" else if (grepl(";", first)) ";" else ","
    return(readr::read_delim(path, delim = delim, show_col_types = FALSE, guess_max = 10000,
                             na = c("", "NA", ".")))
  }
  
  stop(glue::glue("Unknown extension: {ext} for {basename(path)}"))
}

# Return first existing column from a candidate list
pick_first <- function(df, candidates) {
  nm <- intersect(candidates, names(df))
  if (length(nm)) nm[1] else NA_character_
}

# Map treatment-like encodings to 0 or 1 robustly
norm_binary <- function(x) {
  if (is.null(x)) return(NULL)
  
  if (is.numeric(x)) {
    vals <- sort(unique(x[!is.na(x)]))
    if (all(vals %in% c(0,1))) return(as.integer(x))
    if (all(vals %in% c(1,2))) return(as.integer(ifelse(x == 1, 1L,
                                                        ifelse(x == 2, 0L, NA_integer_))))
    if (all(vals %in% c(0,2))) return(as.integer(ifelse(x == 2, 1L,
                                                        ifelse(x == 0, 0L, NA_integer_))))
    return(as.integer(x > 0))
  }
  
  if (is.factor(x)) x <- as.character(x)
  if (is.logical(x)) return(as.integer(x))
  
  if (is.character(x)) {
    pos <- c("1","t","trt","treat","treatment","yes","y","true","t1","treated")
    neg <- c("0","c","ctrl","control","no","n","false","untreated","t0","placebo")
    low <- tolower(trimws(x))
    out <- ifelse(low %in% pos, 1L, ifelse(low %in% neg, 0L, NA_integer_))
    return(as.integer(out))
  }
  
  stop("Could not normalize treatment variable to binary.")
}

# Clean a raw survey data frame and standardize key vars
clean_svy <- function(df) {
  df <- df |> janitor::clean_names()
  
  # Candidate names updated to include your baseline fields
  hh_cands   <- c("caseid","hhid","hh_id","household_id","householdid","id_household","hh","hid")
  ind_cands  <- c("ind_id","person_id","member_id","pid","id_person","individual_id")
  trt_cands  <- c("group","treat","treatment","arm","assignment","assigned","mhmi_treat","mhmi","trt")
  strat_cand <- c("a1_tsp","a2_ward","uni_cluster_id","strata","stratum","strata_id",
                  "township","ts","ts_code","ward","cluster")
  
  hh_col   <- pick_first(df, hh_cands)
  ind_col  <- pick_first(df, ind_cands)
  trt_col  <- pick_first(df, trt_cands)
  stra_col <- pick_first(df, strat_cand)
  
  if (is.na(hh_col)) {
    message("Available columns:\n", paste(names(df)[1:min(60, ncol(df))], collapse = ", "))
    stop("No household id column found. Update hh_cands in clean_svy().")
  }
  
  trt_raw_vals <- NULL
  if (!is.na(trt_col)) {
    trt_raw_vals <- unique(df[[trt_col]])
    trt_raw_vals <- utils::head(trt_raw_vals, 20)
  }
  
  df <- df |>
    dplyr::mutate(
      hh_id   = .data[[hh_col]],
      ind_id  = if (!is.na(ind_col)) .data[[ind_col]] else NA,
      strata  = if (!is.na(stra_col)) as.character(.data[[stra_col]]) else NA_character_,
      treat   = if (!is.na(trt_col)) norm_binary(.data[[trt_col]]) else NA_integer_
    ) |>
    dplyr::mutate(
      hh_id  = as.character(hh_id),
      ind_id = if (!all(is.na(ind_id))) as.character(ind_id) else ind_id
    ) |>
    dplyr::distinct()
  
  message(
    "Picked columns - hh_id: ", hh_col,
    if (!is.na(ind_col)) paste0(", ind_id: ", ind_col) else "",
    if (!is.na(stra_col)) paste0(", strata: ", stra_col) else "",
    if (!is.na(trt_col)) paste0(", treat: ", trt_col) else ""
  )
  if (!is.null(trt_raw_vals)) {
    message("Distinct raw values of treatment (up to 20): ",
            paste(trt_raw_vals, collapse = ", "))
  }
  
  return(df)
}

# ---- 4) Locate files -----------------------------------------------

path_or_na <- function(fname) {
  p <- fs::path(PATH_RAW, fname)
  if (fs::file_exists(p)) p else NA_character_
}

if (USE_EXACT_FILENAMES) {
  p_bl <- path_or_na(F_BASELINE)
  p_el <- path_or_na(F_ENDLINE)
  p_ml <- path_or_na(F_MIDLINE)
  p_mg <- path_or_na(F_MERGED)  # optional cross-check
} else {
  stop("We expect exact filenames. Set USE_EXACT_FILENAMES <- TRUE and set F_*.")
}

message("Detected files:")
message("  Baseline: ", ifelse(is.na(p_bl), "NONE", basename(p_bl)))
message("  Endline : ", ifelse(is.na(p_el), "NONE", basename(p_el)))
message("  Midline : ", ifelse(is.na(p_ml), "NONE", basename(p_ml)))
message("  Merged  : ", ifelse(is.na(p_mg), "NONE", basename(p_mg)))

if (is.na(p_bl) && is.na(p_el) && is.na(p_ml)) {
  stop("No raw files found. Place your survey files in data_raw and re-run.")
}

# ---- 5) Read and clean ---------------------------------------------

bl <- if (!is.na(p_bl)) read_any(p_bl) |> clean_svy() else NULL
el <- if (!is.na(p_el)) read_any(p_el) |> clean_svy() else NULL
# 2025-08-21: midline might not have treat; only read if file exists
ml <- if (!is.na(p_ml)) read_any(p_ml) |> clean_svy() else NULL
mg <- if (!is.na(p_mg)) read_any(p_mg) else NULL  # not cleaned, optional inspection only

# ---- 6) Key checks -------------------------------------------------

check_uniqueness <- function(df, unit) {
  if (is.null(df)) return(invisible(NULL))
  if (unit == "household") {
    dup <- df |> dplyr::count(hh_id) |> dplyr::filter(n > 1)
    if (nrow(dup) > 0) warning(nrow(dup), " households have multiple rows.")
  } else if (unit == "individual") {
    if (all(is.na(df$ind_id))) stop("UNIT is 'individual' but ind_id is missing in this wave.")
    dup <- df |> dplyr::count(hh_id, ind_id) |> dplyr::filter(n > 1)
    if (nrow(dup) > 0) warning(nrow(dup), " individuals have multiple rows.")
  } else {
    stop("UNIT must be 'household' or 'individual'.")
  }
}

if (!is.null(bl)) check_uniqueness(bl, UNIT)
if (!is.null(el)) check_uniqueness(el, UNIT)
if (!is.null(ml)) check_uniqueness(ml, UNIT)

# ---- 7) Save cleaned waves -----------------------------------------

if (!is.null(bl)) save_clean(bl, "baseline_clean.rds")
if (!is.null(el)) save_clean(el, "endline_clean.rds")
if (!is.null(ml)) save_clean(ml, "midline_clean.rds")

# ---- 8) Build panel -------------------------------------------------

panel <- NULL
if (!is.null(bl) && !is.null(el)) {
  if (UNIT == "household") {
    panel <- bl |>
      dplyr::select(hh_id, strata, treat, dplyr::everything()) |>
      dplyr::inner_join(
        el |> dplyr::select(hh_id, dplyr::everything()),
        by = "hh_id",
        suffix = c("_bl","_el")
      )
  } else if (UNIT == "individual") {
    panel <- bl |>
      dplyr::select(hh_id, ind_id, strata, treat, dplyr::everything()) |>
      dplyr::inner_join(
        el |> dplyr::select(hh_id, ind_id, dplyr::everything()),
        by = c("hh_id","ind_id"),
        suffix = c("_bl","_el")
      )
  }
  save_clean(panel, ifelse(UNIT == "household", "panel_hh.rds", "panel_indiv.rds"))
}

# ---- 9) Summary table ----------------------------------------------

summarize_wave <- function(df, label) {
  if (is.null(df)) return(NULL)
  tibble::tibble(
    dataset   = label,
    n_rows    = nrow(df),
    n_hh      = dplyr::n_distinct(df$hh_id),
    n_ind     = if (all(is.na(df$ind_id))) NA_integer_ else dplyr::n_distinct(df$ind_id),
    treat_na  = sum(is.na(df$treat)),
    treat_1   = sum(df$treat %in% 1, na.rm = TRUE),
    treat_0   = sum(df$treat %in% 0, na.rm = TRUE)
  )
}

summary_tbl <- dplyr::bind_rows(
  summarize_wave(bl, "baseline"),
  summarize_wave(el, "endline"),
  summarize_wave(ml, "midline"),
  if (!is.null(panel)) summarize_wave(panel, "panel")
)

if (!is.null(summary_tbl)) {
  write_table(summary_tbl, "data_build_summary.csv", stamp = FALSE)
  print(summary_tbl)
}

message("Data build complete.")
