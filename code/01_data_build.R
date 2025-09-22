# -------------------------------------------------------------------
# 01_data_build.R — import original survey files → per-wave raw .rds
# Author: Dominik Schwarzkopf
# Date: 2025-08-28
#
# What this script does:
# 1) Load setup (packages, paths, helpers) from 00_setup.R
# 2) Read baseline, endline, midline from DATA_DIR/00_data_original
#    - For .data: try haven::read_dta(); if that fails, read as delimited text
# 3) Clean names; standardize hh_id, optional ind_id, strata; normalize treatment
#    - Source-of-truth for treatment here is `group` (kept as group_raw); normalized to `treat` ∈ {0,1}
# 4) Drop columns that contain PII placeholders in their cell values
#    (e.g., "PII Anonymized Data", "Deidentifiaction"/"Deidentification")
# 5) Drop completely empty variables (all NA or blanks) and log them
# 6) Validate uniqueness at chosen UNIT
# 7) Save wave-level .rds to DATA_DIR/01_waves_raw
# 8) Write diagnostics to out/data_prep/01_data_build
# 9) Append to docs/data_flow_log.md and refresh repo_structure_snapshot.md
# -------------------------------------------------------------------

# ---- 1) Load setup -------------------------------------------------
source("code/00_setup.R")

# ---- 2) User choices -----------------------------------------------
UNIT <- "household"                 # or "individual"
USE_EXACT_FILENAMES <- TRUE         # expect exact filenames in 00_data_original
REMOVE_PII <- TRUE                  # drop columns with PII placeholders found in cell values
DROP_EMPTY_VARS <- TRUE             # drop columns that are completely empty (all NA/blank)

# Filenames as provided by IPA (edit if they change upstream)
F_BASELINE <- "MHMI_baseline_anon.data"
F_ENDLINE  <- "mhmi_endline_anon.data"
F_MIDLINE  <- "MHMI_midline_anon.dta"

# Patterns that signal anonymized PII content inside cells
# - covers "PII Anonymized", "Deidentifiaction" (misspelling), "Deidentification"
PII_REGEX <- "(?i)(pii\\s*anonymized|deidentif)"

# ---- 3) Helpers: I/O & cleaning -----------------------------------
read_any <- function(path) {
  ext <- tolower(fs::path_ext(path))
  if (ext %in% c("dta"))   return(haven::read_dta(path))
  if (ext %in% c("sav"))   return(haven::read_sav(path))
  if (ext %in% c("csv"))   return(readr::read_csv(path, show_col_types = FALSE))
  if (ext %in% c("tsv"))   return(readr::read_tsv(path, show_col_types = FALSE))
  if (ext %in% c("xlsx","xls")) return(readxl::read_excel(path))
  if (ext %in% c("data")) {
    as_dta <- try(haven::read_dta(path), silent = TRUE)  # many .data are actually Stata
    if (!inherits(as_dta, "try-error")) {
      message("Read as Stata via haven: ", basename(path))
      return(as_dta)
    }
    message("Falling back to delimited text for ", basename(path))
    first <- try(readr::read_lines(path, n_max = 1), silent = TRUE)
    if (inherits(first, "try-error")) stop("Cannot read .data as text; confirm format or rename to .dta.")
    delim <- if (grepl("\t", first)) "\t" else if (grepl(";", first)) ";" else ","
    return(readr::read_delim(
      path, delim = delim, show_col_types = FALSE, guess_max = 10000,
      na = c("", "NA", ".", as.character(NA_CODES))
    ))
  }
  stop(glue::glue("Unknown extension: {ext} for {basename(path)}"))
}

pick_first <- function(df, candidates) {
  nm <- intersect(candidates, names(df))
  if (length(nm)) nm[1] else NA_character_
}

norm_binary <- function(x) {
  if (is.null(x)) return(NULL)
  if (is.numeric(x)) {
    vals <- sort(unique(x[!is.na(x)]))
    if (all(vals %in% c(0,1))) return(as.integer(x))
    if (all(vals %in% c(1,2))) return(as.integer(ifelse(x == 1, 1L, ifelse(x == 2, 0L, NA_integer_))))
    if (all(vals %in% c(0,2))) return(as.integer(ifelse(x == 2, 1L, ifelse(x == 0, 0L, NA_integer_))))
    warning("norm_binary(): unusual numeric values: ", paste(vals, collapse = ", "))
    return(as.integer(x > 0))
  }
  if (is.factor(x))  x <- as.character(x)
  if (is.logical(x)) return(as.integer(x))
  if (is.character(x)) {
    pos <- c("1","t","trt","treat","treatment","yes","y","true","t1","treated")
    neg <- c("0","c","ctrl","control","no","n","false","untreated","t0","placebo")
    low <- tolower(trimws(x))
    return(as.integer(ifelse(low %in% pos, 1L, ifelse(low %in% neg, 0L, NA_integer_))))
  }
  stop("Could not normalize binary-like variable.")
}

clean_svy <- function(df) {
  df <- df |> janitor::clean_names()
  hh_cands   <- c("caseid","hhid","hh_id","household_id","householdid","id_household","hh","hid")
  ind_cands  <- c("ind_id","person_id","member_id","pid","id_person","individual_id")
  strat_cand <- c("a1_tsp","strata","stratum","strata_id","township","ts","ts_code")
  trt_cands  <- c("group","treat","treatment","arm","assignment","assigned","mhmi_treat","mhmi","trt")
  
  hh_col   <- pick_first(df, hh_cands)
  ind_col  <- pick_first(df, ind_cands)
  trt_col  <- pick_first(df, trt_cands)
  stra_col <- pick_first(df, strat_cand)
  
  if (is.na(hh_col)) {
    message("Available columns:\n", paste(names(df)[1:min(60, ncol(df))], collapse = ", "))
    stop("No household id column found. Update hh_cands in clean_svy().")
  }
  
  trt_raw_vals <- NULL
  if (!is.na(trt_col)) trt_raw_vals <- utils::head(unique(df[[trt_col]]), 20)
  
  out <- df |>
    dplyr::mutate(
      hh_id     = .data[[hh_col]],
      ind_id    = if (!is.na(ind_col)) .data[[ind_col]] else NA,
      strata    = if (!is.na(stra_col)) as.character(.data[[stra_col]]) else NA_character_,
      group_raw = if (!is.na(trt_col)) .data[[trt_col]] else NA,
      treat     = if (!is.na(trt_col)) norm_binary(.data[[trt_col]]) else NA_integer_
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
    if (!is.na(trt_col)) paste0(", group/treat source: ", trt_col) else ""
  )
  if (!is.null(trt_raw_vals)) {
    message("Distinct raw values in group/treat (up to 20): ", paste(trt_raw_vals, collapse = ", "))
  }
  out
}

# ---- 4) PII placeholder detection ----------------------------------
detect_pii_columns <- function(df, pattern = PII_REGEX, sample_n = NULL) {
  if (!is.null(sample_n) && nrow(df) > sample_n) {
    df_chk <- df[sample(seq_len(nrow(df)), sample_n), , drop = FALSE]
  } else df_chk <- df
  
  cols <- names(df_chk)
  flagged <- logical(length(cols))
  for (j in seq_along(cols)) {
    v <- df_chk[[j]]
    if (inherits(v, "labelled")) v <- haven::as_factor(v)
    if (is.factor(v)) v <- as.character(v)
    if (is.character(v)) {
      if (any(grepl(pattern, v, perl = TRUE))) flagged[j] <- TRUE
    }
  }
  cols[flagged]
}

# ---- 5) Drop completely empty variables ----------------------------
detect_empty_columns <- function(df) {
  if (is.null(df)) return(character(0))
  names(df)[vapply(
    df,
    function(x) {
      all(is.na(x) | (is.character(x) & trimws(x) == ""))
    },
    logical(1)
  )]
}

# ---- 6) Locate files in 00_data_original ---------------------------
path_or_na <- function(fname) {
  p <- fs::path(PATH_DATA$`00_data_original`, fname)
  if (fs::file_exists(p)) p else NA_character_
}

if (USE_EXACT_FILENAMES) {
  p_bl <- path_or_na(F_BASELINE)
  p_el <- path_or_na(F_ENDLINE)
  p_ml <- path_or_na(F_MIDLINE)
} else stop("We expect exact filenames. Set USE_EXACT_FILENAMES <- TRUE and set F_*.")

message("Detected files in 00_data_original:")
message("  Baseline: ", ifelse(is.na(p_bl), "NONE", basename(p_bl)))
message("  Endline : ", ifelse(is.na(p_el), "NONE", basename(p_el)))
message("  Midline : ", ifelse(is.na(p_ml), "NONE", basename(p_ml)))

if (is.na(p_bl) && is.na(p_el) && is.na(p_ml)) {
  stop("No raw files found. Place original files in /data/00_data_original and re-run.")
}

# ---- 7) Read, clean ------------------------------------------------
bl <- if (!is.na(p_bl)) read_any(p_bl) |> clean_svy() else NULL
el <- if (!is.na(p_el)) read_any(p_el) |> clean_svy() else NULL
ml <- if (!is.na(p_ml)) read_any(p_ml) |> clean_svy() else NULL

# ---- 8) Drop PII-placeholder columns -------------------------------
pii_log <- tibble::tibble(wave = character(), column = character())

drop_pii <- function(df, wave_label) {
  if (is.null(df) || !REMOVE_PII) return(list(df = df, dropped = character(0)))
  cols <- detect_pii_columns(df, pattern = PII_REGEX)
  if (length(cols)) {
    message(wave_label, ": dropping ", length(cols), " PII-placeholder column(s): ", paste(cols, collapse = ", "))
    df <- dplyr::select(df, -dplyr::any_of(cols))
  }
  list(df = df, dropped = cols)
}

tmp <- drop_pii(bl, "baseline"); bl <- tmp$df
if (length(tmp$dropped)) pii_log <- dplyr::bind_rows(pii_log, tibble::tibble(wave = "baseline", column = tmp$dropped))
tmp <- drop_pii(el, "endline");  el <- tmp$df
if (length(tmp$dropped)) pii_log <- dplyr::bind_rows(pii_log, tibble::tibble(wave = "endline",  column = tmp$dropped))
tmp <- drop_pii(ml, "midline");  ml <- tmp$df
if (length(tmp$dropped)) pii_log <- dplyr::bind_rows(pii_log, tibble::tibble(wave = "midline",  column = tmp$dropped))

# ---- 9) Drop completely empty variables ----------------------------
empty_log <- tibble::tibble(wave = character(), column = character())

drop_empty_vars <- function(df, wave_label) {
  if (is.null(df) || !DROP_EMPTY_VARS) return(list(df = df, dropped = character(0)))
  cols <- detect_empty_columns(df)
  if (length(cols)) {
    message(wave_label, ": dropping ", length(cols), " completely empty column(s): ", paste(cols, collapse = ", "))
    df <- dplyr::select(df, -dplyr::any_of(cols))
  }
  list(df = df, dropped = cols)
}

tmp <- drop_empty_vars(bl, "baseline"); bl <- tmp$df
if (length(tmp$dropped)) empty_log <- dplyr::bind_rows(empty_log, tibble::tibble(wave = "baseline", column = tmp$dropped))
tmp <- drop_empty_vars(el, "endline");  el <- tmp$df
if (length(tmp$dropped)) empty_log <- dplyr::bind_rows(empty_log, tibble::tibble(wave = "endline",  column = tmp$dropped))
tmp <- drop_empty_vars(ml, "midline");  ml <- tmp$df
if (length(tmp$dropped)) empty_log <- dplyr::bind_rows(empty_log, tibble::tibble(wave = "midline",  column = tmp$dropped))

# ---- 10) Key checks ------------------------------------------------
check_uniqueness <- function(df, unit) {
  if (is.null(df)) return(invisible(NULL))
  if (unit == "household") {
    dup <- df |> dplyr::count(hh_id) |> dplyr::filter(n > 1)
    if (nrow(dup) > 0) warning(nrow(dup), " households have multiple rows.")
  } else if (unit == "individual") {
    if (all(is.na(df$ind_id))) stop("UNIT is 'individual' but ind_id is missing in this wave.")
    dup <- df |> dplyr::count(hh_id, ind_id) |> dplyr::filter(n > 1)
    if (nrow(dup) > 0) warning(nrow(dup), " individuals have multiple rows.")
  } else stop("UNIT must be 'household' or 'individual'.")
}
if (!is.null(bl)) check_uniqueness(bl, UNIT)
if (!is.null(el)) check_uniqueness(el, UNIT)
if (!is.null(ml)) check_uniqueness(ml, UNIT)

# ---- 11) Save per-wave cleaned raw files ---------------------------
created_data <- character(0)

if (!is.null(bl)) {
  f <- save_rds_step(bl, "01_baseline_raw.rds", PATH_DATA$`01_waves_raw`)
  created_data <- c(created_data, f)
}
if (!is.null(el)) {
  f <- save_rds_step(el, "01_endline_raw.rds", PATH_DATA$`01_waves_raw`)
  created_data <- c(created_data, f)
}
if (!is.null(ml)) {
  f <- save_rds_step(ml, "01_midline_raw.rds", PATH_DATA$`01_waves_raw`)
  created_data <- c(created_data, f)
}

# ---- 12) Diagnostics: summary, schema, label counts ----------------
summary_wave <- function(df, label) {
  if (is.null(df)) return(NULL)
  t <- if ("treat" %in% names(df)) df[["treat"]] else NULL
  if (!is.null(t)) {
    if (is.logical(t)) t <- as.integer(t)
    if (is.factor(t))  t <- as.character(t)
    if (is.character(t)) {
      low <- tolower(trimws(t))
      t <- ifelse(low %in% c("1","yes","true","t","treat","treated"), 1L,
                  ifelse(low %in% c("0","no","false","c","ctrl","control"), 0L, NA_integer_))
    }
  }
  tibble::tibble(
    dataset   = label,
    n_rows    = nrow(df),
    n_hh      = dplyr::n_distinct(df$hh_id),
    n_ind     = if ("ind_id" %in% names(df) && !all(is.na(df$ind_id)))
      dplyr::n_distinct(df$ind_id) else NA_integer_,
    treat_na  = if (!is.null(t)) sum(is.na(t)) else NA_integer_,
    treat_1   = if (!is.null(t)) sum(t == 1, na.rm = TRUE) else NA_integer_,
    treat_0   = if (!is.null(t)) sum(t == 0, na.rm = TRUE) else NA_integer_
  )
}

summary_tbl <- dplyr::bind_rows(
  summary_wave(bl, "baseline"),
  summary_wave(el, "endline"),
  summary_wave(ml, "midline")
)

created_out <- character(0)
if (!is.null(summary_tbl)) {
  f1 <- write_table(summary_tbl, "data_build_summary.csv",
                    out_dir = PATH_OUT_DP$`01_data_build`, stamp = FALSE)
  created_out <- c(created_out, f1)
  print(summary_tbl)
}

schema_dump <- function(df, label) {
  if (is.null(df)) return(NULL)
  tibble::tibble(
    dataset = label,
    var     = names(df),
    class   = vapply(df, function(x) paste(class(x), collapse = "/"), character(1))
  )
}

schemas <- dplyr::bind_rows(
  schema_dump(bl, "baseline"),
  schema_dump(el, "endline"),
  schema_dump(ml, "midline")
)
if (!is.null(schemas)) {
  f2 <- write_table(schemas, "schema_variables.csv",
                    out_dir = PATH_OUT_DP$`01_data_build`, stamp = FALSE)
  created_out <- c(created_out, f2)
}

# PII and Empty column logs
if (REMOVE_PII) {
  if (nrow(pii_log) == 0) pii_log <- tibble::tibble(wave = character(0), column = character(0))
  f3 <- write_table(pii_log, "pii_columns_dropped.csv",
                    out_dir = PATH_OUT_DP$`01_data_build`, stamp = FALSE)
  created_out <- c(created_out, f3)
}
if (DROP_EMPTY_VARS) {
  if (nrow(empty_log) == 0) empty_log <- tibble::tibble(wave = character(0), column = character(0))
  f4 <- write_table(empty_log, "empty_columns_dropped.csv",
                    out_dir = PATH_OUT_DP$`01_data_build`, stamp = FALSE)
  created_out <- c(created_out, f4)
}

# Label counts (present in the processed RDS, inherited from Stata)
label_counts <- function(df, wave) {
  if (is.null(df)) return(NULL)
  n_vars <- ncol(df)
  has_var_lab  <- vapply(df, function(x) {
    lb <- labelled::var_label(x); !is.null(lb) && nzchar(as.character(lb))
  }, logical(1))
  has_val_labs <- vapply(df, function(x) {
    labs <- labelled::val_labels(x); !is.null(labs) && length(labs) > 0
  }, logical(1))
  tibble::tibble(
    wave                 = wave,
    n_vars               = n_vars,
    n_with_var_label     = sum(has_var_lab,  na.rm = TRUE),
    n_with_value_labels  = sum(has_val_labs, na.rm = TRUE)
  )
}

labels_summary <- dplyr::bind_rows(
  label_counts(bl, "baseline"),
  label_counts(el, "endline"),
  label_counts(ml, "midline")
)

f5 <- write_table(labels_summary, "labels_summary_by_wave.csv",
                  out_dir = PATH_OUT_DP$`01_data_build`, stamp = FALSE)
created_out <- c(created_out, f5)

message("Step 01 complete: per-wave raw files saved; diagnostics written.")

# ---- 13) Log and snapshot -----------------------------------------
append_log(
  step       = "01_data_build.R",
  code_files = "code/01_data_build.R",
  docs_files = NULL,
  out_files  = if (length(created_out)) created_out else NULL,
  data_files = if (length(created_data)) created_data else NULL
)
write_structure_snapshot()
