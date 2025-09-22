# -------------------------------------------------------------------
# 02b_schema_scan_auto_suggest_selections.R
# Propose raw vars per outcome family from schema_scan outputs (level + window aware)
# -------------------------------------------------------------------

source("code/00_setup.R")

# --- paths -----------------------------------------------------------
SCHEMA_DIR <- fs::path(PATH_OUT_TBL, "schema_scan")
read_schema <- function(fname) readr::read_csv(fs::path(SCHEMA_DIR, fname), show_col_types = FALSE)

vars_all   <- read_schema("schema_variables.csv")
blocks_all <- read_schema("schema_repeated_blocks.csv")

cand_util  <- read_schema("schema_candidates_utilization.csv")
cand_fin   <- read_schema("schema_candidates_finance.csv")
cand_mat   <- read_schema("schema_candidates_maternal.csv")
cand_child <- read_schema("schema_candidates_child.csv")
cand_cov   <- read_schema("schema_candidates_covid.csv")

# --- helpers ---------------------------------------------------------

rank_candidates <- function(df) {
  wave_rank <- c("endline" = 1L, "midline" = 2L, "baseline" = 3L)
  df |>
    dplyr::mutate(
      wave_priority = dplyr::coalesce(wave_rank[.data$wave], 99L),
      miss          = dplyr::coalesce(.data$pct_missing, 100)
    ) |>
    dplyr::arrange(.data$wave_priority, .data$miss, dplyr::desc(.data$n_unique))
}

ensure_metrics <- function(df) {
  if (!nrow(df)) return(df)
  df |>
    dplyr::left_join(
      vars_all |>
        dplyr::select(wave, name, type, pct_missing, n_unique, examples),
      by = c("wave" = "wave", "var" = "name"),
      suffix = c("", "_vars")
    ) |>
    dplyr::mutate(
      pct_missing = dplyr::coalesce(.data$pct_missing, .data$pct_missing_vars),
      n_unique    = dplyr::coalesce(.data$n_unique, .data$n_unique_vars),
      type        = dplyr::coalesce(.data$type, .data$type_vars),
      examples    = dplyr::coalesce(.data$examples, .data$examples_vars)
    ) |>
    dplyr::select(-dplyr::any_of(c("pct_missing_vars","n_unique_vars","type_vars","examples_vars")))
}

# AND-include, NOT-exclude on a given text field (label or var)
apply_text_filters <- function(pool, include = character(), exclude = character(), where = "label") {
  if (!nrow(pool)) return(pool)
  tgt <- if (!is.null(pool[[where]])) pool[[where]] else rep("", nrow(pool))
  ok  <- rep(TRUE, nrow(pool))
  for (p in include) ok <- ok & stringr::str_detect(tgt, stringr::regex(p, ignore_case = TRUE))
  for (p in exclude) ok <- ok & !stringr::str_detect(tgt, stringr::regex(p, ignore_case = TRUE))
  pool[ok, , drop = FALSE]
}

# pick top K after filters
pick_k <- function(pool, include = character(), exclude = character(), k = 3L) {
  # try labels first, else names
  cand <- apply_text_filters(pool, include, exclude, "label")
  if (!nrow(cand)) cand <- apply_text_filters(pool, include, exclude, "var")
  if (!nrow(cand)) return(cand)
  cand |> ensure_metrics() |> rank_candidates() |> dplyr::slice_head(n = k)
}

# --- target library ---------------------------------------------------
# NOTE: include/exclude is now lighter; strong level/time gating is applied below.
TARGETS <- tibble::tribble(
  ~family,        ~indicator,                    ~level,        ~include,                                       ~exclude,
  # UTILIZATION (HH)
  "utilization",  "any_telemed_30d",             "household",   c("tele|virtual|hotline|phone|call|video"),     c("ever|lifetime"),
  "utilization",  "any_outpatient_30d",          "household",   c("outpatient|clinic|primary|gp|doctor|opd"),   c("inpatient|admit"),
  "utilization",  "any_inpatient_12m",           "household",   c("inpatient|admit|hospitali?z"),               character(0),
  
  # FINANCIAL PROTECTION (HH)
  "finance",      "oop_total_30d",               "household",   c("amount|spent|expense|expenditure|paid|cost"), character(0),
  "finance",      "any_catastrophic_10pct_12m",  "household",   c("catastrophic|10"),                            character(0),
  "finance",      "any_hosp_cash_claim_12m",     "household",   c("hospital\\s*cash|insurance|claim|benefit"),   character(0),
  
  # MATERNAL (INDIVIDUAL: pregnancy / recent birth)
  "maternal",     "anc_visits_current",          "individual",  c("antenatal|\\bANC\\b","how many|times|number"), c("claim|insurance"),
  "maternal",     "delivery_facility_type",      "individual",  c("deliver|birth","hospital|clinic|home"),        c("hospitali?zation episode"),
  
  # CHILD (INDIVIDUAL: under 7)
  "child",        "imm_card_seen",               "individual",  c("immuni|vaccin","card|record"),                character(0),
  "child",        "imm_full_schedule",           "individual",  c("immuni|vaccin","all|complete|full"),          character(0),
  
  # COVID
  "covid",        "covid_knowledge_index",       "household",   c("covid","knowledge|symptom|prevent"),          character(0)
)

# Bind candidates with family tag
bind_cand <- function(df, fam) df |> dplyr::mutate(family = fam, .before = 1)
CANDS <- dplyr::bind_rows(
  bind_cand(cand_util,  "utilization"),
  bind_cand(cand_fin,   "finance"),
  bind_cand(cand_mat,   "maternal"),
  bind_cand(cand_child, "child"),
  bind_cand(cand_cov,   "covid")
)

# --- level & window gating -------------------------------------------
gate_pool <- function(pool, fam, lvl, indicator) {
  # Level gating by variable NAMES first (safe, deterministic)
  if (lvl == "household") {
    pool <- pool |> dplyr::filter(
      !stringr::str_detect(.data$var, stringr::regex("u7|child|preg|_[0-9]+$", ignore_case = TRUE))
    )
  }
  if (fam == "maternal") {
    pool <- pool |> dplyr::filter(
      stringr::str_detect(.data$var, stringr::regex("preg|anc", ignore_case = TRUE)) |
        stringr::str_detect(.data$label %||% "", stringr::regex("pregnan|deliver|antenatal|ANC", ignore_case = TRUE))
    )
  }
  if (fam == "child") {
    pool <- pool |> dplyr::filter(
      stringr::str_detect(.data$var, stringr::regex("u7|child", ignore_case = TRUE)) |
        stringr::str_detect(.data$label %||% "", stringr::regex("child|under\\s*7|u7", ignore_case = TRUE))
    )
  }
  
  # Time window nudges from indicator suffix
  inc_extra <- character(0)
  exc_extra <- character(0)
  if (grepl("30d$", indicator)) {
    inc_extra <- c(inc_extra, "past\\s*30|last\\s*30|30\\s*day|month")
    exc_extra <- c(exc_extra, "12\\s*month|last\\s*12|past\\s*12|year|annual|ever|lifetime")
  }
  if (grepl("12m$", indicator)) {
    inc_extra <- c(inc_extra, "12\\s*month|last\\s*12|past\\s*12|year|annual")
  }
  list(pool = pool, inc_extra = inc_extra, exc_extra = exc_extra)
}

`%||%` <- function(x, y) if (is.null(x)) y else x

# --- propose picks ----------------------------------------------------
rows_to_list <- split(TARGETS, seq_len(nrow(TARGETS)))

pick_for_row <- function(row_df, top_k = 3L) {
  fam      <- row_df$family[[1]]
  ind      <- row_df$indicator[[1]]
  lvl      <- row_df$level[[1]]
  include  <- row_df$include[[1]]
  exclude  <- row_df$exclude[[1]]
  
  pool <- CANDS |> dplyr::filter(.data$family == fam)
  g    <- gate_pool(pool, fam, lvl, ind)
  
  # First pass (label/name filters + extra window filters)
  out <- pick_k(
    pool = g$pool,
    include = c(include, g$inc_extra),
    exclude = c(exclude, g$exc_extra),
    k = top_k
  )
  if (!nrow(out)) return(NULL)
  
  out |>
    dplyr::mutate(
      family    = fam,
      indicator = ind,
      level     = lvl,
      .before = 1
    ) |>
    dplyr::select(family, indicator, level, wave, var, label, type, pct_missing, n_unique, examples)
}

opts <- purrr::map_dfr(rows_to_list, pick_for_row, top_k = 3L)

# take the top 1 for the draft
proposals <- opts |>
  dplyr::group_by(family, indicator, level, wave) |>
  dplyr::slice_head(n = 1) |>
  dplyr::ungroup()

# --- write outputs ---------------------------------------------------
out1 <- fs::path(SCHEMA_DIR, "analysis_selections_draft.csv")
out2 <- fs::path(SCHEMA_DIR, "analysis_selections_options_top3.csv")
out3 <- fs::path(SCHEMA_DIR, "roster_stems_proposal.csv")

readr::write_csv(proposals, out1, na = "")
readr::write_csv(opts,      out2, na = "")

roster_stems <- blocks_all |>
  dplyr::filter(stringr::str_detect(.data$stem, stringr::regex("preg|anc|u7|under.?7|child", ignore_case = TRUE))) |>
  dplyr::arrange(dplyr::desc(.data$n_indices))
readr::write_csv(roster_stems, out3, na = "")

message("Wrote:\n - ", out1, "\n - ", out2, "\n - ", out3)
