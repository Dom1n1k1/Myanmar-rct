# How to use the schema scan files

## What each file is for

- `out/tables/schema_scan/schema_variables.csv`  
  One row per variable per wave: name, label, type, missingness, unique count, examples. Use it to see what exists and how complete it is.

- `out/tables/schema_scan/labels_{wave}.csv`  
  Clean dump of question or variable labels. Use it to confirm meaning of each field.

- `out/tables/schema_scan/value_labels_{wave}.csv`  
  Code -> label maps for categorical variables (for example 1 = Yes, 2 = No). Use it to define recodes.

- `out/tables/schema_scan/schema_repeated_blocks.csv`  
  Shows roster stems like `a14_*_1`, `a14_*_2`, ... and how many repeats exist. Use it to identify the pregnancy and under-7 child blocks that we will pivot to long.

- `out/tables/schema_scan/schema_candidates_{family}.csv`  
  Shortlists likely variables by family: `utilization`, `finance`, `maternal`, `child`, `covid`. These are your starting points to pick final raw variables.

## What we are trying to decide - the data model

For each outcome you plan to estimate, choose:
1. exact raw variable(s) per wave
2. time window (30 days, 12 months, most recent)
3. coding rule (binary any or none, sum, max, amount in MMK, etc.)
4. level (household or individual or pregnancy or child)
5. denominator for derived rates or catastrophic OOP
6. baseline control(s) for ANCOVA
7. cluster and strata fields to use for SEs and FE

Once this is filled, we will generate `04_build_household.R` and `05_build_individual.R` to produce clean analysis-ready datasets with canonical names.

---

## Before you start

1) Load the helpers once per R session:

```r
source("code/02a_schema_scan_help.R")
```

2) Generate easy-to-read inputs (only once unless you re-scan):

```r
export_pretty_candidates()          # writes docs/candidates_pretty_*.csv
build_candidates_template()         # writes docs/analysis_selections_template.csv
```

Open `docs/analysis_selections_template.csv` for editing.

---

## Step by step workflow

### Step 1. Shortlist candidates per family

- Open the family CSVs in `docs/` created by `export_pretty_candidates()`.
- Or view them in R:

```r
pretty_candidates("utilization") %>% arrange(pct_missing, wave) %>% head(25)
pretty_candidates("finance") %>% arrange(pct_missing, wave) %>% head(25)
```

Pick one or two promising variables for each intended indicator and each wave.

### Step 2. Verify meaning, time window, and quality

Use `preview_var()` to confirm wording, type, and missingness for the exact variable names you picked.

```r
preview_var("tele_any_30d")
preview_var("clinic_visit_30d")
preview_var("hospital_admit_12m")
```

Check that the label says the expected window (for example last 30 days). Check that `pct_missing` is not excessive.

### Step 3. Lock recodes with value labels

For categorical variables, use `valmap()` to see numeric codes and decide your recode rule.

```r
valmap("tele_any_30d")
valmap("clinic_visit_30d")
```

Examples of recode rules to write later into the template:
- `1->1, 2->0, 8/9->NA`
- `1->1, 0->0, else NA`

### Step 4. Identify rosters to pivot

Use roster stems to plan pregnancy and child long files.

```r
roster_quicklook(40)  # shows stems with many _1, _2, ...
```

Record the stems you will pivot in `docs/data_model.md` under two lists:
- Pregnancy stems (for example `a12_preg`, `a14_*`)
- Under-7 child stems (for example `a14_u7*`, `u7_*`)

### Step 5. Fill the selections template (key deliverable)

Open and edit:

``
docs/analysis_selections_template.csv
``

For each row you want to use, fill the blank columns:
- `level`: `hh`, `pregnancy`, or `child`.
- `indicator`: canonical name, for example `hh_tele_any_30d`, `hh_oop_total_30d`, `child_any_outpatient_30d`.
- `window`: `30d`, `12m`, or `most_recent` as per the label.
- `coding`: for example `binary any`, `sum amount`, `mean index`, `count`.
- `recode`: exact rule from Step 3, for example `1->1, 2->0, 8/9->NA`. Leave blank for continuous variables.
- `notes`: any details like combining fields or thresholds for catastrophic OOP.

Tips:
- Keep only rows you intend to analyze. Delete the rest from the template.
- Prefer consistent variables across waves and lower `pct_missing`.

### Step 6. Sanity-check treatment and IDs

Until we finalize `keys.rds`, you can run a light check on baseline:

```r
source("code/00_setup.R")
bl <- readRDS(fs::path(PATH_CLEAN, "baseline_clean.rds"))
bl %>% count(strata, treat) %>% arrange(strata, treat)
if ("a2_ward" %in% names(bl)) bl %>% count(strata, a2_ward) %>% arrange(desc(n)) %>% head(20)
```

If anything looks off, note it in `docs/deviations_log.md`.

### Step 7. Validate your template is complete

Run this small check. It will list any rows with missing required fields.

```r
library(readr); library(dplyr); library(stringr)
sel <- readr::read_csv("docs/analysis_selections_template.csv", show_col_types = FALSE)
problems <- sel %>%
  filter(
    str_trim(indicator) == "" |
    str_trim(level) == "" |
    str_trim(window) == "" |
    str_trim(coding) == ""
  )
if (nrow(problems)) {
  print(problems %>% select(family, wave, var, indicator, level, window, coding))
  stop("Fill the missing fields above in docs/analysis_selections_template.csv.")
} else {
  message("Selections look complete. Good to go.")
}
```

### Optional. Auto-write a human-readable data model

This turns your completed selections into a short Markdown note for methods.

```r
library(readr); library(dplyr); library(glue)
sel <- readr::read_csv("docs/analysis_selections_template.csv", show_col_types = FALSE) %>%
  filter(indicator != "")
mk_line <- function(row) {
  glue("- **{row$indicator}** [{row$level}, {row$wave}]: `{row$var}` | window: {row$window} | coding: {row$coding}{ifelse(row$recode!='', glue(' | recode: {row$recode}'), '')}{ifelse(row$notes!='', glue(' | notes: {row$notes}'), '')}")
}
txt <- c(
  "# Data model selections",
  "",
  "Below is the machine-readable summary of outcomes we selected from the schema scans.",
  ""
)
for (fam in unique(sel$family)) {
  txt <- c(txt, glue("## {tools::toTitleCase(fam)}"), "")
  fam_rows <- sel %>% filter(family == fam)
  for (ind in unique(fam_rows$indicator)) {
    block <- fam_rows %>% filter(indicator == ind) %>% arrange(wave)
    txt <- c(txt, glue("### {ind}"), vapply(split(block, seq_len(nrow(block))), mk_line, character(1)), "")
  }
}
writeLines(txt, "docs/data_model.md")
message("Wrote docs/data_model.md")
```

---

## Family-specific pointers

**Utilization**
- Search helpers: `search_vars("tele")`, `search_vars("clinic")`, `search_vars("outpatient")`, `search_vars("hospital")`.
- Typical hh indicators: `hh_tele_any_30d`, `hh_outpatient_any_30d`, `hh_hospital_any_12m`.
- Confirm time windows with `preview_var()` and lock Yes or No recodes with `valmap()`.

**Finance**
- Search helpers: `search_vars("out of pocket")`, `search_vars("expend")`, `search_vars("spent")`, `search_vars("fee")`.
- Typical hh indicators: `hh_oop_total_30d` (numeric), `hh_cat_oop_30d` (needs a denominator variable; note threshold in `notes`).

**Maternal**
- Search helpers: `search_vars("preg")`, `search_vars("anc")`, `search_vars("delivery")`, `search_vars("postnatal")`.
- Level can be `pregnancy` if in a roster.
- Typical indicators: `preg_anc4plus`, `preg_facility_delivery`, `preg_knowledge_index` (list the component items in `notes`).

**Child**
- Search helpers: `search_vars("immuni")`, `search_vars("vaccine")`, `search_vars("child")`, `search_vars("under7")`.
- Level is `child` when using roster data.
- Typical indicators: `child_any_outpatient_30d`, `child_full_immunization` (list doses in `notes`).

**Covid**
- Search helpers: `search_vars("covid")`, `search_vars("vaccine")`.
- Decide if questions are hh level or roster level, then proceed as above.

---

## What to send next

- The completed `docs/analysis_selections_template.csv` with only the rows you intend to analyze and their blanks filled.
- The roster stems listed in `docs/data_model.md`.

With that, we will generate the builder scripts that compute your indicators with canonical names.
