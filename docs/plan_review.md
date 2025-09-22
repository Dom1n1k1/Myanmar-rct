
# Study plan snapshot

**Partners and roles.** IPA coordinated data; Brown University PIs designed the RCT and analysis; Common Health implemented Bright Start; Terre des hommes recruited and listed eligible households; UNICEF supported funding and M&E.

**Intervention.** Three components offered to pregnant women and children under 7: hospital cash insurance of MMK 55,000 per hospitalization, telemedicine primary care, and an outpatient benefits package at enrollment. Insurance has a 30-day waiting period. Coverage is provisioned at enrollment. (Protocol; Baseline report.)

**Design.** Cluster RCT in two Yangon townships: Hlaing Thar Yar and Shwe Pyi Thar. Clusters grouped by local geography (ward or similar), randomized to treatment or control. Respondent is the main female; modules cover eligible children and pregnancy. (Protocol; Baseline report.)

**Sample.** Listing ~4,399 HHs (6,013 individuals). Target analytic HHs 2,942 split 1:1 T vs C, with 883 in SPT and 2,059 in HTY. Baseline fieldwork Oct 11 to Nov 3, 2021 with 3,210 HHs. (Baseline report.)

**Outcomes originally emphasized.** Access and utilization (telehealth, outpatient, hospital); OOP health spending and financial resilience; information on maternal and child health and COVID-19; insurance use. (Protocol; Endline instrument.)

**Power notes.** Household is the unit of intervention. Illustrative assumptions: outpatient use baseline 0.15 with +0.05 pp effect; hospitalization baseline 5 percent with +1.5 pp effect; OOP reduction about 5 to 6 USD; ICC in the range 0.01 to 0.3 depending on framing; alpha 0.05; attrition 20 to 30 percent. (Baseline report; Protocol.)

---

## Decisions to lock before building

### Identification and inference

- **Randomization unit:** operational cluster. Working choice: `cluster_id = ward (A2)` if stable; fallback `uni_cluster_id`. Keep `strata = township (A1)`.  
- **SEs and fixed effects:** cluster standard errors at `cluster_id`. Include township strata fixed effects in estimation.  
- **Timing:** Main endpoint is endline (~12 months). Midline (~500 HHs) is for short-run process checks.

### Analysis populations

- **Household level (primary ITT):** 1 row per HH x wave, respondent is main female. Outcomes aggregated across eligible members where relevant.
  - **Utilization:** any telemedicine use in last 30 days; any outpatient/primary care in last 30 days; any hospitalization in last 12 months.
  - **Financial protection:** household OOP from inpatient episode items when present; outpatient OOP if collected; catastrophic OOP indicator at chosen threshold; resilience proxies from financial health module.
  - **Knowledge:** maternal and child health knowledge index; COVID-19 knowledge and vaccination for respondent/HH.

- **Individual level (secondary):** two subgroups built from rosters.
  1) **Pregnant women:** ANC initiation and number of ANC visits; services received; pregnancy-related admissions; delivery location and assistance; Bright Start support received.  
  2) **Children under 7:** recent illness care seeking; telemedicine vs in-person care format; immunization status by schedule; any hospitalization and related OOP if available.

### Outcomes to estimate with indicators and survey references

Below, “Ref” points to sections and question blocks in the endline instrument so code book mapping can link raw names.

#### A. Access and utilization

- **Telemedicine use (HH-level any; Individual-level when available).**  
  - Indicators: any telemedicine in past 30 days; format of care (telemedicine only vs mixed vs in-person).  
  - Ref: Section C Part II care format C15 and communication modes C16.  
- **Outpatient/primary care use (HH & Individual).**  
  - Indicators: any outpatient visit in past 30 days; ancillary items like tests C12 and medications C13 and adherence C14 for those with visits.  
  - Ref: Section C Part II items C12–C16 gating on care in the past 30 days.  
- **Hospital use (HH & Individual).**  
  - Indicators: any hospitalization in past 12 months (C28); number of episodes (C30); facility type (C33); days in hospital (C35).  
  - Ref: Section C Part III C28, C30, C33, C35.

#### B. Financial protection

- **Inpatient OOP amounts.**  
  - Indicators: direct costs (C36), indirect costs (C37), and total = C36 + C37 for most recent episode.  
  - Ref: Section C Part III C36, C37.  
- **Outpatient OOP amounts.**  
  - Indicators: direct and indirect OOP per most recent outpatient contact if collected in Part II; construct total outpatient OOT and 30-day total.  
  - Ref: Section C Part II cost items (map during codebook step).  
- **Catastrophic OOP.**  
  - Indicator: 1 if OOP share of household resources exceeds threshold (e.g., 10 percent of monthly consumption or income, depending on available F-section aggregates).  
  - Ref: Section F Household Income, Consumption and Assets for denominators.

#### C. Maternal health and pregnancy

- **Current pregnancy care.**  
  - Indicators: ANC ever (D4), GA at first ANC (D5), ANC visits count (D6), services received list (D8), any pregnancy-related overnight admission (D9).  
  - Ref: Section D Part I D1–D9.  
- **Past pregnancy and delivery.**  
  - Indicators: delivery location (D12), skilled assistance (D12.1), reasons for choosing facility (D13).  
  - Ref: Section D Part II D10–D13.  
- **Pregnancy-specific hospitalizations and insurance.**  
  - Indicators: reasons for pregnancy hospitalization (D15.1) and insurance covering ANC/delivery (D15.2).  
  - Ref: Section D Part II D15–D15.2.  
- **Program take-up among women.**  
  - Indicators: any Bright Start assistance (D16), type of support (D17), cash amount received (D18).  
  - Ref: Section D items D16–D18.

#### D. Child health and immunization (under 7)

- **Immunization.**  
  - Indicators: ever vaccinated (E1); card available (E2); schedule adherence at birth, 2, 4, 6, 9, and 18 months (E3a–E3f).  
  - Ref: Section E E1–E3.  
- **COVID vaccination for child.**  
  - Indicators: any COVID vaccine (E4.1) and doses (E4.2) if applicable.  
  - Ref: Section E E4.1–E4.2.  
- **Utilization format for recent care.**  
  - Indicators: telemedicine vs in-person as in C15 for child encounters.  
  - Ref: Section C Part II C15.

#### E. Knowledge and COVID

- **COVID knowledge and vaccination.**  
  - Indicators: emergency signs knowledge multi-select (H5) to build index; respondent vaccination H6.1 and doses H6.2.  
  - Ref: Section H H5–H6.2.  
- **Program knowledge.**  
  - Indicators: Section I I1–I6 on Bright Start knowledge and claims, for implementation checks rather than causal endpoints.  
  - Ref: Section I I1–I6.

### Estimators

- **Primary specification (household-level ANCOVA):**  
  `Y_endline = α + β * treat + γ * Y_baseline + strata_FE + ε`, with SEs clustered at `cluster_id`.  
  Use for binary and continuous outcomes; report marginal effects for binary where relevant.

- **If baseline Y missing:** difference-in-means with pre-specified covariates (township FE, baseline HH size, head education, wealth proxy).

- **Secondary individual-level analyses:** same ANCOVA logic with clustering at `cluster_id` and township FE; restrict to eligible subgroup.

- **Multiple testing:** bundle outcomes into families (Utilization, Financial protection, Maternal, Child, Knowledge). Control FDR within family or report sharpened q-values.

### Missing data and weights

- **Item nonresponse:** pre-specify handling per outcome (e.g., treat missing OOP as 0 only when explicitly asked and 0 is a valid response; otherwise missing).  
- **Unit attrition:** document rates and balance; report Lee bounds where applicable for key endpoints.  
- **Weights:** no survey weights planned. Unweighted ITT unless a design weight is discovered later.

---

## Next steps that follow from this plan

1) Run schema scan to confirm the presence and names of the referenced items and detect roster structures.  
2) Create `cluster_id` from ward A2 (fallback `uni_cluster_id`) and `strata` from township A1; save a keys file for merges.  
3) Build HH-level and individual-level clean analysis files reflecting the outcome families above.  
4) Draft codebook mapping raw variables to canonical names for table labels.


