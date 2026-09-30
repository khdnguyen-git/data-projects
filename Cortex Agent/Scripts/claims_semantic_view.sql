create or replace semantic view tmp_1q.claims_view
    tables (
        VING_PRD_TREND_DB.TMP_1M.KN_CLAIMS_SEMANTIC_BASE,
        VING_PRD_TREND_DB.TMP_1M.KN_CLAIMS_SEMANTIC_MEMBERSHIP
            unique (POPULATION, SRVC_MONTH, MARKET_FNL, BRAND_FNL, PRODUCT_LEVEL_3_FNL)
    )
    relationships (
        KN_CLAIMS_SEMANTIC_BASE_TO_KN_CLAIMS_SEMANTIC_MEMBERSHIP
            as KN_CLAIMS_SEMANTIC_BASE(POPULATION, SRVC_MONTH, MARKET_FNL, BRAND_FNL, PRODUCT_LEVEL_3_FNL)
            references KN_CLAIMS_SEMANTIC_MEMBERSHIP(POPULATION, SRVC_MONTH, MARKET_FNL, BRAND_FNL, PRODUCT_LEVEL_3_FNL)
    )
    facts (
        KN_CLAIMS_SEMANTIC_BASE.MNR_FFS_PAID labels = (filter)
            as POPULATION = 'M&R FFS' AND DENIAL_FLAG = 'Paid'
            comment='Filters to M&R FFS population with paid claims.',
        KN_CLAIMS_SEMANTIC_BASE.YEAR_2025 labels = (filter)
            as SRVC_YEAR = '2025'
            comment='Filters to service year 2025.',
        KN_CLAIMS_SEMANTIC_BASE.YEAR_2026 labels = (filter)
            as SRVC_YEAR = '2026'
            comment='Filters to service year 2026.',
        KN_CLAIMS_SEMANTIC_MEMBERSHIP.MEMBER_MONTHS
            as MEMBER_MONTHS
            sample_values ('28176', '12067', '41013')
    )
    dimensions (
        -- Claims base: time
        KN_CLAIMS_SEMANTIC_BASE.SRVC_DT as SRVC_DT,
        KN_CLAIMS_SEMANTIC_BASE.SRVC_MONTH as SRVC_MONTH
            sample_values ('202501', '202604', '202410'),
        KN_CLAIMS_SEMANTIC_BASE.SRVC_QTR as SRVC_QTR
            sample_values ('2025Q1', '2026Q2', '2024Q4'),
        KN_CLAIMS_SEMANTIC_BASE.SRVC_YEAR as SRVC_YEAR
            sample_values ('2024', '2025', '2026'),
        -- Claims base: identifiers
        KN_CLAIMS_SEMANTIC_BASE.VISIT_ID as VISIT_ID,
        KN_CLAIMS_SEMANTIC_BASE.PROC_SRVC_ID as PROC_SRVC_ID
            comment='Procedure-service ID: concat(mbi, srvc_prov_id, fst_srvc_dt, proc_cd). Represents a unique procedure instance.',
        KN_CLAIMS_SEMANTIC_BASE.MBI as MBI,
        KN_CLAIMS_SEMANTIC_BASE.SITE_CLM_AUD_NBR as SITE_CLM_AUD_NBR,
        KN_CLAIMS_SEMANTIC_BASE.CLM_PD_DT as CLM_PD_DT
            comment='Date the claim (site_clm_aud_nbr) was paid/processed.',
        KN_CLAIMS_SEMANTIC_BASE.CLM_PD_MONTH as CLM_PD_MONTH
            comment='Month (YYYYMM) the claim was paid.'
            sample_values ('202501', '202604', '202410'),
        KN_CLAIMS_SEMANTIC_BASE.CLM_PD_QTR as CLM_PD_QTR
            comment='Quarter the claim was paid (e.g. 2026Q1).'
            sample_values ('2025Q1', '2026Q2', '2024Q4'),
        KN_CLAIMS_SEMANTIC_BASE.CLM_PD_YEAR as CLM_PD_YEAR
            comment='Year the claim was paid.'
            sample_values ('2024', '2025', '2026'),
        -- Claims base: service details
        KN_CLAIMS_SEMANTIC_BASE.CLAIM_PLATFORM as CLAIM_PLATFORM
            sample_values ('COSMOS', 'CSP', 'NICE'),
        KN_CLAIMS_SEMANTIC_BASE.COMPONENT as COMPONENT
            sample_values ('OP', 'PR'),
        KN_CLAIMS_SEMANTIC_BASE.PROC_CD as PROC_CD
            sample_values ('99213', '99214', '36415'),
        KN_CLAIMS_SEMANTIC_BASE.PROC_MOD1_CD as PROC_MOD1_CD
            comment='Procedure modifier 1.'
            sample_values ('GP', 'GO', 'GN', '25', '59'),
        KN_CLAIMS_SEMANTIC_BASE.PROC_MOD2_CD as PROC_MOD2_CD
            comment='Procedure modifier 2.',
        KN_CLAIMS_SEMANTIC_BASE.PROC_MOD3_CD as PROC_MOD3_CD
            comment='Procedure modifier 3.',
        KN_CLAIMS_SEMANTIC_BASE.PROC_MOD4_CD as PROC_MOD4_CD
            comment='Procedure modifier 4.',
        KN_CLAIMS_SEMANTIC_BASE.SERVICE_CODE as SERVICE_CODE
            comment='Service line category. OP_ prefix for outpatient, PR_ prefix for professional.'
            sample_values ('OP_EMERG', 'OP_SURG', 'PR_OFFVISIT', 'PR_REHABSERV', 'OP_REHAB'),
        KN_CLAIMS_SEMANTIC_BASE.PRIMARY_DIAG_CD as PRIMARY_DIAG_CD
            sample_values ('M79.3', 'Z23', 'R10.9'),
        KN_CLAIMS_SEMANTIC_BASE.RVNU_CD as RVNU_CD
            sample_values ('0301', '0450', '0510'),
        KN_CLAIMS_SEMANTIC_BASE.AMA_PL_OF_SRVC_CD as AMA_PL_OF_SRVC_CD
            sample_values ('11', '22', '23'),
        KN_CLAIMS_SEMANTIC_BASE.BIL_TYP_CD as BIL_TYP_CD
            sample_values ('131', '851', '711'),
        -- Claims base: provider
        KN_CLAIMS_SEMANTIC_BASE.PROV_TIN as PROV_TIN,
        KN_CLAIMS_SEMANTIC_BASE.SRVC_PROV_NPI_NBR as SRVC_PROV_NPI_NBR,
        KN_CLAIMS_SEMANTIC_BASE.FULL_NM as FULL_NM,
        KN_CLAIMS_SEMANTIC_BASE.HOSPITAL_GROUP as HOSPITAL_GROUP
            sample_values ('Cleveland Clinic Health System', 'HCA', 'Sutter Health Physicians'),
        -- Claims base: geography and contract
        KN_CLAIMS_SEMANTIC_BASE.MARKET_FNL as MARKET_FNL
            sample_values ('FL', 'TX', 'CA'),
        KN_CLAIMS_SEMANTIC_BASE.CONTRACT_FNL as CONTRACT_FNL
            sample_values ('H2247', 'H3805', 'H0421'),
        KN_CLAIMS_SEMANTIC_BASE.ST_ABBR_CD as ST_ABBR_CD
            sample_values ('FL', 'TX', 'CA', 'MD'),
        -- Claims base: population and flags
        KN_CLAIMS_SEMANTIC_BASE.POPULATION as POPULATION
            sample_values ('M&R FFS', 'C&S DSNP', 'OAH', 'INSTITUTIONAL', 'Other'),
        KN_CLAIMS_SEMANTIC_BASE.DENIAL_FLAG as DENIAL_FLAG
            sample_values ('Paid', 'Denied'),
        KN_CLAIMS_SEMANTIC_BASE.THERAPIES_FLAG as THERAPIES_FLAG
            comment='Y = therapies claim (excludes DSNP/INSTITUTIONAL members). N = not therapies.'
            sample_values ('Y', 'N') is_enum,
        KN_CLAIMS_SEMANTIC_BASE.SKIN_SUB_FLAG as SKIN_SUB_FLAG
            sample_values ('Y', 'N') is_enum,
        KN_CLAIMS_SEMANTIC_BASE.SUBCATEGORY as SUBCATEGORY
            comment='Subcategory from proc_category lookup. For therapies (therapies_flag=Y): Chiro, PT-OT, ST. For skin subs (skin_sub_flag=Y): Covered, Unproven. NULL for non-flagged claims or revenue-code-only therapies matches.'
            sample_values ('Chiro', 'PT-OT', 'ST', 'Covered', 'Unproven'),
        KN_CLAIMS_SEMANTIC_BASE.BRAND_FNL as BRAND_FNL
            sample_values ('M&R', 'C&S'),
        KN_CLAIMS_SEMANTIC_BASE.PRODUCT_LEVEL_3_FNL as PRODUCT_LEVEL_3_FNL
            sample_values ('DUAL', 'INSTITUTIONAL'),
        KN_CLAIMS_SEMANTIC_BASE.GLOBAL_CAP as GLOBAL_CAP
            sample_values ('NA', 'ENC'),
        KN_CLAIMS_SEMANTIC_BASE.GROUP_IND_FNL as GROUP_IND_FNL,
        KN_CLAIMS_SEMANTIC_BASE.MIGRATION_SOURCE as MIGRATION_SOURCE
            sample_values ('OAH', 'NA'),
        KN_CLAIMS_SEMANTIC_BASE.TFM_INCLUDE_FLAG as TFM_INCLUDE_FLAG
            sample_values ('1', '0'),
        -- Membership dimensions
        KN_CLAIMS_SEMANTIC_MEMBERSHIP.SRVC_MONTH as SRVC_MONTH
            sample_values ('202501', '202604', '202410'),
        KN_CLAIMS_SEMANTIC_MEMBERSHIP.POPULATION as POPULATION
            sample_values ('M&R FFS', 'C&S DSNP', 'OAH', 'INSTITUTIONAL', 'Other'),
        KN_CLAIMS_SEMANTIC_MEMBERSHIP.MARKET_FNL as MARKET_FNL
            sample_values ('FL', 'TX', 'CA'),
        KN_CLAIMS_SEMANTIC_MEMBERSHIP.BRAND_FNL as BRAND_FNL
            sample_values ('M&R', 'C&S'),
        KN_CLAIMS_SEMANTIC_MEMBERSHIP.PRODUCT_LEVEL_3_FNL as PRODUCT_LEVEL_3_FNL
    )
    metrics (
        KN_CLAIMS_SEMANTIC_BASE.TOTAL_ALLOWED
            as SUM(kn_claims_semantic_base.allw_amt_fnl)
            with synonyms=('cost','how much did we spend','spend','spent','total_spend')
            comment='Total allowed amount. When users say spent, spend, or cost, use this metric.',
        KN_CLAIMS_SEMANTIC_BASE.TOTAL_PAID
            as SUM(kn_claims_semantic_base.net_pd_amt_fnl)
            comment='Total net paid amount. Only use when user explicitly asks for net paid.',
        KN_CLAIMS_SEMANTIC_BASE.TOTAL_BILLED
            as SUM(kn_claims_semantic_base.sbmt_chrg_amt)
            with synonyms=('billed','submitted_charges','charges')
            comment='Total submitted/billed charges.',
        KN_CLAIMS_SEMANTIC_BASE.TOTAL_UNITS
            as SUM(kn_claims_semantic_base.tadm_unit_cnt)
            with synonyms=('units','tadm_units')
            comment='Total TADM units.',
        KN_CLAIMS_SEMANTIC_BASE.TOTAL_ADJ_UNITS
            as SUM(kn_claims_semantic_base.adj_srvc_unit_cnt)
            comment='Total adjusted service units.',
        KN_CLAIMS_SEMANTIC_BASE.COST_PER_VISIT
            as SUM(kn_claims_semantic_base.allw_amt_fnl) / NULLIF(COUNT(DISTINCT kn_claims_semantic_base.visit_id), 0)
            comment='Average allowed amount per visit/encounter.',
        KN_CLAIMS_SEMANTIC_BASE.COST_PER_PROC
            as SUM(kn_claims_semantic_base.allw_amt_fnl) / NULLIF(COUNT(DISTINCT kn_claims_semantic_base.proc_srvc_id), 0)
            comment='Average allowed amount per distinct procedure instance.',
        KN_CLAIMS_SEMANTIC_BASE.COST_PER_LINE
            as SUM(kn_claims_semantic_base.allw_amt_fnl) / NULLIF(COUNT(kn_claims_semantic_base.visit_id), 0)
            comment='Average allowed amount per claim line.',
        KN_CLAIMS_SEMANTIC_BASE.COST_PER_UNIT
            as SUM(kn_claims_semantic_base.allw_amt_fnl) / NULLIF(SUM(kn_claims_semantic_base.adj_srvc_unit_cnt), 0)
            comment='Average allowed amount per adjusted service unit.',
        KN_CLAIMS_SEMANTIC_BASE.LINE_COUNT
            as COUNT(kn_claims_semantic_base.visit_id)
            with synonyms=('bill_lines','volume')
            comment='Count of bill lines (rows).',
        KN_CLAIMS_SEMANTIC_BASE.CLAIM_COUNT
            as COUNT(DISTINCT kn_claims_semantic_base.site_clm_aud_nbr)
            with synonyms=('distinct_claims','claims')
            comment='Count of distinct claims.',
        KN_CLAIMS_SEMANTIC_BASE.PROC_COUNT
            as COUNT(DISTINCT kn_claims_semantic_base.proc_srvc_id)
            with synonyms=('procedure_count','procs')
            comment='Count of distinct procedure instances.',
        KN_CLAIMS_SEMANTIC_BASE.VISIT_COUNT
            as COUNT(DISTINCT kn_claims_semantic_base.visit_id)
            with synonyms=('distinct_visits','encounters','visits')
            comment='Count of distinct visits/encounters.',
        KN_CLAIMS_SEMANTIC_BASE.UNIQUE_MEMBERS
            as COUNT(DISTINCT kn_claims_semantic_base.mbi)
            with synonyms=('distinct_members','member_count')
            comment='Count of distinct members with claims.'
    )
    ai_sql_generation 'Default behavior:
- Always include component (OP/PR) as a GROUP BY dimension unless the user explicitly says "combined", "total", or "overall".
- Always default to population = ''M&R FFS'' unless the user specifies a different population.
- When the user says "total" or "combined", aggregate across components without GROUP BY component.
- Always apply denial_flag = ''Paid'' UNLESS user explicitly asks about denied claims or denial rates.
- When the query involves procedure codes (proc_cd in GROUP BY or filter), default to procedure count (COUNT(DISTINCT proc_srvc_id)) rather than visit count as the volume metric.

Metric formulas:
- PMPM (allowed) = SUM(allw_amt_fnl) / SUM(member_months). Round to 2 decimals. Do NOT multiply by 1000.
- Utilization per K = SUM(adj_srvc_unit_cnt) / SUM(member_months) * 12000. Round to 1 decimal.
- Cost per unit = SUM(allw_amt_fnl) / NULLIF(SUM(adj_srvc_unit_cnt), 0). Round to 2 decimals.
- Cost per visit = SUM(allw_amt_fnl) / NULLIF(COUNT(DISTINCT visit_id), 0). Round to 2 decimals.
- Do not use net_pd_amt_fnl or TOTAL_PAID unless the user explicitly asks for "net paid" or "paid amount".

Formatting:
- Use TO_CHAR with ''999,999,999,999'' format for large integers (counts, sums).
- Use TO_CHAR with ''999,999,999,999.00'' for dollar amounts.
- Applies to total_allowed, line_count, unique_members, and any metric that can exceed thousands.

Membership grain:
- Exists at: population + srvc_month + market_fnl + brand_fnl + product_level_3_fnl (or coarser).
- Does NOT exist at: proc_cd, proc_mod1_cd, prov_tin, component, claim_platform, hospital_group, primary_diag_cd, rvnu_cd, ama_pl_of_srvc_cd, bil_typ_cd, denial_flag, srvc_prov_npi_nbr, full_nm, service_code, therapies_flag, skin_sub_flag, proc_srvc_id, visit_id, site_clm_aud_nbr, clm_pd_dt, clm_pd_month.

CTE pattern (mandatory for PMPM and utilization per K):
- ALWAYS aggregate claims and membership in SEPARATE CTEs first, then join the aggregated results.
- Never join kn_claims_semantic_membership to kn_claims_semantic_base at the row level.
- Join on the dimensions common to both CTEs (subset of: population, srvc_month, market_fnl, brand_fnl, product_level_3_fnl).

Population details:
- "M&R FFS" / "Medicare FFS" / "MNR" = Medicare and Retirement Fee-for-Service. Includes M&R DSNP members.
- "C&S DSNP" / "Community DSNP" / "dual" / "DSNP" = Community and State Dual Special Needs Plan.
- "INSTITUTIONAL" / "ISNP" / "institutional" = Institutional Special Needs Plan (nursing facility members).
- "OAH" / "Optum at Home" = Optum at Home population (home-based care).
- "Other" = catch-all for remaining claims not in a named population.

Platform synonyms:
- "SMART" = claim_platform = ''CSP''.

Place of service mappings:
- "office visits" / "in-office" = ama_pl_of_srvc_cd IN (''11'', ''49'').
- "telehealth" / "virtual visits" = ama_pl_of_srvc_cd IN (''02'', ''10'').

Service code synonyms:
- "DME" = service_code IN (''PR_DME'', ''OP_DME'').

Entity synonyms:
- "facility name" / "facility" / "hospital" = hospital_group.
- "bill line" / "claim line" = one row in the table (LINE_COUNT metric for counts, or individual rows for detail pulls).

Diagnosis filtering:
- Use LEFT(primary_diag_cd, 3) for ICD-10 category filtering.
- Example: diabetes = LEFT(primary_diag_cd, 3) IN (''E08'', ''E09'', ''E10'', ''E11'', ''E12'', ''E13'').

Modifier filtering:
- When checking for a modifier (e.g. KF, GP, GO, GN), check ALL four columns: proc_mod1_cd, proc_mod2_cd, proc_mod3_cd, proc_mod4_cd.
- Use a CASE WHEN to create a with/without flag when the user asks for a modifier split.

Detail-level pulls:
- When the user asks for "detailed", "line-level", "most detailed level", or "individual claims", generate a SELECT of the requested dimensions and raw columns (allw_amt_fnl, net_pd_amt_fnl, adj_srvc_unit_cnt, sbmt_chrg_amt) with no aggregation.
- Always add LIMIT 100 unless user specifies a different limit.
- Do NOT wrap amounts in SUM() for detail pulls; select the raw column values directly.

Physical therapy / therapies routing:
- "physical therapy" / "PT" / "PT-OT" / "chiro" / "chiropractic" / "speech therapy" / "ST" / "therapies" = filter therapies_flag = ''Y''.
- When user asks about therapies broadly, GROUP BY subcategory to show the breakdown (Chiro, PT-OT, ST).
- When user specifies a type (PT, chiro, speech), filter subcategory directly (e.g., subcategory = ''PT-OT'' for PT/physical therapy, subcategory = ''Chiro'' for chiropractic, subcategory = ''ST'' for speech therapy).
- "skin substitutes" / "skin subs" = filter skin_sub_flag = ''Y''. Use subcategory to split Covered vs Unproven if user asks about breakdown.

Pay vs allowed clarification:
- "how much did we pay" / "total paid" / "payments" / "spend" = use allw_amt_fnl (allowed amount) unless user explicitly says "net paid" or "net payment".
- In this dataset, "paid" in common usage refers to allowed (the plan''s financial exposure), not net paid (after member cost-sharing).
- Only use net_pd_amt_fnl when user says "net paid", "net payment", or "after cost sharing".

Analytical / trend questions:
- "show trends" / "trending" / "what''s trending" = show PMPM or total_allowed by month with component split for the most recent 12 months.
- "what''s concerning" / "notable trends" / "highlights" / "what stands out" = compare recent months to prior period. Show top movers (largest absolute or % change). Include at least 2 breakouts: component + one other (service_code, market, or hospital_group depending on question context).
- "highest" / "lowest" / "top" / "bottom" = ORDER BY the relevant metric DESC or ASC, LIMIT 10 unless user specifies.
- When showing growth or change, always include both the absolute dollar change AND percent change.'

    ai_question_categorization 'If the user asks about "utilization" without specifying a metric, consider this UNCLEAR and ask: do you mean visits (distinct encounters), adjusted service units, or claim lines?

If the user asks "why did X increase", "what caused Y to go up", "explain the change in Z", "what''s driving the increase":
- Generate a data breakdown showing the components (by month, service_code, market, hospital_group, etc.) that contribute most to the change.
- Add a disclaimer note: "This shows where the change is concentrated. Determining root cause (policy changes, coding shifts, population mix, seasonality) requires clinical or operational context beyond this dataset."

If the user asks "show me trends", "what stands out", "anything concerning", "highlights":
- Treat as an analytical question. Generate SQL that shows the top movers (largest changes month-over-month or year-over-year) by the most relevant breakout dimensions.
- Default to PMPM trend by component + service_code if no other context provided.

If the user asks about billing rules, clinical guidelines, or regulatory requirements (e.g., "when should I use modifier KF", "what qualifies as telehealth", "is this medically necessary"):
- Respond that this tool is designed for data queries and aggregated analytics. For billing rules, clinical guidelines, and regulatory questions, consult your compliance team or CMS documentation.

If the user asks to explain what a field means, describe billing concepts (e.g. bill type + revenue code combos, facility types like CAH), or asks "which field would help me with X", respond that this tool is designed for data queries and aggregated analytics. For field definitions and billing logic explanations, consult your team''s data dictionary or a subject matter expert.'

    ai_verified_queries (
        pmpm_by_month AS (
            QUESTION 'What is the PMPM by month for M&R FFS?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION TRUE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims AS (
    SELECT srvc_month, component, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    GROUP BY srvc_month, component
),
membership AS (
    SELECT srvc_month, SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS''
    GROUP BY srvc_month
)
SELECT claims.srvc_month, claims.component,
    ROUND(claims.total_allowed / membership.member_months, 2) AS pmpm
FROM claims
JOIN membership ON claims.srvc_month = membership.srvc_month
ORDER BY claims.srvc_month, claims.component'
        ),
        pmpm_by_component_2025 AS (
            QUESTION 'Show me PMPM by component for 2025'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims AS (
    SELECT component, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
    GROUP BY component
),
membership AS (
    SELECT SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS'' AND srvc_month BETWEEN ''202501'' AND ''202512''
)
SELECT claims.component,
    ROUND(claims.total_allowed / membership.member_months, 2) AS pmpm
FROM claims
CROSS JOIN membership
ORDER BY claims.component'
        ),
        pmpm_by_market AS (
            QUESTION 'What is the PMPM by market for M&R FFS in 2025?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims AS (
    SELECT market_fnl, component, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
    GROUP BY market_fnl, component
),
membership AS (
    SELECT market_fnl, SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS'' AND srvc_month BETWEEN ''202501'' AND ''202512''
    GROUP BY market_fnl
)
SELECT claims.market_fnl, claims.component,
    ROUND(claims.total_allowed / membership.member_months, 2) AS pmpm
FROM claims
JOIN membership ON claims.market_fnl = membership.market_fnl
ORDER BY claims.market_fnl, claims.component'
        ),
        util_per_k_by_month AS (
            QUESTION 'What is the utilization per K by month?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims AS (
    SELECT srvc_month, component, SUM(adj_srvc_unit_cnt) AS total_units
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    GROUP BY srvc_month, component
),
membership AS (
    SELECT srvc_month, SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS''
    GROUP BY srvc_month
)
SELECT claims.srvc_month, claims.component,
    ROUND(claims.total_units / membership.member_months * 12000, 1) AS util_per_k
FROM claims
JOIN membership ON claims.srvc_month = membership.srvc_month
ORDER BY claims.srvc_month, claims.component'
        ),
        total_spend_by_component AS (
            QUESTION 'What is the total allowed amount by component?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION TRUE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
GROUP BY component
ORDER BY component'
        ),
        cost_per_unit_by_service_code AS (
            QUESTION 'What is the cost per unit by service code for M&R FFS?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT service_code, component,
    ROUND(SUM(allw_amt_fnl) / NULLIF(SUM(adj_srvc_unit_cnt), 0), 2) AS cost_per_unit,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(SUM(adj_srvc_unit_cnt), ''999,999,999,999'') AS total_units
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
GROUP BY service_code, component
ORDER BY SUM(allw_amt_fnl) DESC'
        ),
        top_proc_codes_by_spend AS (
            QUESTION 'What are the top 10 procedure codes by total spend?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION TRUE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT proc_cd, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT proc_srvc_id), ''999,999,999,999'') AS proc_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
GROUP BY proc_cd, component
ORDER BY SUM(allw_amt_fnl) DESC
LIMIT 10'
        ),
        therapies_spend_by_month AS (
            QUESTION 'What is the total spend on therapies claims by month?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_month, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND therapies_flag = ''Y''
GROUP BY srvc_month, component
ORDER BY srvc_month, component'
        ),
        skin_sub_cost_per_visit AS (
            QUESTION 'What is the cost per visit for skin substitutes?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT component,
    ROUND(SUM(allw_amt_fnl) / NULLIF(COUNT(DISTINCT visit_id), 0), 2) AS cost_per_visit,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND skin_sub_flag = ''Y''
GROUP BY component
ORDER BY component'
        ),
        denial_rate_by_component AS (
            QUESTION 'What is the denial rate by component?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT component,
    TO_CHAR(COUNT(*), ''999,999,999,999'') AS total_lines,
    TO_CHAR(SUM(CASE WHEN denial_flag = ''Denied'' THEN 1 ELSE 0 END), ''999,999,999,999'') AS denied_lines,
    ROUND(SUM(CASE WHEN denial_flag = ''Denied'' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS denial_rate_pct
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS''
GROUP BY component
ORDER BY component'
        ),
        top_hospital_groups AS (
            QUESTION 'What are the top 5 hospital groups by total allowed?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION TRUE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT hospital_group, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND hospital_group IS NOT NULL
GROUP BY hospital_group, component
ORDER BY SUM(allw_amt_fnl) DESC
LIMIT 5'
        ),
        spend_by_market_component AS (
            QUESTION 'Show spend by market and component for 2025'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT market_fnl, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
GROUP BY market_fnl, component
ORDER BY SUM(allw_amt_fnl) DESC'
        ),
        spend_for_specific_proc AS (
            QUESTION 'How much did we spend on proc code 97110?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT proc_srvc_id), ''999,999,999,999'') AS proc_count,
    ROUND(SUM(allw_amt_fnl) / NULLIF(SUM(adj_srvc_unit_cnt), 0), 2) AS cost_per_unit
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND proc_cd = ''97110''
GROUP BY component
ORDER BY component'
        ),
        cost_per_unit_for_proc_by_month AS (
            QUESTION 'What is the cost per unit for procedure code 99213 by month?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_month, component,
    ROUND(SUM(allw_amt_fnl) / NULLIF(SUM(adj_srvc_unit_cnt), 0), 2) AS cost_per_unit,
    TO_CHAR(SUM(adj_srvc_unit_cnt), ''999,999,999,999'') AS total_units
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND proc_cd = ''99213''
GROUP BY srvc_month, component
ORDER BY srvc_month, component'
        ),
        spend_by_population AS (
            QUESTION 'Compare total spend across populations for 2025'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT population, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT mbi), ''999,999,999,999'') AS unique_members
FROM kn_claims_semantic_base
WHERE denial_flag = ''Paid'' AND srvc_year = ''2025''
GROUP BY population, component
ORDER BY SUM(allw_amt_fnl) DESC'
        ),
        modifier_split_kf AS (
            QUESTION 'Pull monthly allowed spend for proc codes 97110, 97140, 97530 with and without the KF modifier for 2025'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_month, component, proc_cd,
    CASE
        WHEN proc_mod1_cd = ''KF'' OR proc_mod2_cd = ''KF'' OR proc_mod3_cd = ''KF'' OR proc_mod4_cd = ''KF''
        THEN ''With KF''
        ELSE ''Without KF''
    END AS kf_flag,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND proc_cd IN (''97110'', ''97140'', ''97530'')
GROUP BY srvc_month, component, proc_cd, kf_flag
ORDER BY proc_cd, srvc_month, kf_flag'
        ),
        dme_by_month AS (
            QUESTION 'Pull DME allowed and units for 2025 by month'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_month, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(SUM(adj_srvc_unit_cnt), ''999,999,999,999'') AS total_units
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND service_code IN (''PR_DME'', ''OP_DME'')
GROUP BY srvc_month, component
ORDER BY srvc_month, component'
        ),
        dme_by_product_level AS (
            QUESTION 'Pull DME allowed and units for 2025 by month broken out by product_level_3_fnl'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_month, component, product_level_3_fnl,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(SUM(adj_srvc_unit_cnt), ''999,999,999,999'') AS total_units
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND service_code IN (''PR_DME'', ''OP_DME'')
GROUP BY srvc_month, component, product_level_3_fnl
ORDER BY srvc_month, component, product_level_3_fnl'
        ),
        op_emerg_by_group_ind AS (
            QUESTION 'Pull OP_EMERG visits and allowed by quarter for 2025 broken out by individual and group members'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_qtr, component, group_ind_fnl,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND service_code = ''OP_EMERG''
GROUP BY srvc_qtr, component, group_ind_fnl
ORDER BY srvc_qtr, component, group_ind_fnl'
        ),
        office_vs_telehealth AS (
            QUESTION 'Compare office visit and telehealth visit volume for 2026'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT
    CASE
        WHEN ama_pl_of_srvc_cd IN (''11'', ''49'') THEN ''Office''
        WHEN ama_pl_of_srvc_cd IN (''02'', ''10'') THEN ''Telehealth''
    END AS visit_type,
    component,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2026''
    AND ama_pl_of_srvc_cd IN (''11'', ''49'', ''02'', ''10'')
GROUP BY visit_type, component
ORDER BY visit_type, component'
        ),
        diabetes_dmesup_dsnp AS (
            QUESTION 'Pull diabetes related OP_DMESUP allowed for 2025 for DSNP members'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_month, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count,
    TO_CHAR(COUNT(DISTINCT mbi), ''999,999,999,999'') AS unique_members
FROM kn_claims_semantic_base
WHERE population = ''C&S DSNP'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND service_code = ''OP_DMESUP''
    AND LEFT(primary_diag_cd, 3) IN (''E08'', ''E09'', ''E10'', ''E11'', ''E12'', ''E13'')
GROUP BY srvc_month, component
ORDER BY srvc_month, component'
        ),
        top_procs_urgi_quarter AS (
            QUESTION 'What are the top procedure codes for OP_URGI in 2026Q1?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT proc_cd, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT proc_srvc_id), ''999,999,999,999'') AS proc_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_qtr = ''2026Q1''
    AND service_code = ''OP_URGI''
GROUP BY proc_cd, component
ORDER BY SUM(allw_amt_fnl) DESC
LIMIT 10'
        ),
        detail_pull_j0225 AS (
            QUESTION 'Pull J0225 claims at the line level for 2026 M&R FFS - show member ID, claim number, proc code, all modifiers, component, facility name, market, allowed, units, net paid'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT mbi, site_clm_aud_nbr, proc_cd,
    proc_mod1_cd, proc_mod2_cd, proc_mod3_cd, proc_mod4_cd,
    component, hospital_group, market_fnl,
    allw_amt_fnl, adj_srvc_unit_cnt, net_pd_amt_fnl
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2026''
    AND proc_cd = ''J0225''
LIMIT 100'
        ),
        yoy_pmpm_comparison AS (
            QUESTION 'Show year over year PMPM change from 2024 to 2025'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims_2024 AS (
    SELECT component, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2024''
    GROUP BY component
),
claims_2025 AS (
    SELECT component, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
    GROUP BY component
),
membership_2024 AS (
    SELECT SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS'' AND srvc_month BETWEEN ''202401'' AND ''202412''
),
membership_2025 AS (
    SELECT SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS'' AND srvc_month BETWEEN ''202501'' AND ''202512''
)
SELECT c24.component,
    ROUND(c24.total_allowed / m24.member_months, 2) AS pmpm_2024,
    ROUND(c25.total_allowed / m25.member_months, 2) AS pmpm_2025,
    ROUND((c25.total_allowed / m25.member_months) - (c24.total_allowed / m24.member_months), 2) AS yoy_abs_chg,
    ROUND(((c25.total_allowed / m25.member_months) - (c24.total_allowed / m24.member_months)) / NULLIF(c24.total_allowed / m24.member_months, 0) * 100, 1) AS yoy_pct_chg
FROM claims_2024 c24
JOIN claims_2025 c25 ON c24.component = c25.component
CROSS JOIN membership_2024 m24
CROSS JOIN membership_2025 m25
ORDER BY c24.component'
        ),
        mom_pmpm_change AS (
            QUESTION 'Show the month-over-month PMPM change for 2025'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims AS (
    SELECT srvc_month, component, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
    GROUP BY srvc_month, component
),
membership AS (
    SELECT srvc_month, SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS'' AND srvc_month BETWEEN ''202501'' AND ''202512''
    GROUP BY srvc_month
),
pmpm AS (
    SELECT c.srvc_month, c.component,
        ROUND(c.total_allowed / m.member_months, 2) AS pmpm
    FROM claims c
    JOIN membership m ON c.srvc_month = m.srvc_month
)
SELECT srvc_month, component, pmpm,
    LAG(pmpm) OVER (PARTITION BY component ORDER BY srvc_month) AS prior_month_pmpm,
    ROUND(pmpm - LAG(pmpm) OVER (PARTITION BY component ORDER BY srvc_month), 2) AS mom_abs_chg,
    ROUND((pmpm - LAG(pmpm) OVER (PARTITION BY component ORDER BY srvc_month)) / NULLIF(LAG(pmpm) OVER (PARTITION BY component ORDER BY srvc_month), 0) * 100, 1) AS mom_pct_chg
FROM pmpm
ORDER BY component, srvc_month'
        ),
        notable_trends_2025 AS (
            QUESTION 'What are the most notable PMPM trends in 2025?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims AS (
    SELECT srvc_month, component, service_code, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
    GROUP BY srvc_month, component, service_code
),
membership AS (
    SELECT srvc_month, SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS'' AND srvc_month BETWEEN ''202501'' AND ''202512''
    GROUP BY srvc_month
),
pmpm AS (
    SELECT c.srvc_month, c.component, c.service_code,
        ROUND(c.total_allowed / m.member_months, 2) AS pmpm
    FROM claims c
    JOIN membership m ON c.srvc_month = m.srvc_month
),
ranked AS (
    SELECT component, service_code,
        MAX(CASE WHEN srvc_month = (SELECT MAX(srvc_month) FROM pmpm) THEN pmpm END) AS latest_pmpm,
        MAX(CASE WHEN srvc_month = (SELECT MIN(srvc_month) FROM pmpm) THEN pmpm END) AS earliest_pmpm,
        MAX(CASE WHEN srvc_month = (SELECT MAX(srvc_month) FROM pmpm) THEN pmpm END) -
            MAX(CASE WHEN srvc_month = (SELECT MIN(srvc_month) FROM pmpm) THEN pmpm END) AS abs_chg
    FROM pmpm
    GROUP BY component, service_code
    HAVING earliest_pmpm > 0 AND latest_pmpm IS NOT NULL AND earliest_pmpm IS NOT NULL
)
SELECT component, service_code, earliest_pmpm, latest_pmpm, abs_chg,
    ROUND(abs_chg / NULLIF(earliest_pmpm, 0) * 100, 1) AS pct_chg
FROM ranked
ORDER BY ABS(abs_chg) DESC
LIMIT 15'
        ),
        fastest_growing_procs AS (
            QUESTION 'What are the fastest growing procedure codes by spend from 2024 to 2025?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH spend_2024 AS (
    SELECT proc_cd, component, SUM(allw_amt_fnl) AS total_allowed_2024
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2024''
    GROUP BY proc_cd, component
),
spend_2025 AS (
    SELECT proc_cd, component, SUM(allw_amt_fnl) AS total_allowed_2025
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
    GROUP BY proc_cd, component
)
SELECT s25.proc_cd, s25.component,
    TO_CHAR(COALESCE(s24.total_allowed_2024, 0), ''999,999,999,999.00'') AS total_allowed_2024,
    TO_CHAR(s25.total_allowed_2025, ''999,999,999,999.00'') AS total_allowed_2025,
    TO_CHAR(s25.total_allowed_2025 - COALESCE(s24.total_allowed_2024, 0), ''999,999,999,999.00'') AS yoy_abs_chg,
    ROUND((s25.total_allowed_2025 - COALESCE(s24.total_allowed_2024, 0)) / NULLIF(s24.total_allowed_2024, 0) * 100, 1) AS yoy_pct_chg
FROM spend_2025 s25
LEFT JOIN spend_2024 s24 ON s25.proc_cd = s24.proc_cd AND s25.component = s24.component
WHERE s25.total_allowed_2025 >= 100000
ORDER BY (s25.total_allowed_2025 - COALESCE(s24.total_allowed_2024, 0)) DESC
LIMIT 25'
        ),
        highest_pmpm_markets AS (
            QUESTION 'Which markets have the highest PMPM in 2025?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH claims AS (
    SELECT market_fnl, component, SUM(allw_amt_fnl) AS total_allowed
    FROM kn_claims_semantic_base
    WHERE population = ''M&R FFS'' AND denial_flag = ''Paid'' AND srvc_year = ''2025''
    GROUP BY market_fnl, component
),
membership AS (
    SELECT market_fnl, SUM(member_months) AS member_months
    FROM kn_claims_semantic_membership
    WHERE population = ''M&R FFS'' AND srvc_month BETWEEN ''202501'' AND ''202512''
    GROUP BY market_fnl
)
SELECT c.market_fnl, c.component,
    ROUND(c.total_allowed / m.member_months, 2) AS pmpm,
    TO_CHAR(c.total_allowed, ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(m.member_months, ''999,999,999,999'') AS member_months
FROM claims c
JOIN membership m ON c.market_fnl = m.market_fnl
ORDER BY c.total_allowed / m.member_months DESC'
        ),
        exclude_emerg_spend AS (
            QUESTION 'What is the total spend by service code excluding OP_EMERG and OP_URGI for 2025?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT service_code, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND service_code NOT IN (''OP_EMERG'', ''OP_URGI'')
GROUP BY service_code, component
ORDER BY SUM(allw_amt_fnl) DESC'
        ),
        therapies_by_subcategory AS (
            QUESTION 'What is the total spend on therapies by subcategory and month for 2025?'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT srvc_month, component, subcategory,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND therapies_flag = ''Y''
GROUP BY srvc_month, component, subcategory
ORDER BY srvc_month, component, subcategory'
        ),
        skin_subs_covered_vs_unproven AS (
            QUESTION 'Compare covered vs unproven skin substitute spend for 2025'
            VERIFIED_AT 1752796800
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT subcategory, component,
    TO_CHAR(SUM(allw_amt_fnl), ''999,999,999,999.00'') AS total_allowed,
    TO_CHAR(COUNT(DISTINCT visit_id), ''999,999,999,999'') AS visit_count,
    TO_CHAR(COUNT(DISTINCT mbi), ''999,999,999,999'') AS unique_members,
    ROUND(SUM(allw_amt_fnl) / NULLIF(COUNT(DISTINCT visit_id), 0), 2) AS cost_per_visit
FROM kn_claims_semantic_base
WHERE population = ''M&R FFS'' AND denial_flag = ''Paid''
    AND srvc_year = ''2025''
    AND skin_sub_flag = ''Y''
GROUP BY subcategory, component
ORDER BY subcategory, component'
        )
    )
;
