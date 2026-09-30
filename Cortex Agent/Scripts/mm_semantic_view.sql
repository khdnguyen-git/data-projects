create or replace semantic view tmp_1q.mm_view
    tables (
        VING_PRD_TREND_DB.TMP_1M.KN_MEMBERSHIP_SEMANTIC
    )
    facts (
        KN_MEMBERSHIP_SEMANTIC.MEMBER_MONTHS
            as MEMBER_MONTHS
            comment = 'Count of member-months at this grain. SUM for total enrollment volume.'
            sample_values ('28176', '12067', '41013'),
        KN_MEMBERSHIP_SEMANTIC.MNR_FFS labels = (filter)
            as POPULATION = 'M&R FFS'
            comment = 'Filters to M&R FFS population.',
        KN_MEMBERSHIP_SEMANTIC.ALL_NAMED_POPULATIONS labels = (filter)
            as POPULATION IN ('M&R FFS', 'OAH', 'C&S DSNP', 'INSTITUTIONAL', 'CNI', 'CIP', 'SFL', 'GROUP')
            comment = 'Filters to all named populations, excluding Other.'
    )
    dimensions (
        -- Time
        KN_MEMBERSHIP_SEMANTIC.FIN_INC_MONTH as FIN_INC_MONTH
            comment = 'Enrollment month in YYYYMM format. Range: 202401-202608.'
            sample_values ('202501', '202604', '202410'),
        -- Population
        KN_MEMBERSHIP_SEMANTIC.POPULATION as POPULATION
            comment = 'Derived population segment (9 buckets). M&R FFS is the default.'
            sample_values ('M&R FFS', 'OAH', 'C&S DSNP', 'INSTITUTIONAL', 'CNI', 'CIP', 'SFL', 'GROUP', 'Other') is_enum,
        -- Geography
        KN_MEMBERSHIP_SEMANTIC.FIN_MARKET as FIN_MARKET
            comment = 'Geographic market (state abbreviation).'
            sample_values ('FL', 'TX', 'CA', 'NY', 'PA'),
        KN_MEMBERSHIP_SEMANTIC.FIN_REGION as FIN_REGION
            comment = 'Region: EAST or WEST.'
            sample_values ('EAST', 'WEST') is_enum,
        -- Product
        KN_MEMBERSHIP_SEMANTIC.FIN_BRAND as FIN_BRAND
            comment = 'Product brand: M&R (Medicare & Retirement) or C&S (Community & State).'
            sample_values ('M&R', 'C&S') is_enum,
        KN_MEMBERSHIP_SEMANTIC.FIN_PRODUCT_LEVEL_2 as FIN_PRODUCT_LEVEL_2
            comment = 'Product level 2: NETWORK, NATIONAL, or SNP.'
            sample_values ('NETWORK', 'NATIONAL', 'SNP') is_enum,
        KN_MEMBERSHIP_SEMANTIC.FIN_PRODUCT_LEVEL_3 as FIN_PRODUCT_LEVEL_3
            comment = 'Product level 3 sub-type.'
            sample_values ('NETWORK', 'DUAL', 'NATIONAL', 'INSTITUTIONAL', 'CHRONIC') is_enum,
        KN_MEMBERSHIP_SEMANTIC.FIN_CONTRACTPBP as FIN_CONTRACTPBP
            comment = 'Contract + PBP combined (e.g. H2247-001).'
            sample_values ('H2247001', 'H3805001', 'H0421001'),
        KN_MEMBERSHIP_SEMANTIC.FIN_G_I as FIN_G_I
            comment = 'Group (G) or Individual (I) membership.'
            sample_values ('G', 'I') is_enum,
        -- Demographics
        KN_MEMBERSHIP_SEMANTIC.FIN_GENDER as FIN_GENDER
            comment = 'Member gender: M, F, or U (unknown).'
            sample_values ('M', 'F', 'U') is_enum,
        KN_MEMBERSHIP_SEMANTIC.AGE as AGE
            comment = 'Member age (numeric).',
        KN_MEMBERSHIP_SEMANTIC.FIN_RACE_CD as FIN_RACE_CD
            comment = 'Race code: 0=Unknown, 1=White, 2=Black, 3=Other, 4=Asian, 5=Hispanic, 6=Native American.'
            sample_values ('0', '1', '2', '3', '4', '5', '6') is_enum,
        -- Operational
        KN_MEMBERSHIP_SEMANTIC.SGR_SOURCE_NAME as SGR_SOURCE_NAME
            comment = 'Source system: COSMOS, CSP, NICE, or UNK.'
            sample_values ('COSMOS', 'CSP', 'NICE', 'UNK') is_enum,
        KN_MEMBERSHIP_SEMANTIC.GLOBAL_CAP as GLOBAL_CAP
            comment = 'Global cap indicator.'
            sample_values ('NA', 'CA', 'WM', 'KS', 'MO', 'TX'),
        KN_MEMBERSHIP_SEMANTIC.TFM_INCLUDE_FLAG as TFM_INCLUDE_FLAG
            comment = 'TFM include flag (1=included, 0=excluded).'
            sample_values ('1', '0') is_enum,
        KN_MEMBERSHIP_SEMANTIC.MIGRATION_SOURCE as MIGRATION_SOURCE
            comment = 'Migration source indicator.'
            sample_values ('NA', 'OAH', 'CIP', 'CSP', 'PC', 'MEDICA')
    )
    metrics (
        KN_MEMBERSHIP_SEMANTIC.TOTAL_MEMBER_MONTHS
            as SUM(kn_membership_semantic.member_months)
            with synonyms=('enrollment','headcount','members','member count')
            comment = 'Total member-months. Primary measure of enrollment volume.',
        KN_MEMBERSHIP_SEMANTIC.UNIQUE_MARKETS
            as COUNT(DISTINCT kn_membership_semantic.fin_market)
            comment = 'Count of distinct markets with enrollment.'
    )
    ai_sql_generation 'Default behavior:
- Always default to population = ''M&R FFS'' unless the user specifies a different population.
- When user says "all populations" or "by population", include all values and GROUP BY POPULATION.
- When user says "total enrollment" or "total members", use SUM(member_months).

Metric formulas:
- Total enrollment = SUM(member_months).
- Do NOT use COUNT(*) for member counts. Always use SUM(member_months).
- There is no member-level identifier in this table. For unique member counts, redirect to claims_view or inpatient_view.

Patterns:
- "How many members" = SUM(member_months) with appropriate filters.
- "Enrollment trend" = SUM(member_months) GROUP BY fin_inc_month ORDER BY fin_inc_month.
- "Membership by market" = SUM(member_months) GROUP BY fin_market ORDER BY 2 DESC.
- "Population mix" or "population breakdown" = SUM(member_months) GROUP BY population ORDER BY 2 DESC.
- "Market share" or "% of enrollment" = SUM(member_months) per dimension / total SUM(member_months) * 100.
- "Demographics" or "age distribution" = GROUP BY age or fin_gender or fin_race_cd.

Formatting:
- Use TO_CHAR with ''999,999,999,999'' format for large integers.

Important:
- This table is pre-aggregated. Each row represents member-months at a dimensional grain.
- Do NOT attempt to count individual members. This table has no MBI or member identifier.
- AGE is numeric (can use ranges: age BETWEEN 65 AND 74).

Population details (9 buckets):
- "M&R FFS" / "Medicare FFS" / "MNR" = Medicare and Retirement Fee-for-Service. Includes M&R DSNP members. Brand=M&R, global_cap=NA, tfm_include_flag=1, excludes DUAL/INSTITUTIONAL product.
- "C&S DSNP" / "Community DSNP" / "dual" / "DSNP" = Community and State Dual Special Needs Plan.
- "INSTITUTIONAL" / "ISNP" = Institutional Special Needs Plan (nursing facility members).
- "OAH" / "Optum at Home" = Optum at Home population (migration_source=OAH).
- "CNI" = Community Network Individual (COSMOS, COMMUNITY/NETWORK, Individual).
- "CIP" = migration_source=CIP.
- "SFL" = South Florida legacy (COSMOS, migration_source in PC/MEDICA).
- "GROUP" = NPPO Group members.
- "Other" = catch-all for remaining members.

Race code mapping:
- 0=Unknown, 1=White, 2=Black, 3=Other, 4=Asian, 5=Hispanic, 6=Native American.

Region:
- EAST or WEST.'

    ai_question_categorization 'REDIRECT TO OTHER TOOLS:
- Questions about cost, PMPM, allowed amounts, paid amounts, claims -> redirect to claims_view.
- Questions about authorizations, admits, IP days -> redirect to inpatient_view.
- Questions about specific members (MBI lookup, individual member activity) -> redirect to claims_view.

THIS VIEW HANDLES:
- Enrollment volume and trends.
- Population mix and distribution.
- Market-level and region-level membership.
- Demographics (age, gender, race).
- Product/contract/brand enrollment breakdowns.
- Source system distribution.
- Year-over-year enrollment growth.'

    ai_verified_queries (
        enrollment_trend_mnr_ffs AS (
            QUESTION 'Show me the M&R FFS enrollment trend by month'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION TRUE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT fin_inc_month,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months
FROM kn_membership_semantic
WHERE population = ''M&R FFS''
GROUP BY fin_inc_month
ORDER BY fin_inc_month'
        ),
        membership_by_population AS (
            QUESTION 'What is the membership breakdown by population for 2025?'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION TRUE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT population,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months
FROM kn_membership_semantic
WHERE fin_inc_month BETWEEN ''202501'' AND ''202512''
GROUP BY population
ORDER BY SUM(member_months) DESC'
        ),
        top_markets_by_enrollment AS (
            QUESTION 'What are the top 10 markets by enrollment for M&R FFS?'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION TRUE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT fin_market,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months
FROM kn_membership_semantic
WHERE population = ''M&R FFS''
GROUP BY fin_market
ORDER BY SUM(member_months) DESC
LIMIT 10'
        ),
        population_mix_for_market AS (
            QUESTION 'Show me the population mix for Florida'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT population,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months
FROM kn_membership_semantic
WHERE fin_market = ''FL''
GROUP BY population
ORDER BY SUM(member_months) DESC'
        ),
        enrollment_by_gender AS (
            QUESTION 'Show enrollment by gender for M&R FFS'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT fin_gender,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months,
    ROUND(SUM(member_months) * 100.0 / SUM(SUM(member_months)) OVER (), 1) AS pct_of_total
FROM kn_membership_semantic
WHERE population = ''M&R FFS''
GROUP BY fin_gender
ORDER BY SUM(member_months) DESC'
        ),
        enrollment_by_region AS (
            QUESTION 'Compare enrollment between East and West regions'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT fin_region, population,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months
FROM kn_membership_semantic
GROUP BY fin_region, population
ORDER BY fin_region, SUM(member_months) DESC'
        ),
        yoy_enrollment_growth AS (
            QUESTION 'Show year over year enrollment growth from 2024 to 2025 for M&R FFS'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'WITH yr_2024 AS (
    SELECT SUM(member_months) AS mm_2024
    FROM kn_membership_semantic
    WHERE population = ''M&R FFS'' AND fin_inc_month BETWEEN ''202401'' AND ''202412''
),
yr_2025 AS (
    SELECT SUM(member_months) AS mm_2025
    FROM kn_membership_semantic
    WHERE population = ''M&R FFS'' AND fin_inc_month BETWEEN ''202501'' AND ''202512''
)
SELECT
    TO_CHAR(yr_2024.mm_2024, ''999,999,999,999'') AS member_months_2024,
    TO_CHAR(yr_2025.mm_2025, ''999,999,999,999'') AS member_months_2025,
    TO_CHAR(yr_2025.mm_2025 - yr_2024.mm_2024, ''999,999,999,999'') AS abs_change,
    ROUND((yr_2025.mm_2025 - yr_2024.mm_2024) / NULLIF(yr_2024.mm_2024, 0) * 100, 1) AS pct_change
FROM yr_2024
CROSS JOIN yr_2025'
        ),
        enrollment_pct_by_population AS (
            QUESTION 'What percent of total enrollment does each population represent?'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT population,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months,
    ROUND(SUM(member_months) * 100.0 / SUM(SUM(member_months)) OVER (), 1) AS pct_of_total
FROM kn_membership_semantic
GROUP BY population
ORDER BY SUM(member_months) DESC'
        ),
        enrollment_by_source AS (
            QUESTION 'Show enrollment by source system for M&R FFS'
            VERIFIED_AT 1721000000
            ONBOARDING_QUESTION FALSE
            VERIFIED_BY '(STEWARD = khang.nguyen@uhc.com)'
            SQL 'SELECT sgr_source_name,
    TO_CHAR(SUM(member_months), ''999,999,999,999'') AS total_member_months,
    ROUND(SUM(member_months) * 100.0 / SUM(SUM(member_months)) OVER (), 1) AS pct_of_total
FROM kn_membership_semantic
WHERE population = ''M&R FFS''
GROUP BY sgr_source_name
ORDER BY SUM(member_months) DESC'
        )
    )
;
