create or replace semantic view tmp_1q.pa_view
    tables (
        VING_PRD_TREND_DB.TMP_1M.KN_PA_SEMANTIC_BASE,
        VING_PRD_TREND_DB.TMP_1M.KN_IPA_SEMANTIC_MEMBERSHIP
            unique (POPULATION, ADMIT_ACT_MONTH, FIN_MARKET, FIN_CONTRACT_NBR, FIN_BRAND, FIN_PRODUCT_LEVEL_3, FIN_STATE, GLOBAL_CAP, MIGRATION_SOURCE, TFM_INCLUDE_FLAG, SGR_SOURCE_NAME)
    )
    relationships (
        KN_PA_SEMANTIC_BASE_TO_KN_IPA_SEMANTIC_MEMBERSHIP
            as KN_PA_SEMANTIC_BASE(POPULATION, NOTIF_YRMONTH, FIN_MARKET, FIN_CONTRACT_NBR, FIN_BRAND, FIN_PRODUCT_LEVEL_3, FIN_STATE, GLOBAL_CAP, MIGRATION_SOURCE, TFM_INCLUDE_FLAG, SGR_SOURCE_NAME)
            references KN_IPA_SEMANTIC_MEMBERSHIP(POPULATION, ADMIT_ACT_MONTH, FIN_MARKET, FIN_CONTRACT_NBR, FIN_BRAND, FIN_PRODUCT_LEVEL_3, FIN_STATE, GLOBAL_CAP, MIGRATION_SOURCE, TFM_INCLUDE_FLAG, SGR_SOURCE_NAME)
    )
    facts (
        KN_PA_SEMANTIC_BASE.MNR_FFS_CASES labels = (filter)
            as POPULATION = 'M&R FFS' AND HCBS_FLAG = 0
            comment='Filters to M&R FFS population excluding HCBS procedure codes.',
        KN_PA_SEMANTIC_BASE.YEAR_2025 labels = (filter)
            as NOTIF_YEAR = '2025'
            comment='Filters to notification year 2025.',
        KN_PA_SEMANTIC_BASE.YEAR_2026 labels = (filter)
            as NOTIF_YEAR = '2026'
            comment='Filters to notification year 2026.',
        KN_IPA_SEMANTIC_MEMBERSHIP.MEMBER_MONTHS
            as MEMBER_MONTHS
            sample_values ('28176', '12067', '41013')
    )
    dimensions (
        -- Time
        KN_PA_SEMANTIC_BASE.NOTIF_YRMONTH as NOTIF_YRMONTH
            sample_values ('202501', '202604', '202410'),
        KN_PA_SEMANTIC_BASE.NOTIF_YEAR as NOTIF_YEAR
            sample_values ('2024', '2025', '2026'),
        KN_PA_SEMANTIC_BASE.NOTIF_QTR as NOTIF_QTR
            sample_values ('2025Q1', '2026Q2', '2024Q4'),
        -- Identifiers
        KN_PA_SEMANTIC_BASE.CASE_ID as CASE_ID,
        KN_PA_SEMANTIC_BASE.FIN_MBI_HICN_FNL as FIN_MBI_HICN_FNL,
        -- Service details
        KN_PA_SEMANTIC_BASE.PROC_CD as PROC_CD
            sample_values ('99213', '99214', '77067'),
        KN_PA_SEMANTIC_BASE.PA_PROGRAM as PA_PROGRAM
            comment='Prior authorization program: ECS, Optum, eviCore, NaviHealth, OrthoNet, UCS, etc.'
            sample_values ('Optum', 'ECS', 'eviCore', 'Home and Community Care (NaviHealth)'),
        KN_PA_SEMANTIC_BASE.ENTITY as ENTITY
            sample_values ('COSMOS', 'NICE'),
        -- Decision details
        KN_PA_SEMANTIC_BASE.CASE_INIT_DECN_CD as CASE_INIT_DECN_CD
            comment='Initial case decision code.'
            sample_values ('FA - Fully Approved', 'AD - Fully Adverse Determination'),
        KN_PA_SEMANTIC_BASE.CASE_DECN_STAT_CD as CASE_DECN_STAT_CD
            comment='Final case decision status code.'
            sample_values ('FA - Fully Approved', 'AD - Fully Adverse Determination', 'FM - Fully Mixed'),
        -- Population and segmentation
        KN_PA_SEMANTIC_BASE.POPULATION as POPULATION
            sample_values ('M&R FFS', 'C&S DSNP', 'OAH', 'INSTITUTIONAL', 'Other'),
        KN_PA_SEMANTIC_BASE.FIN_BRAND as FIN_BRAND
            sample_values ('M&R', 'C&S'),
        KN_PA_SEMANTIC_BASE.FIN_PRODUCT_LEVEL_3 as FIN_PRODUCT_LEVEL_3
            sample_values ('NETWORK', 'DUAL', 'INSTITUTIONAL'),
        KN_PA_SEMANTIC_BASE.FIN_MARKET as FIN_MARKET
            sample_values ('FL', 'TX', 'CA'),
        KN_PA_SEMANTIC_BASE.FIN_STATE as FIN_STATE
            sample_values ('AZ', 'CA', 'TX'),
        KN_PA_SEMANTIC_BASE.FIN_G_I as FIN_G_I
            sample_values ('G', 'I'),
        KN_PA_SEMANTIC_BASE.FIN_CONTRACT_NBR as FIN_CONTRACT_NBR
            sample_values ('H2247', 'H1045', 'H0421'),
        KN_PA_SEMANTIC_BASE.SGR_SOURCE_NAME as SGR_SOURCE_NAME
            sample_values ('COSMOS', 'NICE', 'CSP'),
        KN_PA_SEMANTIC_BASE.MIGRATION_SOURCE as MIGRATION_SOURCE
            sample_values ('OAH', 'NA', 'CIP'),
        KN_PA_SEMANTIC_BASE.GLOBAL_CAP as GLOBAL_CAP
            sample_values ('NA', 'SC', 'CO'),
        KN_PA_SEMANTIC_BASE.TFM_INCLUDE_FLAG as TFM_INCLUDE_FLAG
            sample_values ('0', '1'),
        KN_PA_SEMANTIC_BASE.NCE_TADM_DEC_RISK_TYPE as NCE_TADM_DEC_RISK_TYPE
            sample_values ('FFS', 'RISK'),
        KN_PA_SEMANTIC_BASE.GROUP_NUMBER as GROUP_NUMBER,
        KN_PA_SEMANTIC_BASE.GROUP_NAME as GROUP_NAME,
        KN_PA_SEMANTIC_BASE.HCBS_FLAG as HCBS_FLAG
            comment='1 = HCBS/home health procedure code (T/S codes not in AVTAR for CnS Duals). Default filter excludes these.'
            sample_values ('0', '1') is_enum,
        -- Membership
        KN_IPA_SEMANTIC_MEMBERSHIP.ADMIT_ACT_MONTH as ADMIT_ACT_MONTH
            sample_values ('202501', '202604', '202410'),
        KN_IPA_SEMANTIC_MEMBERSHIP.POPULATION as POPULATION
            sample_values ('M&R FFS', 'C&S DSNP', 'OAH', 'INSTITUTIONAL', 'Other')
    )
    metrics (
        KN_PA_SEMANTIC_BASE.CASE_COUNT
            as COUNT(DISTINCT kn_pa_semantic_base.case_id)
            with synonyms=('case_volume','authorization_count','auth_count','cases')
            comment='Total distinct prior authorization cases.',
        KN_PA_SEMANTIC_BASE.INITIAL_ADR_COUNT
            as COUNT(DISTINCT CASE WHEN kn_pa_semantic_base.initialfulladr_cases = 1 THEN kn_pa_semantic_base.case_id END)
            with synonyms=('initial_denials','initial_adverse_count')
            comment='Count of cases with initial adverse determination.',
        KN_PA_SEMANTIC_BASE.INITIAL_ADR_RATE
            as COUNT(DISTINCT CASE WHEN kn_pa_semantic_base.initialfulladr_cases = 1 THEN kn_pa_semantic_base.case_id END) / NULLIF(COUNT(DISTINCT kn_pa_semantic_base.case_id), 0) * 100
            with synonyms=('adr_rate','initial_denial_rate','initial_adverse_rate')
            comment='Initial adverse determination rate %. Denominator = total case count.',
        KN_PA_SEMANTIC_BASE.PERSISTENT_ADR_COUNT
            as COUNT(DISTINCT CASE WHEN kn_pa_semantic_base.persistentfulladr_cases = 1 THEN kn_pa_semantic_base.case_id END)
            with synonyms=('persistent_denials','final_adverse_count')
            comment='Count of cases with persistent (final) adverse determination.',
        KN_PA_SEMANTIC_BASE.PERSISTENT_ADR_RATE
            as COUNT(DISTINCT CASE WHEN kn_pa_semantic_base.persistentfulladr_cases = 1 THEN kn_pa_semantic_base.case_id END) / NULLIF(COUNT(DISTINCT kn_pa_semantic_base.case_id), 0) * 100
            with synonyms=('persistent_denial_rate','final_adverse_rate')
            comment='Persistent adverse determination rate %. Denominator = total case count.',
        KN_PA_SEMANTIC_BASE.PERSISTENCY
            as COUNT(DISTINCT CASE WHEN kn_pa_semantic_base.persistentfulladr_cases = 1 THEN kn_pa_semantic_base.case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN kn_pa_semantic_base.initialfulladr_cases = 1 THEN kn_pa_semantic_base.case_id END), 0) * 100
            with synonyms=('persistency_rate','denial_persistency')
            comment='Persistency rate %. Of cases initially denied, what % remained denied at final decision. Denominator = initial ADR count.'
    )
    ai_sql_generation 'Default behavior:
- Always default to population = ''M&R FFS'' AND hcbs_flag = 0 unless the user specifies a different population or explicitly asks about HCBS.
- HCBS flag = 1 means home/community-based service procedure codes (T/S codes). These are excluded by default because they do not align with the AVTAR for CnS Duals.
- When showing auth per K, always use the CTE pattern (see below).

Metric formulas:
- Case count = COUNT(DISTINCT case_id).
- Auth per K = COUNT(DISTINCT case_id) / SUM(member_months) * 12000. Round to 1 decimal.
- Initial ADR rate = COUNT(DISTINCT CASE WHEN initialfulladr_cases = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT case_id), 0) * 100. Round to 1 decimal.
- Persistent ADR rate = COUNT(DISTINCT CASE WHEN persistentfulladr_cases = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT case_id), 0) * 100. Round to 1 decimal.
- Persistency = persistent_adr_count / initial_adr_count * 100. Round to 1 decimal.
- All rates are expressed as percentages (multiply by 100).

Membership grain:
- Exists at: population + notif_yrmonth (as admit_act_month) + fin_market + fin_contract_nbr + fin_brand + fin_product_level_3 + fin_state + global_cap + migration_source + tfm_include_flag + sgr_source_name (or coarser).
- Does NOT exist at: proc_cd, pa_program, case_id, entity, group_number, group_name, hcbs_flag, case_init_decn_cd, case_decn_stat_cd, fin_g_i.

CTE pattern (mandatory for auth per K):
- ALWAYS aggregate PA cases and membership in SEPARATE CTEs first, then join the aggregated results.
- Never join kn_ipa_semantic_membership to kn_pa_semantic_base at the row level.
- Join on the dimensions common to both CTEs (subset of: population, notif_yrmonth=admit_act_month, fin_market, fin_contract_nbr, fin_brand, fin_product_level_3, fin_state, global_cap, migration_source, tfm_include_flag, sgr_source_name).

Population details:
- "M&R FFS" / "Medicare FFS" / "MNR" = Medicare and Retirement Fee-for-Service.
- "C&S DSNP" / "Community DSNP" / "dual" / "DSNP" = Community and State Dual Special Needs Plan.
- "INSTITUTIONAL" / "ISNP" = Institutional Special Needs Plan.
- "OAH" / "Optum at Home" = Optum at Home population.
- "Other" = catch-all.

PA program synonyms:
- "NaviHealth" / "home health" / "HCC" = pa_program = ''Home and Community Care (NaviHealth)''.
- "eviCore" = pa_program = ''eviCore''.
- "ECS" = pa_program = ''ECS''.

Formatting:
- Use TO_CHAR with ''999,999,999,999'' for large integers.
- Rates: ROUND(metric, 1) with no TO_CHAR (leave as numeric for sorting).'

    ai_question_categorization 'If the user asks "why did ADR rate increase", "what is driving denials", "explain the change":
- Generate a breakdown showing the components (by month, pa_program, proc_cd, fin_market) that contribute most to the change.
- Add disclaimer: "This shows where the change is concentrated. Root cause (policy changes, clinical criteria updates, staffing) requires operational context beyond this dataset."

If the user asks about appeal rates, overturn rates, P2P rates, MCR reconsideration:
- Respond that this PA view covers initial and persistent ADR metrics only. For appeal/overturn/P2P metrics, use the inpatient_view (INPATIENT_VIEW) which tracks the full appeals funnel.'
;
