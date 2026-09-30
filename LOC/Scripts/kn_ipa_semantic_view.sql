/*==============================================================================
 * SEMANTIC VIEW: VING_PRD_TREND_DB.TMP_1Q.INPATIENT_VIEW
 * Updated: 2026-07-17
 * Changes: Renamed tables, new population values, new dimensions, new metrics
 *==============================================================================*/

create or replace semantic view VING_PRD_TREND_DB.TMP_1Q.INPATIENT_VIEW
    tables (
        VING_PRD_TREND_DB.TMP_1M.KN_IPA_SEMANTIC_BASE,
        VING_PRD_TREND_DB.TMP_1M.KN_IPA_SEMANTIC_MEMBERSHIP unique (POPULATION, ADMIT_ACT_MONTH, FIN_MARKET, FIN_CONTRACT_NBR, FIN_BRAND, FIN_PRODUCT_LEVEL_3, FIN_STATE, GLOBAL_CAP, MIGRATION_SOURCE, TFM_INCLUDE_FLAG, SGR_SOURCE_NAME)
    )
    relationships (
        KN_IPA_SEMANTIC_BASE_TO_KN_IPA_SEMANTIC_MEMBERSHIP as KN_IPA_SEMANTIC_BASE(POPULATION, ADMIT_ACT_MONTH, FIN_MARKET, FIN_CONTRACT_NBR, FIN_BRAND, FIN_PRODUCT_LEVEL_3, FIN_STATE, GLOBAL_CAP, MIGRATION_SOURCE, TFM_INCLUDE_FLAG, SGR_SOURCE_NAME) references KN_IPA_SEMANTIC_MEMBERSHIP(POPULATION, ADMIT_ACT_MONTH, FIN_MARKET, FIN_CONTRACT_NBR, FIN_BRAND, FIN_PRODUCT_LEVEL_3, FIN_STATE, GLOBAL_CAP, MIGRATION_SOURCE, TFM_INCLUDE_FLAG, SGR_SOURCE_NAME)
    )
    facts (
        -- Time filter facts
        KN_IPA_SEMANTIC_BASE.ADMIT_MONTH_202604 labels = (filter) as admit_act_month = '202604'
            comment='Filters to April 2026 cases. Use for single-month scorecards covering initial ADR rate, persistency, MD escalation, P2P, and member appeal metrics.',
        KN_IPA_SEMANTIC_BASE.ADMIT_YEAR_2026 labels = (filter) as admit_year = '2026'
            comment='Filters to 2026 cases. Use for current year performance, year-to-date analysis. Commonly combined with population and loc_flag filters.',

        -- Population filter facts
        KN_IPA_SEMANTIC_BASE.MNR_FFS_IPA_PAC_CASES labels = (filter) as population = 'M&R FFS' AND ipa_pac_flag IN ('IPA', 'PAC')
            with synonyms=('ipa_pac_mnr_filter','mnr_ffs_ipa_pac','mnr_ffs_service_type')
            comment='Filters to M&R FFS population with IPA or PAC flag set.',
        KN_IPA_SEMANTIC_BASE.MNR_FFS_LOC_CASES labels = (filter) as LOC_FLAG = 1 AND POPULATION = 'M&R FFS'
            with synonyms=('ffs_loc_filter','mnr_ffs_location','mnr_ffs_with_loc')
            comment='Filters to M&R FFS population with LOC flag = 1. Most common filter for standard LOC reporting.',
        KN_IPA_SEMANTIC_BASE.ICM_OWNED_CASES labels = (filter) as icm_ever_owned = 1
            with synonyms=('icm_owned','icm_ever_owned_filter')
            comment='Filters to cases ever owned by ICM. Use when questions ask about ICM-owned cases or ICM ownership rate.',

        -- Numeric facts
        KN_IPA_SEMANTIC_MEMBERSHIP.MEMBER_MONTHS as MEMBER_MONTHS sample_values ('28176', '12067', '41013')
    )
    dimensions (
        -- === KN_IPA_SEMANTIC_BASE dimensions ===
        -- Time dimensions
        KN_IPA_SEMANTIC_BASE.ADMIT_ACT_MONTH as ADMIT_ACT_MONTH sample_values ('202510', '202602', '202504'),
        KN_IPA_SEMANTIC_BASE.ADMIT_ACT_QTR as ADMIT_ACT_QTR sample_values ('2026Q2', '2025Q4', '2026Q1'),
        KN_IPA_SEMANTIC_BASE.ADMIT_WEEK as ADMIT_WEEK sample_values ('202621', '202531', '202524'),
        KN_IPA_SEMANTIC_BASE.ADMIT_YEAR as ADMIT_YEAR sample_values ('2024', '2026', '2025'),

        -- Identifiers
        KN_IPA_SEMANTIC_BASE.CASE_ID as CASE_ID sample_values ('280078131', '275855236', '284540262'),
        KN_IPA_SEMANTIC_BASE.FIN_CONTRACT_NBR as FIN_CONTRACT_NBR sample_values ('H2247', 'H1045', 'H0421'),

        -- Geography / Segment dimensions
        KN_IPA_SEMANTIC_BASE.FIN_MARKET as FIN_MARKET sample_values ('UT', 'WI', 'MO'),
        KN_IPA_SEMANTIC_BASE.FIN_BRAND as FIN_BRAND
            comment='Brand segment: M&R (Medicare & Retirement) or C&S (Community & State).'
            sample_values ('M&R', 'C&S'),
        KN_IPA_SEMANTIC_BASE.FIN_PRODUCT_LEVEL_3 as FIN_PRODUCT_LEVEL_3
            comment='Product level 3 classification.'
            sample_values ('NETWORK', 'DUAL', 'INSTITUTIONAL'),
        KN_IPA_SEMANTIC_BASE.FIN_STATE as FIN_STATE
            comment='State abbreviation for the member.'
            sample_values ('AZ', 'CA', 'TX'),
        KN_IPA_SEMANTIC_BASE.GLOBAL_CAP as GLOBAL_CAP
            comment='Global capitation indicator. NA means not globally capitated.'
            sample_values ('NA', 'SC', 'CO'),
        KN_IPA_SEMANTIC_BASE.SGR_SOURCE_NAME as SGR_SOURCE_NAME
            with synonyms=('source_system','platform','claim_platform')
            comment='Source system name: COSMOS, NICE, CSP, or UNK.'
            sample_values ('COSMOS', 'NICE', 'CSP'),
        KN_IPA_SEMANTIC_BASE.MIGRATION_SOURCE as MIGRATION_SOURCE
            comment='Migration source identifier. OAH = Optum at Home.'
            sample_values ('OAH', 'CIP', 'NA'),
        KN_IPA_SEMANTIC_BASE.TFM_INCLUDE_FLAG as TFM_INCLUDE_FLAG
            comment='TFM inclusion indicator (0 or 1).'
            sample_values ('0', '1'),
        KN_IPA_SEMANTIC_BASE.NCE_TADM_DEC_RISK_TYPE as NCE_TADM_DEC_RISK_TYPE
            comment='NCE risk type for NICE-sourced cases.'
            sample_values ('FFS', 'PHYSICIAN', 'GLOBAL'),

        -- Provider dimensions
        KN_IPA_SEMANTIC_BASE.HOSPITAL_GROUP as HOSPITAL_GROUP
            comment='Hospital group or system name from TIN collection mapping.'
            sample_values ('CUPERTINO HEALTHCARE AND WELLNESS CENTER', 'PAVILION-THS', 'WYTHE COUNTY COMMUNITY HOSPITAL'),
        KN_IPA_SEMANTIC_BASE.GROUP_NAME as GROUP_NAME
            comment='Provider group name from authorization source.'
            sample_values ('UNIVERSAL HEALTH SERVICES', 'HCA HEALTHCARE', 'TENET HEALTHCARE'),
        KN_IPA_SEMANTIC_BASE.PROV_TIN as PROV_TIN with synonyms=('TIN') sample_values ('218014159', '463214504', '362169147'),
        KN_IPA_SEMANTIC_BASE.PAR_NONPAR as PAR_NONPAR sample_values ('Par', 'Non-Par', '0'),

        -- Clinical dimensions
        KN_IPA_SEMANTIC_BASE.SVC_SETTING as SVC_SETTING comment='Service Setting' sample_values ('Inpatient', 'Outpatient Physician', '0'),
        KN_IPA_SEMANTIC_BASE.PRIM_DIAG_CD as PRIM_DIAG_CD sample_values ('M25.851', 'Z47.1', 'A08.11'),
        KN_IPA_SEMANTIC_BASE.PROC_CD as PROC_CD comment='Procedure Code' sample_values ('28285', '22327', '38747'),
        KN_IPA_SEMANTIC_BASE.PRIM_PROC_IND as PRIM_PROC_IND sample_values ('Y', 'N', '0'),
        KN_IPA_SEMANTIC_BASE.LOS_CATEGORIES as LOS_CATEGORIES sample_values ('4-5', '3', '1'),
        KN_IPA_SEMANTIC_BASE.RESPIRATORY_FLAG as RESPIRATORY_FLAG sample_values ('NA', 'ILI', 'COVID-19'),

        -- Population / Classification dimensions
        KN_IPA_SEMANTIC_BASE.POPULATION as POPULATION sample_values ('M&R FFS', 'C&S DSNP', 'INSTITUTIONAL'),
        KN_IPA_SEMANTIC_BASE.IPA_PAC_FLAG as IPA_PAC_FLAG sample_values ('IPA', 'PAC'),
        KN_IPA_SEMANTIC_BASE.IPA_LI_SPLIT as IPA_LI_SPLIT sample_values ('Medical', 'Surgical', 'Transplant'),
        KN_IPA_SEMANTIC_BASE.LOC_FLAG as LOC_FLAG sample_values ('1', '0'),
        KN_IPA_SEMANTIC_BASE.ICM_EVER_OWNED as ICM_EVER_OWNED
            comment='ICM ownership indicator (0/1). Use as filter: icm_ever_owned = 1 for ICM-owned cases.'
            sample_values ('0', '1'),

        -- Indicator dimensions (used in metric formulas)
        KN_IPA_SEMANTIC_BASE.INITIALFULLADR_CASES as INITIALFULLADR_CASES with synonyms=('Initial ADR') comment='Initial ADR indicator (0/1)' sample_values ('1', '0'),
        KN_IPA_SEMANTIC_BASE.PERSISTENTFULLADR_CASES as PERSISTENTFULLADR_CASES sample_values ('0', '1'),
        KN_IPA_SEMANTIC_BASE.ICM_MD_REVIEWED_IND as ICM_MD_REVIEWED_IND sample_values ('0', '1'),
        KN_IPA_SEMANTIC_BASE.P2P_FULL_EVERTOUCHED_CNT as P2P_FULL_EVERTOUCHED_CNT sample_values ('1', '0'),
        KN_IPA_SEMANTIC_BASE.P2P_FULL_OVTN as P2P_FULL_OVTN sample_values ('0', '1'),
        KN_IPA_SEMANTIC_BASE.MCR_RECONSIDERATION_IND as MCR_RECONSIDERATION_IND
            comment='MCR reconsideration indicator (0/1). 1 = case went to MCR reconsideration.'
            sample_values ('0', '1'),
        KN_IPA_SEMANTIC_BASE.MCR_OVTRN_IND as MCR_OVTRN_IND
            comment='MCR overturn indicator (0/1). 1 = MCR reconsideration resulted in overturn.'
            sample_values ('0', '1'),
        KN_IPA_SEMANTIC_BASE.MEMBER_APPEAL_IND as MEMBER_APPEAL_IND sample_values ('1', '0'),
        KN_IPA_SEMANTIC_BASE.MEMBER_APPEAL_OVTN_IND as MEMBER_APPEAL_OVTN_IND sample_values ('0', '1'),

        -- === KN_IPA_SEMANTIC_MEMBERSHIP dimensions ===
        KN_IPA_SEMANTIC_MEMBERSHIP.ADMIT_ACT_MONTH as ADMIT_ACT_MONTH sample_values ('202410', '202605', '202504'),
        KN_IPA_SEMANTIC_MEMBERSHIP.FIN_CONTRACT_NBR as FIN_CONTRACT_NBR sample_values ('H3805', 'H1537', 'H2247'),
        KN_IPA_SEMANTIC_MEMBERSHIP.FIN_MARKET as FIN_MARKET sample_values ('ME', 'GU', 'WY'),
        KN_IPA_SEMANTIC_MEMBERSHIP.POPULATION as POPULATION sample_values ('M&R FFS', 'C&S DSNP', 'INSTITUTIONAL'),
        KN_IPA_SEMANTIC_MEMBERSHIP.FIN_BRAND as FIN_BRAND sample_values ('M&R', 'C&S'),
        KN_IPA_SEMANTIC_MEMBERSHIP.FIN_PRODUCT_LEVEL_3 as FIN_PRODUCT_LEVEL_3 sample_values ('NETWORK', 'DUAL', 'INSTITUTIONAL'),
        KN_IPA_SEMANTIC_MEMBERSHIP.FIN_STATE as FIN_STATE sample_values ('AZ', 'CA', 'TX'),
        KN_IPA_SEMANTIC_MEMBERSHIP.GLOBAL_CAP as GLOBAL_CAP sample_values ('NA', 'SC', 'CO'),
        KN_IPA_SEMANTIC_MEMBERSHIP.MIGRATION_SOURCE as MIGRATION_SOURCE sample_values ('OAH', 'CIP', 'NA'),
        KN_IPA_SEMANTIC_MEMBERSHIP.TFM_INCLUDE_FLAG as TFM_INCLUDE_FLAG sample_values ('0', '1'),
        KN_IPA_SEMANTIC_MEMBERSHIP.SGR_SOURCE_NAME as SGR_SOURCE_NAME sample_values ('COSMOS', 'NICE', 'CSP')
    )
    metrics (
        -- Volume
        KN_IPA_SEMANTIC_BASE.CASE_VOLUME as COUNT(DISTINCT kn_ipa_semantic_base.case_id)
            with synonyms=('authorization_volume','case_count')
            comment='Total distinct authorization cases. Use for case volume, number of cases, workload measurement.',

        -- Initial ADR rate
        KN_IPA_SEMANTIC_BASE.INITIAL_ADR_RATE as COUNT(DISTINCT CASE WHEN kn_ipa_semantic_base.initialfulladr_cases = 1 THEN kn_ipa_semantic_base.case_id END) / NULLIF(COUNT(DISTINCT kn_ipa_semantic_base.case_id), 0) * 100
            with synonyms=('adr_rate','initial_adverse_rate','initial_denial_rate')
            comment='Initial adverse determination rate %. Denominator = total case count. Use for denial rate, initial ADR rate.',

        -- Persistency
        KN_IPA_SEMANTIC_BASE.PERSISTENCY_RATE as COUNT(DISTINCT CASE WHEN persistentfulladr_cases = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN initialfulladr_cases = 1 THEN case_id END), 0) * 100
            comment='Persistency rate %. Denominator = initial ADR cases. Measures how often initial adverse determinations are upheld.',

        -- MD escalation
        KN_IPA_SEMANTIC_BASE.MD_ESCALATION_RATE as COUNT(DISTINCT CASE WHEN kn_ipa_semantic_base.icm_md_reviewed_ind = 1 THEN kn_ipa_semantic_base.case_id END) / NULLIF(COUNT(DISTINCT kn_ipa_semantic_base.case_id), 0) * 100
            with synonyms=('md_review_rate','medical_director_rate','physician_escalation_rate')
            comment='MD escalation rate %. Denominator = total case count. Measures cases requiring physician-level review.',

        -- P2P rate
        KN_IPA_SEMANTIC_BASE.P2P_RATE as COUNT(DISTINCT CASE WHEN p2p_full_evertouched_cnt = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN initialfulladr_cases = 1 THEN case_id END), 0) * 100
            comment='P2P review rate %. Denominator = initial ADR cases. Measures how often initial ADRs go to peer-to-peer review.',

        -- P2P overturn
        KN_IPA_SEMANTIC_BASE.P2P_OVERTURN_RATE as COUNT(DISTINCT CASE WHEN p2p_full_ovtn = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN p2p_full_evertouched_cnt = 1 THEN case_id END), 0) * 100
            comment='P2P overturn rate %. Denominator = P2P cases. Measures effectiveness of peer-to-peer reviews.',

        -- MCR reconsideration rate (NEW)
        KN_IPA_SEMANTIC_BASE.MCR_RATE as COUNT(DISTINCT CASE WHEN mcr_reconsideration_ind = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN initialfulladr_cases = 1 THEN case_id END), 0) * 100
            with synonyms=('mcr_reconsideration_rate')
            comment='MCR reconsideration rate %. Denominator = initial ADR cases.',

        -- MCR overturn (NEW)
        KN_IPA_SEMANTIC_BASE.MCR_OVERTURN_RATE as COUNT(DISTINCT CASE WHEN mcr_ovtrn_ind = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN mcr_reconsideration_ind = 1 THEN case_id END), 0) * 100
            comment='MCR overturn rate %. Denominator = MCR reconsideration cases.',

        -- Member appeal rate
        KN_IPA_SEMANTIC_BASE.MEMBER_APPEAL_RATE as COUNT(DISTINCT CASE WHEN member_appeal_ind = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN initialfulladr_cases = 1 THEN case_id END), 0) * 100
            comment='Member appeal rate %. Denominator = initial ADR cases. Measures member engagement in appeals.',

        -- Member appeal overturn
        KN_IPA_SEMANTIC_BASE.MEMBER_APPEAL_OVERTURN_RATE as COUNT(DISTINCT CASE WHEN member_appeal_ovtn_ind = 1 THEN case_id END) / NULLIF(COUNT(DISTINCT CASE WHEN member_appeal_ind = 1 THEN case_id END), 0) * 100
            comment='Member appeal overturn rate %. Denominator = member appeal cases.'
    )
    ai_sql_generation 'Metric formulas:
- All rates = (numerator / denominator) * 100. Round to 1 decimal. Express as percentages.
- Auth per K = (case_count / member_months) * 12000. Round to 1 decimal.
- All metrics use COUNT(DISTINCT case_id) pattern, not SUM().

Rate denominators:
- Initial ADR rate: denominator is case_count (count distinct case_id).
- Persistent ADR rate: denominator is case_count.
- MD escalation rate: denominator is case_count.
- P2P rate: denominator is initial_adr_cnt (count distinct case_id where initialfulladr_cases = 1).
- Appeal rate: denominator is initial_adr_cnt.
- MCR rate: denominator is initial_adr_cnt.
- Member appeal rate: denominator is initial_adr_cnt.
- Persistency: denominator is initial_adr_cnt.
- P2P overturn rate: denominator is p2p_case_cnt.
- MCR overturn rate: denominator is mcr_reconsideration_case_cnt.
- Member appeal overturn rate: denominator is member_appeal_cnt.

Formatting: Use TO_CHAR with ''999,999,999,999'' format for large integers (case counts, member months).

Membership grain:
- Exists at: population + admit_act_month + fin_market + fin_contract_nbr + fin_brand + fin_product_level_3 + fin_state + global_cap + migration_source + tfm_include_flag + sgr_source_name (or coarser).
- Does NOT exist at: hospital_group, prov_tin, par_nonpar, ipa_pac_flag, ipa_li_split, los_categories, respiratory_flag, svc_setting, group_name.

CTE pattern (mandatory for auth per K):
- ALWAYS aggregate each table separately in CTEs first, then join the aggregated results.
- Never join kn_ipa_semantic_membership to kn_ipa_semantic_base at the row level.
- Auth per K = SUM(case_count) / NULLIF(SUM(member_months), 0) * 12000 at any grain coarser than row level.
- Auth per K CANNOT be calculated at: hospital_group, prov_tin, par_nonpar, group_name, ipa_pac_flag, ipa_li_split, los_categories, or svc_setting level. If user asks for auth/K by hospital or TIN, explain that membership does not exist at that grain.

Disambiguation:
- "LOC" -> loc_flag = 1.
- "IPA" -> ipa_pac_flag = ''IPA''.
- "M&R FFS" / "MNR FFS" -> population = ''M&R FFS''.
- "denial rate" -> initial ADR rate.
- "ICM owned" -> icm_ever_owned = 1.
- "ADR" -> adverse determination rate, NOT adverse drug rate.
- "volume" / "case volume" / "how many cases" -> return case_count AND auth_per_k (if grain allows).
- "auth per K" / "auth/k" / "utilization rate" -> compute auth_per_k via CTE pattern.
- "brand" -> fin_brand (M&R or C&S).
- "platform" / "source system" -> sgr_source_name (COSMOS, NICE, CSP).
- "state" -> fin_state.
- "MCR rate" -> mcr_rate (MCR reconsideration, denominator = initial ADR).
- "member appeal" -> member_appeal_rate (member-initiated, denominator = initial ADR).'
    ai_verified_queries (
        "0;1" AS (
            QUESTION 'What is the month over month trend in initial ADR rate for MNR FFS population?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'WITH monthly AS (SELECT ADMIT_ACT_MONTH, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(initial_adr_cnt / NULLIF(case_count, 0) * 100, 1) AS initial_adr_rate_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' GROUP BY ADMIT_ACT_MONTH) SELECT ADMIT_ACT_MONTH, initial_adr_rate_pct, LAG(initial_adr_rate_pct) OVER (ORDER BY ADMIT_ACT_MONTH) AS prior_month_pct, ROUND(initial_adr_rate_pct - prior_month_pct, 1) AS mom_change FROM monthly ORDER BY ADMIT_ACT_MONTH'
        ),
        "1;1" AS (
            QUESTION 'What are all the key rates for level of care cases in the minor fee-for-service population for April 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT ROUND(COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE_ID), 0) * 100, 1) AS initial_adr_rate_pct, ROUND(COUNT(DISTINCT CASE WHEN PERSISTENTFULLADR_CASES = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END), 0) * 100, 1) AS persistency_pct, ROUND(COUNT(DISTINCT CASE WHEN ICM_MD_REVIEWED_IND = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE_ID), 0) * 100, 1) AS md_escalation_rate_pct, ROUND(COUNT(DISTINCT CASE WHEN P2P_FULL_EVERTOUCHED_CNT = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END), 0) * 100, 1) AS p2p_rate_pct, ROUND(COUNT(DISTINCT CASE WHEN P2P_FULL_OVTN = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN P2P_FULL_EVERTOUCHED_CNT = 1 THEN CASE_ID END), 0) * 100, 1) AS p2p_overturn_rate_pct, ROUND(COUNT(DISTINCT CASE WHEN MEMBER_APPEAL_IND = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END), 0) * 100, 1) AS member_appeal_rate_pct, ROUND(COUNT(DISTINCT CASE WHEN MEMBER_APPEAL_OVTN_IND = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN MEMBER_APPEAL_IND = 1 THEN CASE_ID END), 0) * 100, 1) AS member_appeal_overturn_rate_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_ACT_MONTH = ''202604'''
        ),
        "2;1" AS (
            QUESTION 'What is the persistency rate for Baptist SFL hospital for LOC MNR FFS by month?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, COUNT(DISTINCT CASE WHEN PERSISTENTFULLADR_CASES = 1 THEN CASE_ID END) AS persistent_adr_cnt, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, ROUND(persistent_adr_cnt / NULLIF(initial_adr_cnt, 0) * 100, 1) AS persistency_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND HOSPITAL_GROUP = ''Baptist SFL'' GROUP BY ADMIT_ACT_MONTH ORDER BY ADMIT_ACT_MONTH'
        ),
        "3;1" AS (
            QUESTION 'How do initial ADR rates compare between IPA and PAC for MNR FFS patients by month in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, IPA_PAC_FLAG, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(initial_adr_cnt / NULLIF(case_count, 0) * 100, 1) AS initial_adr_rate_pct FROM kn_ipa_semantic_base WHERE POPULATION = ''M&R FFS'' AND IPA_PAC_FLAG IN (''IPA'', ''PAC'') AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH, IPA_PAC_FLAG ORDER BY ADMIT_ACT_MONTH, IPA_PAC_FLAG'
        ),
        "4;1" AS (
            QUESTION 'What is the initial ADR rate by admit type for Medical versus Surgical cases in the MNR FFS population by month in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, IPA_LI_SPLIT, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(initial_adr_cnt / NULLIF(case_count, 0) * 100, 1) AS initial_adr_rate_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND IPA_LI_SPLIT IN (''Medical'', ''Surgical'') AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH, IPA_LI_SPLIT ORDER BY ADMIT_ACT_MONTH, IPA_LI_SPLIT'
        ),
        "5;1" AS (
            QUESTION 'How does the initial ADR rate vary across different populations by month in 2026?'
            VERIFIED_AT 1783694062 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION true
            SQL 'SELECT ADMIT_ACT_MONTH, POPULATION, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(initial_adr_cnt / NULLIF(case_count, 0) * 100, 1) AS initial_adr_rate_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH, POPULATION ORDER BY ADMIT_ACT_MONTH, POPULATION'
        ),
        "6;1" AS (
            QUESTION 'What is the distribution of length of stay categories for LOC MNR FFS patients in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT LOS_CATEGORIES, COUNT(DISTINCT CASE_ID) AS case_count FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY LOS_CATEGORIES ORDER BY case_count DESC'
        ),
        "7;1" AS (
            QUESTION 'What are the top 10 diagnosis codes by volume for MNR FFS patients in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT PRIM_DIAG_CD, COUNT(DISTINCT CASE_ID) AS case_count FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' AND NOT PRIM_DIAG_CD IS NULL GROUP BY PRIM_DIAG_CD ORDER BY case_count DESC LIMIT 10'
        ),
        "8;1" AS (
            QUESTION 'What is the case count and persistent ADR rate for LOC MNR FFS by market in 2026?'
            VERIFIED_AT 1783694546 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION true
            SQL 'select fin_market, count(distinct case_id) as case_count, count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt, round(persistent_adr_cnt / nullif(case_count, 0) * 100, 1) as persistent_adr_rate_pct from ving_prd_trend_db.tmp_1m.kn_ipa_semantic_base where loc_flag = 1 and population = ''M&R FFS'' and admit_year = ''2026'' group by fin_market order by fin_market'
        ),
        "9;1" AS (
            QUESTION 'Which contracts have the highest initial ADR rates for LOC MNR FFS patients in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT FIN_CONTRACT_NBR, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(initial_adr_cnt / NULLIF(case_count, 0) * 100, 1) AS initial_adr_rate_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY FIN_CONTRACT_NBR HAVING case_count >= 30 ORDER BY initial_adr_rate_pct DESC LIMIT 10'
        ),
        "10;1" AS (
            QUESTION 'What are the monthly P2P rates, P2P overturn rates, and persistency rates for LOC MNR FFS patients in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, ROUND(COUNT(DISTINCT CASE WHEN P2P_FULL_EVERTOUCHED_CNT = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END), 0) * 100, 1) AS p2p_rate_pct, ROUND(COUNT(DISTINCT CASE WHEN P2P_FULL_OVTN = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN P2P_FULL_EVERTOUCHED_CNT = 1 THEN CASE_ID END), 0) * 100, 1) AS p2p_overturn_rate_pct, ROUND(COUNT(DISTINCT CASE WHEN PERSISTENTFULLADR_CASES = 1 THEN CASE_ID END) / NULLIF(COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END), 0) * 100, 1) AS persistency_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH ORDER BY ADMIT_ACT_MONTH'
        ),
        "11;1" AS (
            QUESTION 'What is the persistency rate by hospital group for Medicare fee-for-service patients in 2026?'
            VERIFIED_AT 1783694147 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION true
            SQL 'SELECT HOSPITAL_GROUP, COUNT(DISTINCT CASE WHEN PERSISTENTFULLADR_CASES = 1 THEN CASE_ID END) AS persistent_adr_cnt, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, ROUND(persistent_adr_cnt / NULLIF(initial_adr_cnt, 0) * 100, 1) AS persistency_pct FROM kn_ipa_semantic_base WHERE POPULATION = ''M&R FFS'' AND LOC_FLAG = 1 AND ADMIT_YEAR = ''2026'' GROUP BY HOSPITAL_GROUP ORDER BY persistency_pct DESC having initial_adr_cnt >= 30'
        ),
        "13;1" AS (
            QUESTION 'What is the MD escalation rate by month for level of care cases in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, COUNT(DISTINCT CASE WHEN ICM_MD_REVIEWED_IND = 1 THEN CASE_ID END) AS md_reviewed_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(md_reviewed_cnt / NULLIF(case_count, 0) * 100, 1) AS md_escalation_rate_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH ORDER BY ADMIT_ACT_MONTH'
        ),
        "14;1" AS (
            QUESTION 'Which are the top 10 hospital groups with the highest initial ADR rates for MNR FFS patients in 2026?'
            VERIFIED_AT 1783693896 VERIFIED_BY 'Semantic Model Generator' ONBOARDING_QUESTION false
            SQL 'SELECT HOSPITAL_GROUP, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(initial_adr_cnt / NULLIF(case_count, 0) * 100, 1) AS initial_adr_rate_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY HOSPITAL_GROUP HAVING case_count >= 30 ORDER BY initial_adr_rate_pct DESC LIMIT 10'
        ),
        "15;1" AS (
            QUESTION 'Show persistent ADR rate and persistency for LOC MNR FFS by month in 2026'
            VERIFIED_AT 1783694580 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION true
            SQL 'select admit_act_month, count(distinct case_id) as case_count, count(distinct case when initialfulladr_cases = 1 then case_id end) as initial_adr_cnt, count(distinct case when persistentfulladr_cases = 1 then case_id end) as persistent_adr_cnt, round(persistent_adr_cnt / nullif(case_count, 0) * 100, 1) as persistent_adr_rate_pct, round(persistent_adr_cnt / nullif(initial_adr_cnt, 0) * 100, 1) as persistency_pct from ving_prd_trend_db.tmp_1m.kn_ipa_semantic_base where loc_flag = 1 and population = ''M&R FFS'' and admit_year = ''2026'' group by admit_act_month order by admit_act_month'
        ),
        "16;1" AS (
            QUESTION 'What is the monthly ICM ownership rate for level of care MNR FFS cases throughout 2026?'
            VERIFIED_AT 1783694159 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, COUNT(DISTINCT CASE WHEN ICM_EVER_OWNED = 1 THEN CASE_ID END) AS icm_owned_cnt, COUNT(DISTINCT CASE_ID) AS case_count, ROUND(icm_owned_cnt / NULLIF(case_count, 0) * 100, 1) AS icm_owned_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH ORDER BY ADMIT_ACT_MONTH'
        ),
        "17;1" AS (
            QUESTION 'What is the monthly trend of persistency rates for PAR vs non-PAR providers for M&R FFS population in 2026?'
            VERIFIED_AT 1783694007 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION true
            SQL 'SELECT ADMIT_ACT_MONTH, PAR_NONPAR, COUNT(DISTINCT CASE WHEN PERSISTENTFULLADR_CASES = 1 THEN CASE_ID END) AS persistent_adr_cnt, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, ROUND(persistent_adr_cnt / NULLIF(initial_adr_cnt, 0) * 100, 1) AS persistency_pct FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH, PAR_NONPAR ORDER BY ADMIT_ACT_MONTH, PAR_NONPAR'
        ),
        "18;1" AS (
            QUESTION 'What is the utilization (auth/k) by market for LOC MNR FFS in Q1 2026?'
            VERIFIED_AT 1783703550 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION true
            SQL 'with auths as (select fin_market, count(distinct case_id) as case_count from kn_ipa_semantic_base where loc_flag = 1 and population = ''M&R FFS'' and admit_act_qtr = ''2026Q1'' group by fin_market), mm as (select fin_market, sum(member_months) as total_mm from kn_ipa_semantic_membership where population = ''M&R FFS'' and admit_act_month between ''202601'' and ''202603'' group by fin_market) select a.fin_market, a.case_count, m.total_mm, round(a.case_count / nullif(m.total_mm, 0) * 12000, 1) as auth_per_k from auths as a left join mm as m on a.fin_market = m.fin_market order by auth_per_k desc'
        ),
        "19;1" AS (
            QUESTION 'What is the monthly auth per K trend with month-over-month change for LOC MNR FFS from 2025 onwards?'
            VERIFIED_AT 1783704013 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION true
            SQL 'with auths as (select admit_act_month, count(distinct case_id) as case_count from kn_ipa_semantic_base where loc_flag = 1 and population = ''M&R FFS'' and admit_act_month >= ''202501'' group by admit_act_month), mm as (select admit_act_month, sum(member_months) as total_mm from kn_ipa_semantic_membership where population = ''M&R FFS'' and admit_act_month >= ''202501'' group by admit_act_month), combined as (select a.admit_act_month, a.case_count, m.total_mm, round(a.case_count / nullif(m.total_mm, 0) * 12000, 1) as auth_per_k from auths as a left join mm as m on a.admit_act_month = m.admit_act_month) select admit_act_month, case_count, auth_per_k, lag(auth_per_k) over (order by admit_act_month) as prior_month, round((auth_per_k - prior_month) / nullif(prior_month, 0) * 100, 1) as mom_pct_change from combined order by admit_act_month'
        ),
        "20;1" AS (
            QUESTION 'What is the member appeal overturn rate by month for LOC MNR FFS in 2026?'
            VERIFIED_AT 1752782400 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, COUNT(DISTINCT CASE WHEN MEMBER_APPEAL_IND = 1 THEN CASE_ID END) AS member_appeal_cnt, COUNT(DISTINCT CASE WHEN MEMBER_APPEAL_OVTN_IND = 1 THEN CASE_ID END) AS member_appeal_ovtn_cnt, ROUND(member_appeal_ovtn_cnt / NULLIF(member_appeal_cnt, 0) * 100, 1) AS member_appeal_overturn_rate FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH ORDER BY ADMIT_ACT_MONTH'
        ),
        "21;1" AS (
            QUESTION 'What is the initial ADR rate by product level 3 for LOC MNR FFS in 2026?'
            VERIFIED_AT 1752782400 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION false
            SQL 'SELECT FIN_PRODUCT_LEVEL_3, COUNT(DISTINCT CASE_ID) AS case_count, COUNT(DISTINCT CASE WHEN INITIALFULLADR_CASES = 1 THEN CASE_ID END) AS initial_adr_cnt, ROUND(initial_adr_cnt / NULLIF(case_count, 0) * 100, 1) AS initial_adr_rate FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ADMIT_YEAR = ''2026'' GROUP BY FIN_PRODUCT_LEVEL_3 ORDER BY case_count DESC'
        ),
        "22;1" AS (
            QUESTION 'What is the auth per K by state for LOC MNR FFS in 2026?'
            VERIFIED_AT 1752782400 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION false
            SQL 'with auths as (select fin_state, count(distinct case_id) as case_count from kn_ipa_semantic_base where loc_flag = 1 and population = ''M&R FFS'' and admit_year = ''2026'' group by fin_state), mm as (select fin_state, sum(member_months) as total_mm from kn_ipa_semantic_membership where population = ''M&R FFS'' and admit_act_month between ''202601'' and ''202606'' group by fin_state) select a.fin_state, a.case_count, m.total_mm, round(a.case_count / nullif(m.total_mm, 0) * 12000, 1) as auth_per_k from auths as a left join mm as m on a.fin_state = m.fin_state order by auth_per_k desc'
        ),
        "23;1" AS (
            QUESTION 'What is the case volume by month for ICM-owned LOC MNR FFS cases in 2026?'
            VERIFIED_AT 1752782400 VERIFIED_BY 'Khang Nguyen' ONBOARDING_QUESTION false
            SQL 'SELECT ADMIT_ACT_MONTH, COUNT(DISTINCT CASE_ID) AS case_count FROM kn_ipa_semantic_base WHERE LOC_FLAG = 1 AND POPULATION = ''M&R FFS'' AND ICM_EVER_OWNED = 1 AND ADMIT_YEAR = ''2026'' GROUP BY ADMIT_ACT_MONTH ORDER BY ADMIT_ACT_MONTH'
        )
    )
;
