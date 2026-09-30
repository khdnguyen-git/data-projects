create or replace semantic view tmp_1q.prmm_view
    tables (
        VING_PRD_TREND_DB.TMP_1M.KN_PRMM_SEMANTIC_BASE,
        VING_PRD_TREND_DB.TMP_1M.KN_PRMM_SEMANTIC_MEMBERSHIP
            unique (POPULATION, SRVC_MONTH, MARKET_FNL, BRAND_FNL, PRODUCT_LEVEL_3_FNL)
    )
    relationships (
        KN_PRMM_SEMANTIC_BASE_TO_KN_PRMM_SEMANTIC_MEMBERSHIP
            as KN_PRMM_SEMANTIC_BASE(POPULATION, SRVC_MONTH, MARKET_FNL, BRAND_FNL, PRODUCT_LEVEL_3_FNL)
            references KN_PRMM_SEMANTIC_MEMBERSHIP(POPULATION, SRVC_MONTH, MARKET_FNL, BRAND_FNL, PRODUCT_LEVEL_3_FNL)
    )
    facts (
        KN_PRMM_SEMANTIC_BASE.MNR_FFS_PAID labels = (filter)
            as POPULATION = 'M&R FFS' AND DENIAL_FLAG = 'Paid'
            comment='Filters to M&R FFS population with paid claims.',
        KN_PRMM_SEMANTIC_BASE.YEAR_2025 labels = (filter)
            as SRVC_YEAR = '2025'
            comment='Filters to service year 2025.',
        KN_PRMM_SEMANTIC_BASE.YEAR_2026 labels = (filter)
            as SRVC_YEAR = '2026'
            comment='Filters to service year 2026.',
        KN_PRMM_SEMANTIC_MEMBERSHIP.MEMBER_MONTHS
            as MEMBER_MONTHS
            comment='Count of member-months at this grain. SUM for total enrollment volume.'
            sample_values ('28176', '12067', '41013')
    )
    dimensions (
        -- PR Base: time
        KN_PRMM_SEMANTIC_BASE.SRVC_DT as SRVC_DT
            with synonyms=('service date', 'dos')
            comment='Service date (first date of service on the claim line).',
        KN_PRMM_SEMANTIC_BASE.SRVC_MONTH as SRVC_MONTH
            with synonyms=('month', 'service month')
            comment='Service month (YYYYMM). Primary time dimension.'
            sample_values ('202501', '202604', '202410'),
        KN_PRMM_SEMANTIC_BASE.SRVC_QTR as SRVC_QTR
            with synonyms=('quarter')
            comment='Service quarter.'
            sample_values ('2025Q1', '2026Q2', '2024Q4'),
        KN_PRMM_SEMANTIC_BASE.SRVC_YEAR as SRVC_YEAR
            with synonyms=('year')
            comment='Service year.'
            sample_values ('2024', '2025', '2026'),
        KN_PRMM_SEMANTIC_BASE.REVISED_ADJD_DT as REVISED_ADJD_DT
            with synonyms=('reprocessing date', 'revised date')
            comment='Revised adjudication date (latest reprocessing).',
        KN_PRMM_SEMANTIC_BASE.CLM_PD_DT as CLM_PD_DT
            with synonyms=('paid date', 'process date')
            comment='Claim paid/processed date.',

        -- PR Base: identifiers
        KN_PRMM_SEMANTIC_BASE.MBI as MBI
            with synonyms=('member', 'member id', 'hicn')
            comment='Member identifier (fin_mbi_hicn_fnl).',
        KN_PRMM_SEMANTIC_BASE.SITE_CLM_AUD_NBR as SITE_CLM_AUD_NBR
            with synonyms=('claim number', 'audit number', 'claim id')
            comment='Claim audit number. Unique claim identifier.',

        -- PR Base: service
        KN_PRMM_SEMANTIC_BASE.PROC_CD as PROC_CD
            with synonyms=('procedure code', 'cpt', 'hcpcs')
            comment='CPT/HCPCS procedure code.'
            sample_values ('99213', '99214', '36415'),

        -- PR Base: adjudication
        KN_PRMM_SEMANTIC_BASE.REVISED_FNL_RSN_CD as REVISED_FNL_RSN_CD
            with synonyms=('reason code', 'revised reason')
            comment='Revised final reason code (latest adjudication). 026 = LOPA.'
            sample_values ('026', '038', '264'),
        KN_PRMM_SEMANTIC_BASE.REVISED_FNL_RSN_DESC as REVISED_FNL_RSN_DESC
            with synonyms=('reason description')
            comment='Revised final reason code description.',

        -- PR Base: provider
        KN_PRMM_SEMANTIC_BASE.PROV_TIN as PROV_TIN
            with synonyms=('tin', 'tax id')
            comment='Provider Tax Identification Number.',
        KN_PRMM_SEMANTIC_BASE.SRVC_PROV_ID as SRVC_PROV_ID
            with synonyms=('provider id')
            comment='Servicing provider ID.',
        KN_PRMM_SEMANTIC_BASE.PROV_PRTCP_STS_CD as PROV_PRTCP_STS_CD
            with synonyms=('par status', 'participation')
            comment='Provider participation status. P=participating, N=non-participating.'
            sample_values ('P', 'N'),

        -- PR Base: geography + plan
        KN_PRMM_SEMANTIC_BASE.MARKET_FNL as MARKET_FNL
            with synonyms=('market', 'state')
            comment='Geographic market (state-level).'
            sample_values ('TX', 'FL', 'CA'),
        KN_PRMM_SEMANTIC_BASE.BRAND_FNL as BRAND_FNL
            with synonyms=('brand')
            comment='Product brand: M&R (Medicare & Retirement) or C&S (Community & State).'
            sample_values ('M&R', 'C&S'),
        KN_PRMM_SEMANTIC_BASE.PLAN_LEVEL_2_FNL as PLAN_LEVEL_2_FNL
            sample_values ('HMOPOS', 'NPPO', 'LPPO', 'HMO', 'RPPO'),
        KN_PRMM_SEMANTIC_BASE.PRODUCT_LEVEL_2_FNL as PRODUCT_LEVEL_2_FNL
            sample_values ('NETWORK', 'NATIONAL', 'SNP'),
        KN_PRMM_SEMANTIC_BASE.PRODUCT_LEVEL_3_FNL as PRODUCT_LEVEL_3_FNL
            with synonyms=('product level')
            comment='Product sub-type.'
            sample_values ('FFS', 'DUAL', 'INSTITUTIONAL'),
        KN_PRMM_SEMANTIC_BASE.CONTRACTPBP_FNL as CONTRACTPBP_FNL
            with synonyms=('contract', 'pbp')
            comment='Contract + PBP identifier.',

        -- PR Base: diagnosis
        KN_PRMM_SEMANTIC_BASE.PRIMARY_DIAG_CD as PRIMARY_DIAG_CD
            with synonyms=('diagnosis', 'icd', 'dx')
            comment='Primary ICD-10 diagnosis code. Use LEFT(primary_diag_cd, 3) for category filtering.'
            sample_values ('M79.3', 'Z23', 'R10.9'),
        KN_PRMM_SEMANTIC_BASE.AHRQ_DIAG_GENL_CATGY_DESC as AHRQ_DIAG_GENL_CATGY_DESC
            with synonyms=('diagnosis category')
            comment='AHRQ general diagnosis category.',
        KN_PRMM_SEMANTIC_BASE.AHRQ_DIAG_DTL_CATGY_DESC as AHRQ_DIAG_DTL_CATGY_DESC
            with synonyms=('diagnosis detail')
            comment='AHRQ detailed diagnosis category.',

        -- PR Base: flags
        KN_PRMM_SEMANTIC_BASE.SKIN_SUB_FLAG as SKIN_SUB_FLAG
            with synonyms=('skin sub')
            comment='Skin substitute procedure indicator. Y/N.'
            sample_values ('Y', 'N'),
        KN_PRMM_SEMANTIC_BASE.SUBCATEGORY as SUBCATEGORY
            comment='Skin sub type: Application, Covered, Unproven. NULL otherwise.'
            sample_values ('Application', 'Covered', 'Unproven'),
        KN_PRMM_SEMANTIC_BASE.MBR_DOS_LATEST_SUBMISSION as MBR_DOS_LATEST_SUBMISSION
            with synonyms=('submission number')
            comment='Submission sequence for MBI+DOS+PROC. 1 = latest/most recent.',
        KN_PRMM_SEMANTIC_BASE.DENIAL_FLAG as DENIAL_FLAG
            with synonyms=('status', 'paid status')
            comment='Claim payment status.'
            sample_values ('Paid', 'Denied'),
        KN_PRMM_SEMANTIC_BASE.POPULATION as POPULATION
            with synonyms=('pop', 'segment')
            comment='Population segment. Default = M&R FFS.'
            sample_values ('M&R FFS', 'OAH', 'Other'),

        -- Membership
        KN_PRMM_SEMANTIC_MEMBERSHIP.FIN_GENDER as FIN_GENDER
            with synonyms=('gender', 'sex')
            comment='Member gender.'
            sample_values ('M', 'F'),
        KN_PRMM_SEMANTIC_MEMBERSHIP.AGE as AGE
            comment='Member age at time of enrollment month.',
        KN_PRMM_SEMANTIC_MEMBERSHIP.REGION_FNL as REGION_FNL
            with synonyms=('region')
            comment='Region: EAST or WEST.'
            sample_values ('EAST', 'WEST'),
        KN_PRMM_SEMANTIC_MEMBERSHIP.GROUP_IND_FNL as GROUP_IND_FNL
            with synonyms=('group individual')
            comment='Group (G) or Individual (I) membership.'
            sample_values ('G', 'I'),
        KN_PRMM_SEMANTIC_MEMBERSHIP.SGR_SOURCE_NAME as SGR_SOURCE_NAME
            with synonyms=('platform', 'source')
            comment='Data platform/source system.'
            sample_values ('COSMOS', 'CSP', 'NICE'),
        KN_PRMM_SEMANTIC_MEMBERSHIP.FIN_RACE_CD as FIN_RACE_CD
            with synonyms=('race')
            comment='Numeric race code.'
            sample_values ('1', '2', '3', '4', '5')
    )
    metrics (
        KN_PRMM_SEMANTIC_BASE.TOTAL_ALLOWED
            as SUM(CLM_REC_RLUP_ALLW_AMT)
            with synonyms=('allowed', 'spend', 'cost')
            comment='Total allowed amount (rollup). Standard dollar metric.',
        KN_PRMM_SEMANTIC_BASE.TOTAL_PAID
            as SUM(CLM_REC_RLP_NET_PD_AMT)
            with synonyms=('net paid', 'paid amount')
            comment='Total net paid amount (after cost-sharing).',
        KN_PRMM_SEMANTIC_BASE.CLAIM_LINE_COUNT
            as COUNT(*)
            with synonyms=('lines', 'volume')
            comment='Number of claim lines.',
        KN_PRMM_SEMANTIC_BASE.UNIQUE_MEMBERS
            as COUNT(DISTINCT MBI)
            with synonyms=('members', 'member count')
            comment='Distinct member count.',
        KN_PRMM_SEMANTIC_BASE.LOPA_COUNT
            as SUM(LOPA_IND_NEW)
            with synonyms=('lopa', 'lopa claims')
            comment='LOPA count (latest submission + revised rsn 026).'
    )
    ai_sql_generation 'Default behavior:
- Always default to population = ''M&R FFS'' AND denial_flag = ''Paid'' unless user specifies otherwise.
- This view contains ONLY professional (PR) claims.

Metric formulas:
- PMPM = SUM(clm_rec_rlup_allw_amt) / SUM(member_months). NO multiplier. Round to 2 decimals.

Membership grain:
- Exists at: population + srvc_month + market_fnl + brand_fnl + product_level_3_fnl.
- Does NOT exist at: proc_cd, prov_tin, denial_flag, revised_fnl_rsn_cd, skin_sub_flag, etc.

CTE pattern (mandatory for PMPM):
- ALWAYS aggregate claims and membership in SEPARATE CTEs first, then join.
- Never join membership to base at row level.
- Join on (population, srvc_month, market_fnl, brand_fnl, product_level_3_fnl).

LOPA:
- lopa_ind_new = latest submission (mbr_dos_latest_submission = 1) + revised rsn 026.
- SUM(lopa_ind_new) for LOPA count.

Skin substitute:
- skin_sub_flag = ''Y''. subcategory for breakdowns.

Population:
- "M&R FFS" / "Medicare" = Medicare Fee-for-Service.
- "OAH" = Optum at Home.
- "Other" = remaining.'
;
