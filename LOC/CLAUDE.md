# LOC — Quick Reference
- One-liner: Inpatient leading indicator reporting, LOC valuation, and hospital anomaly detection
- Session variables: notifications_date, membership_month

## Sub-projects

| Sub-project | Status | Key artifact |
|---|---|---|
| Isolation Forest (hospital outlier detection) | Active | `LOC_IF_SHAP_no_ICM.ipynb` (production) |
| Leading Indicator / Notification Reporting | Active | `LOC_<notifdate>.sql` |
| Semantic View (IPA/LOC auths) | Active | `KN_AUTHS_SEMANTIC_BASE.SQL`, `kn_ipa_semantic_view.sql` |
| OD P2P Analysis | Active | `OD_P2P_MnR_07222026.sql`, `OD_P2P_shifted-vs-added.ipynb` |
| P2P Gap Analysis | Paused | `kn_loc_p2p_cases_with_gap.sql` |
| Changepoint Detection | On hold | `LOC_changepoint_detection.ipynb` |
| Shift-Share Decomposition | Exploratory | `LOC_shiftshare_decomposition.ipynb` |

## Key Tables
- Latest IP dataset: `tmp_1m.kn_ip_dataset_loc_08192026_od`
- Latest notifications: `tmp_1m.kn_loc_notif_08192026_od`
- Latest membership: `tmp_1m.kn_loc_mm_08192026_od`
- Semantic base (auths): `tmp_1m.kn_auths_semantic_base`
- Semantic base (IPA): `tmp_1m.kn_ipa_semantic_base`
- AVTAR source: `hce_ops_fnl.hce_adr_avtar_like_25_26_f`
- TIN lookup: `tmp_1y.tin_collection`

## Source Table Convention
For any LOC metric analysis, use `kn_ip_dataset_loc_{notifdate}_od` (has `population`, `admit_act_month`) or upstream `ec_ip_dataset_notif_{notifdate}_od`. Do NOT use `kn_loc_notif_*` or `kn_loc_od_p2p_*` as primary sources.

## On-demand context
To get current status and recent work, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\LOC\04 - Progress.md`

For methodology and sub-project details, read:
`C:\Users\knguy139\Documents\Work Vault\02 - Projects\LOC\01 - Context and Methodology.md`

## Update trigger
When asked to "update my work vault doc" or "update project notes", edit `04 - Progress.md` directly.
