import numpy as np
import pandas as pd
from pathlib import Path

rng = np.random.default_rng(42)

# ── helpers ────────────────────────────────────────────────────────────────────

def mess_case(values, arr):
    """randomly assign casing variants from a list of variants"""
    idx = rng.integers(0, len(values), size=len(arr))
    return np.array(values)[idx]

def mess_yn(arr):
    """randomly encode boolean array as mixed Y/N/Yes/No/yes/no"""
    variants_yes = ["Y", "Yes", "yes"]
    variants_no  = ["N", "No", "no"]
    out = np.where(arr, rng.choice(variants_yes, size=len(arr)),
                        rng.choice(variants_no,  size=len(arr)))
    return out

# ── Project 1: HIV ML ──────────────────────────────────────────────────────────
print("Generating HIV dataset...")

N = 50_000

# latent risk score drives HIV status
risk = rng.normal(0, 1, N)

gender_clean     = rng.choice(["male", "female"], p=[0.55, 0.45], size=N)
race_clean       = rng.choice(["Black", "Hispanic", "White", "Asian", "Other"],
                               p=[0.30, 0.25, 0.28, 0.10, 0.07], size=N)
education_clean  = rng.choice([1, 2, 3], p=[0.30, 0.45, 0.25], size=N)  # 1=<HS, 2=HS, 3=College+

# behavioral variables correlated with risk
num_partners     = np.clip(rng.poisson(2 + 3 * (risk > 0.5), N), 0, 20).astype(float)
sti_history      = (rng.random(N) < 0.15 + 0.35 * (risk > 0.5)).astype(int)
substance_use    = (rng.random(N) < 0.10 + 0.40 * (risk > 0.5)).astype(int)
prep_use         = (rng.random(N) < 0.50 - 0.30 * (risk > 0.5)).astype(int)

hiv_prob         = 1 / (1 + np.exp(-(
    -3.5
    + 0.15 * num_partners
    + 1.2  * sti_history
    + 1.0  * substance_use
    - 1.8  * prep_use
    + 0.3  * (education_clean == 1)
)))
hiv_status = (rng.random(N) < hiv_prob).astype(int)

# inject mess
gender_mess = mess_case(["Male", "male", "M", "FEMALE", "Female", "F", "female"],
                         np.where(gender_clean == "male",
                                  rng.integers(0, 3, N),
                                  rng.integers(3, 7, N)))
# gender_mess built differently — redo properly
gender_variants_m = ["Male", "male", "M"]
gender_variants_f = ["FEMALE", "Female", "F", "female"]
gender_mess = np.where(
    gender_clean == "male",
    np.array(gender_variants_m)[rng.integers(0, 3, N)],
    np.array(gender_variants_f)[rng.integers(0, 4, N)]
)

race_pad   = np.where(rng.random(N) < 0.3, " " + race_clean + " ", race_clean)
educ_str   = education_clean.astype(str)  # stored as "1"/"2"/"3" strings

num_partners_mess = num_partners.copy().astype(object)
na_mask = rng.random(N) < 0.05
num_partners_mess[na_mask] = "N/A"

substance_variants_yes = ["Yes", "yes", "1"]
substance_variants_no  = ["No", "NO", "0"]
substance_mess = np.where(
    substance_use == 1,
    np.array(substance_variants_yes)[rng.integers(0, 3, N)],
    np.array(substance_variants_no)[rng.integers(0, 3, N)]
)

prep_mess = mess_yn(prep_use.astype(bool))

hiv_df = pd.DataFrame({
    "patient_id":    np.arange(1, N + 1),
    "gender":        gender_mess,
    "race_ethnicity": race_pad,
    "education":     educ_str,
    "num_partners":  num_partners_mess,
    "sti_history":   sti_history,
    "substance_use": substance_mess,
    "prep_use":      prep_mess,
    "hiv_status":    hiv_status,
})

Path("01_hiv_ml").mkdir(exist_ok=True)
hiv_df.to_csv("01_hiv_ml/hiv_raw.csv", index=False)
print(f"  HIV: {len(hiv_df):,} rows | {hiv_status.mean():.1%} HIV+")

# ── Project 2: Therapy Cohort ──────────────────────────────────────────────────
print("Generating therapy visit dataset...")

N_MEMBERS = 5_000
states_all = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY"
]
pilot_states    = ["AR", "GA"]
national_states = [s for s in states_all if s not in pilot_states]

# member-level attributes
member_ids      = np.arange(1, N_MEMBERS + 1)
member_therapy  = rng.choice(["CBT", "DBT", "Medication"], p=[0.40, 0.35, 0.25], size=N_MEMBERS)
member_age      = rng.integers(18, 76, N_MEMBERS)
member_gender   = rng.choice(["male", "female", "nonbinary"], p=[0.45, 0.50, 0.05], size=N_MEMBERS)

# pilot markets oversampled relative to population (small pilot)
pilot_weight = 0.08
market_pool  = (pilot_states * int(N_MEMBERS * pilot_weight) +
                national_states * int(N_MEMBERS * (1 - pilot_weight) / len(national_states)))
member_market = rng.choice(states_all, size=N_MEMBERS,
                            p=[0.04 if s in pilot_states else (0.92/48) for s in states_all])

# visits per member: CBT > DBT > Medication
visit_means = {"CBT": 14, "DBT": 10, "Medication": 5}
n_visits = np.array([max(1, int(rng.normal(visit_means[t], 3))) for t in member_therapy])
n_visits = np.clip(n_visits, 1, 40)

total_visits = n_visits.sum()

# expand to visit-level rows
member_col  = np.repeat(member_ids,   n_visits)
therapy_col = np.repeat(member_therapy, n_visits)
age_col     = np.repeat(member_age,   n_visits)
gender_col  = np.repeat(member_gender, n_visits)
market_col  = np.repeat(member_market, n_visits)

# visit cost: DBT most expensive, Medication cheapest
cost_means = {"CBT": 180, "DBT": 260, "Medication": 95}
visit_cost_clean = np.array([
    max(20, rng.normal(cost_means[t], 40))
    for t in therapy_col
])

# visit dates — spread over 2 years
base_date = pd.Timestamp("2023-01-01")
visit_dates_raw = base_date + pd.to_timedelta(rng.integers(0, 730, total_visits), unit="D")

# no-show: Medication highest, CBT lowest
no_show_probs = {"CBT": 0.08, "DBT": 0.12, "Medication": 0.20}
no_show_clean = np.array([rng.random() < no_show_probs[t] for t in therapy_col])

visit_type_clean = np.full(total_visits, "Follow-up")
# first visit per member is Initial
first_visit_idx = np.concatenate([[0], np.cumsum(n_visits)[:-1]])
visit_type_clean[first_visit_idx] = "Initial"

# ── inject mess ──────────────────────────────────────────────────────────────
therapy_variants = {
    "CBT":        ["CBT", "cbt", "Cbt"],
    "DBT":        ["DBT", "dbt", "dbt "],
    "Medication": ["Medication", "medication", "medication "],
}
therapy_mess = np.array([
    rng.choice(therapy_variants[t]) for t in therapy_col
])

# mixed date formats
fmt_choice = rng.integers(0, 3, total_visits)
def fmt_date(d, fmt):
    if fmt == 0: return d.strftime("%Y-%m-%d")
    if fmt == 1: return d.strftime("%m/%d/%Y")
    return d.strftime("%B %d, %Y")

visit_date_mess = np.array([fmt_date(d, f) for d, f in zip(visit_dates_raw, fmt_choice)])

# cost as "$1,234.56" string ~40% of rows
dollar_mask = rng.random(total_visits) < 0.40
visit_cost_mess = np.where(
    dollar_mask,
    ["${:,.2f}".format(c) for c in visit_cost_clean],
    ["{:.2f}".format(c) for c in visit_cost_clean]
)

# market case mess
market_case = rng.integers(0, 3, total_visits)
market_mess = np.where(market_case == 0, market_col,
              np.where(market_case == 1, np.array([m.lower() for m in market_col]),
                                         np.array([m.title() for m in market_col])))

gender_m_variants = ["Male", "male", "M"]
gender_f_variants = ["Female", "female", "F"]
gender_nb_variants = ["Nonbinary", "nonbinary", "Non-Binary"]
def mess_gender(g):
    if g == "male":     return rng.choice(gender_m_variants)
    if g == "female":   return rng.choice(gender_f_variants)
    return rng.choice(gender_nb_variants)
gender_mess2 = np.array([mess_gender(g) for g in gender_col])

visit_type_variants = {
    "Initial":   ["Initial", "initial", "INITIAL"],
    "Follow-up": ["Follow-up", "follow-up", "FOLLOW UP", "Follow Up"],
}
visit_type_mess = np.array([rng.choice(visit_type_variants[v]) for v in visit_type_clean])

no_show_mess = mess_yn(no_show_clean)

therapy_df = pd.DataFrame({
    "claim_id":    np.arange(1, total_visits + 1),
    "member_id":   member_col,
    "visit_date":  visit_date_mess,
    "therapy_type": therapy_mess,
    "visit_type":  visit_type_mess,
    "visit_cost":  visit_cost_mess,
    "age":         age_col,
    "gender":      gender_mess2,
    "market":      market_mess,
    "no_show":     no_show_mess,
})

# trim to ~50k if over
if len(therapy_df) > 50_000:
    therapy_df = therapy_df.sample(50_000, random_state=42).reset_index(drop=True)

Path("02_therapy_cohort").mkdir(exist_ok=True)
therapy_df.to_csv("02_therapy_cohort/therapy_raw.csv", index=False)
print(f"  Therapy: {len(therapy_df):,} rows | {N_MEMBERS:,} members | {therapy_df['market'].str.upper().isin(pilot_states).mean():.1%} pilot")

# ── Project 3: Fraud / Anomaly Detection ──────────────────────────────────────
print("Generating claims dataset...")

N_TINS = 500
N_CLAIMS = 50_000

specialties = ["Internal Medicine", "Family Medicine", "Cardiology",
                "Orthopedics", "Psychiatry", "Neurology", "Oncology", "Radiology"]
settings    = ["Outpatient", "Inpatient"]

# normal TINs
tin_base        = rng.integers(100_000_000, 999_999_999, N_TINS)
tin_claim_count = rng.integers(30, 200, N_TINS)

# inject 15 outlier TINs
outlier_tin_idx = rng.choice(N_TINS, 15, replace=False)
tin_claim_count[outlier_tin_idx] = rng.integers(400, 800, 15)  # extreme volume

# expand TINs to claim rows
tin_repeat = np.repeat(tin_base, tin_claim_count[:N_TINS])
# truncate/pad to exactly N_CLAIMS
if len(tin_repeat) > N_CLAIMS:
    tin_repeat = tin_repeat[:N_CLAIMS]
else:
    extra = N_CLAIMS - len(tin_repeat)
    tin_repeat = np.concatenate([tin_repeat, rng.choice(tin_base[:50], extra)])

n = len(tin_repeat)

# billed amounts — outlier TINs bill much more
is_outlier_tin = np.isin(tin_repeat, tin_base[outlier_tin_idx])
billed_clean = np.where(
    is_outlier_tin,
    rng.lognormal(7.5, 0.8, n),   # outlier: ~$1,800 median
    rng.lognormal(5.5, 0.6, n)    # normal:  ~$245 median
)

paid_clean = billed_clean * rng.uniform(0.6, 0.95, n)

# service dates
claim_dates_raw = base_date + pd.to_timedelta(rng.integers(0, 730, n), unit="D")

procedure_codes_clean = rng.choice(
    ["99213", "99214", "99232", "A0001", "G0008", "J1030", "27447", "70553"],
    size=n
)
specialty_clean  = rng.choice(specialties, size=n)
setting_clean    = rng.choice(settings, p=[0.75, 0.25], size=n)
member_ids_c     = rng.integers(1, 200_001, n)

# ── inject mess ──────────────────────────────────────────────────────────────
# TIN formatting
tin_fmt = rng.integers(0, 3, n)
tin_str = tin_repeat.astype(str)
tin_mess = np.where(tin_fmt == 0, tin_str,
           np.where(tin_fmt == 1, np.array([f"{t[:2]}-{t[2:]}" for t in tin_str]),
                                  np.array([f"{t[:3]} {t[3:]}" for t in tin_str])))

# date formats
date_fmt = rng.integers(0, 3, n)
claim_date_mess = np.array([fmt_date(d, f) for d, f in zip(claim_dates_raw, date_fmt)])

# dollar-format billed ~50%, paid has -999 sentinels ~4%
billed_dollar_mask = rng.random(n) < 0.50
billed_mess = np.where(
    billed_dollar_mask,
    ["${:,.2f}".format(v) for v in billed_clean],
    ["{:.2f}".format(v) for v in billed_clean]
)
paid_mess = paid_clean.copy().astype(object)
paid_mess[rng.random(n) < 0.04] = -999

# procedure code mess
proc_case = rng.integers(0, 3, n)
proc_mess = np.where(proc_case == 0, procedure_codes_clean,
            np.where(proc_case == 1, np.array([p.lower() for p in procedure_codes_clean]),
                                     np.array([p + " " for p in procedure_codes_clean])))

# specialty/setting mess
spec_mess = np.array([
    rng.choice([s, s.lower(), s.upper(), " " + s]) for s in specialty_clean
])
setting_variants = {
    "Outpatient": ["Outpatient", "outpatient", "OUTPATIENT"],
    "Inpatient":  ["Inpatient",  "inpatient",  "INPATIENT"],
}
setting_mess = np.array([rng.choice(setting_variants[s]) for s in setting_clean])

claims_df = pd.DataFrame({
    "claim_id":       np.arange(1, n + 1),
    "provider_tin":   tin_mess,
    "member_id":      member_ids_c,
    "service_date":   claim_date_mess,
    "billed_amount":  billed_mess,
    "paid_amount":    paid_mess,
    "procedure_code": proc_mess,
    "specialty":      spec_mess,
    "service_setting": setting_mess,
})

Path("03_fraud_anomaly").mkdir(exist_ok=True)
claims_df.to_csv("03_fraud_anomaly/claims_raw.csv", index=False)
print(f"  Claims: {len(claims_df):,} rows | {N_TINS} TINs | {len(outlier_tin_idx)} injected outlier TINs")
print(f"  Outlier TINs (ground truth): {sorted(tin_base[outlier_tin_idx].tolist())}")

# ── Project 4: Mental Health ───────────────────────────────────────────────────
print("Generating mental health screening dataset...")

N_STUDENTS   = 3_000
N_SCREENINGS = 50_000  # multiple screenings per student

student_ids      = np.arange(1, N_STUDENTS + 1)
student_gender   = rng.choice(["male", "female", "nonbinary"], p=[0.42, 0.50, 0.08], size=N_STUDENTS)
student_race     = rng.choice(["White", "Black", "Hispanic", "Asian", "Other"],
                               p=[0.45, 0.20, 0.18, 0.10, 0.07], size=N_STUDENTS)
student_gpa      = np.clip(rng.normal(3.0, 0.6, N_STUDENTS), 0.0, 4.0)
student_fin_stress = rng.choice(["High", "Medium", "Low"], p=[0.30, 0.45, 0.25], size=N_STUDENTS)
student_prior_c  = (rng.random(N_STUDENTS) < 0.35).astype(int)

# each student has 1–4 screenings
screenings_per = rng.integers(1, 5, N_STUDENTS)
# truncate so total ≈ N_SCREENINGS
while screenings_per.sum() > N_SCREENINGS:
    screenings_per = np.clip(screenings_per - 1, 1, 4)

total_screenings = screenings_per.sum()

student_col = np.repeat(student_ids,   screenings_per)
gender_col2 = np.repeat(student_gender, screenings_per)
race_col    = np.repeat(student_race,  screenings_per)
gpa_col     = np.repeat(student_gpa,   screenings_per)
fin_col     = np.repeat(student_fin_stress, screenings_per)
prior_col   = np.repeat(student_prior_c, screenings_per)

# screening dates
screen_dates_raw = base_date + pd.to_timedelta(rng.integers(0, 730, total_screenings), unit="D")

# PHQ-9 and GAD-7 item scores — correlated with GPA, financial stress
fin_numeric = np.where(fin_col == "High", 2, np.where(fin_col == "Medium", 1, 0))
severity    = (4 - gpa_col) * 0.5 + fin_numeric * 0.8 + prior_col * 0.5

phq_items = np.zeros((total_screenings, 9), dtype=int)
gad_items = np.zeros((total_screenings, 7), dtype=int)
for i in range(9):
    p = np.clip(0.05 + 0.08 * severity, 0, 0.85)
    phq_items[:, i] = rng.binomial(3, p)
for i in range(7):
    p = np.clip(0.05 + 0.07 * severity, 0, 0.80)
    gad_items[:, i] = rng.binomial(3, p)

phq_total = phq_items.sum(axis=1)
gad_total = gad_items.sum(axis=1)

# referral outcome driven by PHQ-9 + GAD-7 totals
ref_logit = -3.0 + 0.15 * phq_total + 0.10 * gad_total - 0.4 * gpa_col + 0.3 * fin_numeric
ref_prob_clinical = 1 / (1 + np.exp(-ref_logit))
ref_prob_peer     = np.clip(0.25 + 0.008 * gad_total - ref_prob_clinical * 0.5, 0, 1)
ref_prob_none     = np.clip(1 - ref_prob_clinical - ref_prob_peer, 0.05, 1)

# normalize
total_p = ref_prob_clinical + ref_prob_peer + ref_prob_none
ref_prob_clinical /= total_p
ref_prob_peer     /= total_p
ref_prob_none     /= total_p

referral_clean = np.array([
    rng.choice(["Clinical Referral", "Peer Support", "No Referral"],
               p=[ref_prob_clinical[i], ref_prob_peer[i], ref_prob_none[i]])
    for i in range(total_screenings)
])

# expand to item-level rows: 9 PHQ items + 7 GAD items = 16 rows per screening
screening_id_col = np.arange(1, total_screenings + 1)
rows = []
for s_idx in range(total_screenings):
    sid  = screening_id_col[s_idx]
    stud = student_col[s_idx]
    sdate = screen_dates_raw[s_idx]
    ref  = referral_clean[s_idx]
    g    = gender_col2[s_idx]
    r    = race_col[s_idx]
    gpa  = gpa_col[s_idx]
    fin  = fin_col[s_idx]
    pri  = prior_col[s_idx]
    for item_i in range(9):
        rows.append((sid, stud, sdate, "PHQ-9", item_i + 1, phq_items[s_idx, item_i],
                     g, r, gpa, fin, pri, ref))
    for item_i in range(7):
        rows.append((sid, stud, sdate, "GAD-7", item_i + 1, gad_items[s_idx, item_i],
                     g, r, gpa, fin, pri, ref))

mh_df = pd.DataFrame(rows, columns=[
    "screening_id", "student_id", "screening_date", "instrument", "item_num",
    "item_score", "gender", "race", "gpa", "financial_stress", "prior_counseling",
    "referral_outcome"
])

# trim to 50k rows (sample whole screenings to avoid partial)
if len(mh_df) > 50_000:
    keep_sids = rng.choice(screening_id_col, size=50_000 // 16, replace=False)
    mh_df = mh_df[mh_df["screening_id"].isin(keep_sids)].reset_index(drop=True)

# ── inject mess ──────────────────────────────────────────────────────────────
n_mh = len(mh_df)

instrument_variants = {
    "PHQ-9": ["PHQ-9", "phq9", "phq-9", "PHQ9"],
    "GAD-7": ["GAD-7", "gad7", "GAD7", "gad-7"],
}
mh_df["instrument"] = [rng.choice(instrument_variants[v]) for v in mh_df["instrument"]]

date_fmt_mh = rng.integers(0, 3, n_mh)
mh_df["screening_date"] = [
    fmt_date(d, f) for d, f in zip(mh_df["screening_date"], date_fmt_mh)
]

gender_nb_v2 = ["Nonbinary", "nonbinary", "Non-Binary"]
def mess_gender2(g):
    if g == "male":     return rng.choice(["Male", "male", "M"])
    if g == "female":   return rng.choice(["Female", "female", "F"])
    return rng.choice(gender_nb_v2)
mh_df["gender"] = [mess_gender2(g) for g in mh_df["gender"]]

mh_df["race"] = [
    rng.choice([r, r.lower(), r.upper(), " " + r + " "])
    for r in mh_df["race"]
]

fin_variants = {
    "High":   ["High", "high", "HIGH"],
    "Medium": ["Medium", "medium", "med", "Med"],
    "Low":    ["Low", "low", "LOW"],
}
mh_df["financial_stress"] = [rng.choice(fin_variants[v]) for v in mh_df["financial_stress"]]

prior_mess_vals = [mess_yn(np.array([bool(v)]))[0] for v in mh_df["prior_counseling"]]
mh_df["prior_counseling"] = prior_mess_vals

ref_variants = {
    "Clinical Referral": ["Clinical Referral", "clinical referral", "CLINICAL REFERRAL"],
    "Peer Support":      ["Peer Support", "peer support", "PEER SUPPORT"],
    "No Referral":       ["No Referral", "no referral", "NO REFERRAL"],
}
mh_df["referral_outcome"] = [rng.choice(ref_variants[v]) for v in mh_df["referral_outcome"]]

Path("04_mental_health_logit").mkdir(exist_ok=True)
mh_df.to_csv("04_mental_health_logit/mh_raw.csv", index=False)
print(f"  Mental health: {len(mh_df):,} rows | {mh_df['student_id'].nunique():,} students")
print(f"  Referral mix: {mh_df.drop_duplicates('screening_id')['referral_outcome'].str.lower().str.strip().value_counts(normalize=True).to_dict()}")

print("\nAll datasets generated.")
