import pandas as pd
from rapidfuzz import fuzz

# Load CSV
file_path = r"C:\Users\knguy139\Documents\Projects\Data\Output\KN_MCR_SS_JOIN_MMID_202509221642.csv"
df = pd.read_csv(file_path)

# Perform fuzzy matching and grading
matches = []
for idx, row in df.iterrows():
    work_id = str(row['work_item_id'])
    site_id = str(row['site_clm_aud_nbr'])
    score = fuzz.partial_ratio(work_id, site_id)

    if score >= 60:
        if score >= 90:
            grade = 'grade_4'
        elif score >= 80:
            grade = 'grade_3'
        elif score >= 70:
            grade = 'grade_2'
        else:
            grade = 'grade_1'

        matches.append({
            'work_item_id': work_id,
            'site_clm_aud_nbr': site_id,
            'match_score': score,
            'grade': grade
        })

# Convert to DataFrame
matched_df = pd.DataFrame(matches)

# Export to CSV
output_path = r"C:\Users\knguy139\Documents\Projects\Data\Output\matched_results_with_grades.csv"
matched_df.to_csv(output_path, index=False)
