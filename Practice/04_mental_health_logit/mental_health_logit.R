library(tidyverse)
library(janitor)
library(skimr)
library(lubridate)
library(nnet)
library(broom)

# ---- 1. Load ----------------------------------------------------------------

mh_raw <- read_csv("04_mental_health_logit/mh_raw.csv", show_col_types = FALSE)

glimpse(mh_raw)

# each row is one item response — 16 rows per screening (9 PHQ + 7 GAD)
mh_raw |> count(instrument)        # see the mess
mh_raw |> count(financial_stress)
mh_raw |> count(referral_outcome)
cat("Unique screenings:", n_distinct(mh_raw$screening_id), "\n")
cat("Unique students:",   n_distinct(mh_raw$student_id),   "\n")

# ---- 2. Clean ---------------------------------------------------------------

mh <- mh_raw |>
  clean_names() |>
  mutate(
    # instrument: normalize to PHQ9 / GAD7
    instrument = str_trim(str_to_upper(instrument)),
    instrument = case_when(
      str_detect(instrument, "PHQ") ~ "PHQ9",
      str_detect(instrument, "GAD") ~ "GAD7"
    ),
    # screening_date: mixed formats
    screening_date = parse_date_time(screening_date,
                                     orders = c("Ymd", "mdY", "BdY"),
                                     quiet  = TRUE),
    # gender
    gender = str_to_lower(str_trim(gender)),
    gender = case_when(
      gender %in% c("m", "male")   ~ "male",
      gender %in% c("f", "female") ~ "female",
      str_detect(gender, "nonbinary|non-binary") ~ "nonbinary"
    ),
    # race: trim whitespace, title case
    race = str_trim(str_to_title(race)),
    # financial_stress: normalize to High/Medium/Low
    financial_stress = str_trim(str_to_lower(financial_stress)),
    financial_stress = case_when(
      financial_stress %in% c("high")         ~ "High",
      financial_stress %in% c("medium", "med") ~ "Medium",
      financial_stress %in% c("low")           ~ "Low"
    ),
    financial_stress = factor(financial_stress, levels = c("Low", "Medium", "High")),
    # prior_counseling: Y/Yes → 1
    prior_counseling = str_to_lower(str_trim(prior_counseling)),
    prior_counseling = as.integer(prior_counseling %in% c("y", "yes")),
    # referral_outcome: normalize to title case
    referral_outcome = str_trim(str_to_title(referral_outcome))
  )

mh |> count(instrument)
mh |> count(financial_stress)
mh |> count(referral_outcome)

# ---- 3. Pivot wide + sum scores ---------------------------------------------

# one row per screening_id × instrument, with summed score
scores_wide <- mh |>
  group_by(screening_id, student_id, screening_date,
           gender, race, gpa, financial_stress, prior_counseling, referral_outcome,
           instrument) |>
  summarise(total_score = sum(item_score), .groups = "drop") |>
  pivot_wider(
    names_from  = instrument,
    values_from = total_score,
    names_prefix = ""
  ) |>
  rename(phq9_total = PHQ9, gad7_total = GAD7)

glimpse(scores_wide)
scores_wide |> summarise(n_na_phq = sum(is.na(phq9_total)),
                          n_na_gad = sum(is.na(gad7_total)))

# ---- 4. One row per student: keep most recent screening --------------------

student_df <- scores_wide |>
  group_by(student_id) |>
  slice_max(screening_date, n = 1, with_ties = FALSE) |>
  ungroup()

cat("Students after deduplication:", nrow(student_df), "\n")

# ---- 5. EDA -----------------------------------------------------------------

skimr::skim(student_df |> select(phq9_total, gad7_total, gpa))

# score distributions by referral outcome — should show clear separation
student_df |>
  pivot_longer(c(phq9_total, gad7_total), names_to = "instrument", values_to = "score") |>
  ggplot(aes(x = score, fill = referral_outcome)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~instrument, scales = "free_x") +
  labs(title = "Score Distributions by Referral Outcome",
       x = "Total Score", fill = NULL) +
  theme_minimal()

# outcome breakdown
student_df |>
  count(referral_outcome) |>
  mutate(pct = n / sum(n))

# ---- 6. Fit multinomial logistic regression --------------------------------

# set reference level
student_df <- student_df |>
  mutate(referral_outcome = factor(referral_outcome,
                                    levels = c("No Referral", "Peer Support", "Clinical Referral")))

set.seed(42)
mn_fit <- multinom(
  referral_outcome ~ phq9_total + gad7_total + gpa + financial_stress +
                     prior_counseling + gender,
  data  = student_df,
  trace = FALSE,
  maxit = 300
)

summary(mn_fit)

# ---- 7. Tidy output with odds ratios ----------------------------------------

tidy_fit <- tidy(mn_fit, conf.int = TRUE, exponentiate = TRUE)

tidy_fit |>
  filter(term != "(Intercept)") |>
  arrange(y.level, desc(abs(log(estimate))))

# ---- 8. Coefficient plot ----------------------------------------------------

tidy_fit |>
  filter(term != "(Intercept)") |>
  ggplot(aes(x = estimate, xmin = conf.low, xmax = conf.high,
             y = reorder(term, estimate), color = y.level)) +
  geom_pointrange(position = position_dodge(width = 0.5)) +
  geom_vline(xintercept = 1, linetype = "dashed", color = "grey50") +
  scale_x_log10() +
  labs(
    title = "Odds Ratios: Multinomial Logistic Regression",
    subtitle = "Reference: No Referral",
    x = "Odds Ratio (log scale)", y = NULL, color = "Outcome"
  ) +
  theme_minimal()

# ---- 9. Predicted probabilities ---------------------------------------------

pred_probs <- predict(mn_fit, type = "probs") |>
  as_tibble() |>
  bind_cols(student_df |> select(phq9_total, gad7_total, financial_stress))

# bin PHQ-9 into risk groups for visualization
pred_probs <- pred_probs |>
  mutate(phq9_group = cut(phq9_total,
                           breaks = c(-1, 4, 9, 14, 19, 27),
                           labels = c("Minimal\n(0-4)", "Mild\n(5-9)",
                                      "Moderate\n(10-14)", "Mod-Severe\n(15-19)",
                                      "Severe\n(20-27)")))

# mean predicted probability per PHQ-9 group
pred_probs |>
  group_by(phq9_group) |>
  summarise(across(c(`No Referral`, `Peer Support`, `Clinical Referral`), mean)) |>
  pivot_longer(-phq9_group, names_to = "outcome", values_to = "prob") |>
  mutate(outcome = factor(outcome, levels = c("No Referral", "Peer Support", "Clinical Referral"))) |>
  ggplot(aes(x = phq9_group, y = prob, fill = outcome)) +
  geom_col(position = "stack") +
  scale_y_continuous(labels = scales::percent) +
  labs(
    title = "Predicted Referral Probabilities by PHQ-9 Severity",
    x = "PHQ-9 Risk Group", y = "Predicted Probability", fill = NULL
  ) +
  theme_minimal()
