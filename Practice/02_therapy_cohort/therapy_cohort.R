library(tidyverse)
library(janitor)
library(skimr)
library(lubridate)

# ---- 1. Load ----------------------------------------------------------------

therapy_raw <- read_csv("02_therapy_cohort/therapy_raw.csv", show_col_types = FALSE)

glimpse(therapy_raw)
therapy_raw |> count(therapy_type)      # see the mess
therapy_raw |> count(market) |> print(n = Inf)
therapy_raw |> count(visit_type)
therapy_raw |> count(no_show)

# ---- 2. Clean ---------------------------------------------------------------

therapy <- therapy_raw |>
  clean_names() |>
  mutate(
    # therapy_type: strip whitespace, lowercase, recode
    therapy_type = str_trim(str_to_lower(therapy_type)),
    therapy_type = case_when(
      therapy_type == "cbt"        ~ "CBT",
      therapy_type == "dbt"        ~ "DBT",
      therapy_type == "medication" ~ "Medication"
    ),
    # visit_type: normalize to Initial / Follow-up
    visit_type = str_trim(str_to_lower(visit_type)),
    visit_type = case_when(
      str_detect(visit_type, "initial")  ~ "Initial",
      str_detect(visit_type, "follow")   ~ "Follow-up"
    ),
    # visit_cost: strip "$" and "," then parse as numeric
    visit_cost = parse_number(visit_cost),
    # visit_date: mixed formats — let lubridate try common orders
    visit_date = parse_date_time(visit_date,
                                 orders = c("Ymd", "mdY", "BdY"),
                                 quiet  = TRUE),
    # market: uppercase and trim
    market = str_to_upper(str_trim(market)),
    # gender: lowercase, recode
    gender = str_to_lower(str_trim(gender)),
    gender = case_when(
      gender %in% c("m", "male")       ~ "male",
      gender %in% c("f", "female")     ~ "female",
      str_detect(gender, "nonbinary|non-binary") ~ "nonbinary"
    ),
    # no_show: Y/Yes/yes → TRUE
    no_show = str_to_lower(str_trim(no_show)) %in% c("y", "yes")
  )

# spot-check
therapy |> count(therapy_type)
therapy |> count(visit_type)
therapy |> count(gender)
therapy |> summarise(n_na_date = sum(is.na(visit_date)),
                     n_na_cost = sum(is.na(visit_cost)))

# ---- 3. Aggregate to member level -------------------------------------------

member_df <- therapy |>
  group_by(member_id, therapy_type, age, gender, market) |>
  summarise(
    total_visits      = n(),
    total_cost        = sum(visit_cost, na.rm = TRUE),
    no_show_count     = sum(no_show, na.rm = TRUE),
    first_visit       = min(visit_date, na.rm = TRUE),
    last_visit        = max(visit_date, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    avg_cost_per_visit = total_cost / total_visits,
    # age groups — you define the breaks
    age_group = cut(age,
                    breaks = c(17, 34, 54, Inf),
                    labels = c("18-34", "35-54", "55+"),
                    right  = TRUE),
    # pilot vs national
    program_group = if_else(market %in% c("AR", "GA"), "Pilot", "National"),
    # crude re-engagement flag: any visit within 30 days of last visit
    # (not perfect but reasonable from aggregated data)
    tenure_days = as.numeric(last_visit - first_visit)
  )

glimpse(member_df)
member_df |> count(age_group)
member_df |> count(program_group)

# ---- 4. Validation checkpoint -----------------------------------------------

# row count and distinct members
cat("Member rows:", nrow(member_df), "\n")
cat("Distinct member IDs:", n_distinct(member_df$member_id), "\n")

# null check
member_df |> summarise(across(everything(), ~sum(is.na(.))))

# ---- 5. Cohort profiles -----------------------------------------------------

# age distribution by cohort
member_df |>
  group_by(therapy_type, age_group) |>
  summarise(n = n(), .groups = "drop") |>
  mutate(pct = n / sum(n), .by = therapy_type)

# gender mix by cohort
member_df |>
  count(therapy_type, gender) |>
  mutate(pct = n / sum(n), .by = therapy_type)

# pilot vs national breakdown by cohort
member_df |>
  count(therapy_type, program_group) |>
  mutate(pct = n / sum(n), .by = therapy_type)

# ---- 6. Utilization comparison ----------------------------------------------

# summary stats per cohort
member_df |>
  group_by(therapy_type) |>
  summarise(
    n             = n(),
    mean_visits   = mean(total_visits),
    median_visits = median(total_visits),
    mean_cost     = mean(total_cost),
    mean_cpv      = mean(avg_cost_per_visit)
  )

# total cost distribution by cohort
ggplot(member_df, aes(x = therapy_type, y = total_cost, fill = therapy_type)) +
  geom_boxplot(outlier.alpha = 0.3, show.legend = FALSE) +
  scale_y_log10(labels = scales::dollar) +
  labs(title = "Total Cost by Therapy Type",
       x = NULL, y = "Total Cost (log scale)") +
  theme_minimal()

# visit count distribution
ggplot(member_df, aes(x = total_visits, fill = therapy_type)) +
  geom_histogram(bins = 30, alpha = 0.7, position = "identity") +
  facet_wrap(~therapy_type, ncol = 1) +
  labs(title = "Visit Count Distribution by Cohort", x = "Total Visits", y = "Count") +
  theme_minimal() +
  theme(legend.position = "none")

# avg cost per visit — boxplot
ggplot(member_df, aes(x = therapy_type, y = avg_cost_per_visit, fill = therapy_type)) +
  geom_boxplot(show.legend = FALSE) +
  scale_y_continuous(labels = scales::dollar) +
  labs(title = "Avg Cost per Visit by Therapy Type", x = NULL, y = "Avg Cost / Visit") +
  theme_minimal()

# ---- 7. Pilot vs National ---------------------------------------------------

# no-show rate by program group and cohort
therapy |>
  group_by(therapy_type, program_group = if_else(market %in% c("AR", "GA"), "Pilot", "National")) |>
  summarise(
    total_visits  = n(),
    no_show_rate  = mean(no_show, na.rm = TRUE),
    .groups = "drop"
  )

# cost comparison: Pilot vs National
member_df |>
  group_by(therapy_type, program_group) |>
  summarise(
    n          = n(),
    mean_cost  = mean(total_cost),
    mean_visits = mean(total_visits),
    .groups = "drop"
  )

# bar chart: mean visits by cohort × program group
member_df |>
  group_by(therapy_type, program_group) |>
  summarise(mean_visits = mean(total_visits), .groups = "drop") |>
  ggplot(aes(x = therapy_type, y = mean_visits, fill = program_group)) +
  geom_col(position = "dodge") +
  labs(title = "Mean Visits by Therapy Type and Program Group",
       x = NULL, y = "Mean Total Visits", fill = NULL) +
  theme_minimal()
