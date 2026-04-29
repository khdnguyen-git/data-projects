library(tidyverse)
library(janitor)
library(skimr)
library(rsample)
library(glmnet)
library(ranger)
library(xgboost)
library(pROC)

# ---- 1. Load ----------------------------------------------------------------

hiv_raw <- read_csv("01_hiv_ml/hiv_raw.csv", show_col_types = FALSE)

glimpse(hiv_raw)
skimr::skim(hiv_raw)

# check class balance before touching anything
hiv_raw |> count(hiv_status)

# ---- 2. Clean ---------------------------------------------------------------

hiv <- hiv_raw |>
  clean_names() |>
  mutate(
    # standardize gender to male/female
    gender = str_to_lower(str_trim(gender)),
    gender = case_when(
      gender %in% c("m", "male")   ~ "male",
      gender %in% c("f", "female", "female") ~ "female",
      .default = NA_character_
    ),
    # strip whitespace from race
    race_ethnicity = str_trim(race_ethnicity),
    # education came in as "1"/"2"/"3" strings
    education = as.integer(education),
    education = factor(education, levels = 1:3,
                       labels = c("less_than_hs", "hs_diploma", "college_plus")),
    # num_partners has "N/A" strings
    num_partners = na_if(num_partners, "N/A"),
    num_partners = as.numeric(num_partners),
    # substance_use: "Yes"/"yes"/"1" → 1, else 0
    substance_use = case_when(
      str_to_lower(substance_use) %in% c("yes", "1") ~ 1L,
      str_to_lower(substance_use) %in% c("no",  "0") ~ 0L
    ),
    # prep_use: Y/Yes/yes → 1
    prep_use = case_when(
      str_to_lower(prep_use) %in% c("y", "yes") ~ 1L,
      str_to_lower(prep_use) %in% c("n", "no")  ~ 0L
    ),
    # outcome as factor for modeling
    hiv_status = factor(hiv_status, levels = c(0, 1), labels = c("negative", "positive"))
  )

# spot-check cleaning results
hiv |> count(gender)
hiv |> count(education)
hiv |> count(substance_use)
hiv |> summarise(n_na_partners = sum(is.na(num_partners)))

# ---- 3. EDA -----------------------------------------------------------------

skimr::skim(hiv)

# class balance after cleaning
hiv |> count(hiv_status) |> mutate(pct = n / sum(n))

# risk factor rates by HIV status — should show clear signal
hiv |>
  group_by(hiv_status) |>
  summarise(
    pct_substance  = mean(substance_use, na.rm = TRUE),
    pct_sti        = mean(sti_history),
    pct_no_prep    = mean(prep_use == 0, na.rm = TRUE),
    mean_partners  = mean(num_partners, na.rm = TRUE)
  )

# ---- 4. Feature prep --------------------------------------------------------

# impute median for num_partners (5% missing)
median_partners <- median(hiv$num_partners, na.rm = TRUE)

hiv_model <- hiv |>
  mutate(
    num_partners = coalesce(num_partners, median_partners),
    gender        = factor(gender),
    race_ethnicity = factor(race_ethnicity)
  ) |>
  drop_na()

# 70/30 train/test split, stratified on outcome
set.seed(42)
split  <- initial_split(hiv_model, prop = 0.70, strata = hiv_status)
train  <- training(split)
test   <- testing(split)

# model matrix (glmnet needs numeric matrix, no intercept)
x_train <- model.matrix(hiv_status ~ . - patient_id, data = train)[, -1]
y_train <- ifelse(train$hiv_status == "positive", 1, 0)

x_test  <- model.matrix(hiv_status ~ . - patient_id, data = test)[, -1]
y_test  <- ifelse(test$hiv_status == "positive", 1, 0)

# ---- 5. LASSO ---------------------------------------------------------------

set.seed(42)
lasso_cv  <- cv.glmnet(x_train, y_train, family = "binomial", alpha = 1, nfolds = 5)

plot(lasso_cv)
# best lambda
lasso_cv$lambda.min

lasso_coefs <- coef(lasso_cv, s = "lambda.min") |>
  as.matrix() |>
  as.data.frame() |>
  rownames_to_column("variable") |>
  rename(coefficient = s1) |>
  filter(variable != "(Intercept)", coefficient != 0) |>
  arrange(desc(abs(coefficient)))

lasso_coefs

# LASSO test AUC
lasso_pred <- predict(lasso_cv, newx = x_test, s = "lambda.min", type = "response")[, 1]
lasso_auc  <- auc(roc(y_test, lasso_pred, quiet = TRUE))
cat("LASSO AUC:", round(lasso_auc, 3), "\n")

# ---- 6. Random Forest -------------------------------------------------------

set.seed(42)
rf_fit <- ranger(
  hiv_status ~ . - patient_id,
  data            = train,
  num.trees       = 500,
  importance      = "impurity",
  probability     = TRUE,
  classification  = FALSE
)

rf_importance <- tibble(
  variable   = names(rf_fit$variable.importance),
  importance = rf_fit$variable.importance
) |>
  arrange(desc(importance))

rf_importance

# RF test AUC
rf_pred <- predict(rf_fit, data = test)$predictions[, "positive"]
rf_auc  <- auc(roc(y_test, rf_pred, quiet = TRUE))
cat("Random Forest AUC:", round(rf_auc, 3), "\n")

# ---- 7. XGBoost -------------------------------------------------------------

dtrain <- xgb.DMatrix(x_train, label = y_train)
dtest  <- xgb.DMatrix(x_test,  label = y_test)

set.seed(42)
xgb_fit <- xgboost(
  data      = dtrain,
  nrounds   = 200,
  objective = "binary:logistic",
  eval_metric = "auc",
  eta       = 0.05,
  max_depth = 4,
  subsample = 0.8,
  verbose   = 0
)

xgb_importance <- xgb.importance(model = xgb_fit) |>
  as_tibble() |>
  arrange(desc(Gain))

xgb_importance

# XGBoost test AUC
xgb_pred <- predict(xgb_fit, dtest)
xgb_auc  <- auc(roc(y_test, xgb_pred, quiet = TRUE))
cat("XGBoost AUC:", round(xgb_auc, 3), "\n")

# ---- 8. Compare models ------------------------------------------------------

# combined importance — normalize each to 0-1 for fair comparison
importance_combined <- bind_rows(
  lasso_coefs |>
    transmute(variable, importance = abs(coefficient) / max(abs(coefficient)), model = "LASSO"),
  rf_importance |>
    transmute(variable, importance = importance / max(importance), model = "Random Forest"),
  xgb_importance |>
    transmute(variable = Feature, importance = Gain / max(Gain), model = "XGBoost")
) |>
  # keep top 10 features per model
  group_by(model) |>
  slice_max(importance, n = 10) |>
  ungroup()

ggplot(importance_combined, aes(x = importance, y = reorder(variable, importance), fill = model)) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~model, scales = "free_x") +
  labs(title = "Variable Importance by Model", x = "Normalized Importance", y = NULL) +
  theme_minimal()

# AUC comparison table
tibble(
  model = c("LASSO", "Random Forest", "XGBoost"),
  auc   = round(c(lasso_auc, rf_auc, xgb_auc), 3)
) |>
  arrange(desc(auc))
