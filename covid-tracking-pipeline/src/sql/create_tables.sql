-- CREATE TABLE IF NOT EXISTS covid_daily (
--     date DATE PRIMARY KEY,
--     positive INTEGER,
--     negative INTEGER,
--     hospitalized_currently INTEGER,
--     death INTEGER,
--     total_test_results INTEGER,
--     data_quality_grade TEXT
-- );
CREATE TABLE IF NOT EXISTS covid_daily (
    cdc_case_earliest_dt DATE PRIMARY KEY,
    cdc_report_dt DATE,
    pos_spec_dt DATE,
    current_status TEXT,
    sex TEXT,
    age_group TEXT,
    race_ethnicity_combined TEXT,
    hosp_yn TEXT,
    icu_yn TEXT,
    death_yn TEXT,
    medcond_yn TEXT
);