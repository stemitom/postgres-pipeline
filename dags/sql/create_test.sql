DROP IF EXISTS TABLE covid_test

CREATE TABLE IF NOT EXISTS covid_test (
    date DATE PRIMARY KEY,
    case_count INT
)