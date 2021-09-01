CREATE TABLE IF NOT EXISTS covid_data (
    date DATE PRIMARY KEY,
    case_count INT,
    hospitalized_count INT,
    death_count INT
);

TRUNCATE TABLE covid_data;
