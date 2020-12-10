CREATE TABLE covid_data (
iso_code String,
continent String,
country String,
case_date DATE,
total_cases DOUBLE,
new_cases DOUBLE,
new_cases_smoothed DOUBLE,
total_deaths DOUBLE,
new_deaths DOUBLE,
new_deaths_smoothed DOUBLE, 
total_cases_per_million DOUBLE,
new_cases_per_million DOUBLE,
new_cases_smoothed_per_million DOUBLE,
total_deaths_per_million DOUBLE,
new_deaths_per_million DOUBLE,
new_deaths_smoothed_per_million DOUBLE,
reproduction_rate DOUBLE,
icu_patients DOUBLE,
icu_patients_per_million DOUBLE,
hosp_patients DOUBLE,
hosp_patients_per_million DOUBLE,
weekly_icu_admissions DOUBLE,
weekly_icu_admissions_per_million DOUBLE,
weekly_hosp_admissions DOUBLE,
weekly_hosp_admissions_per_million DOUBLE,
total_tests DOUBLE,
new_tests DOUBLE,
total_tests_per_thousand DOUBLE,
new_tests_per_thousand DOUBLE, 
new_tests_smoothed DOUBLE,
 new_tests_smoothed_per_thousand DOUBLE,
 positive_rate DOUBLE,
 tests_per_case DOUBLE,
 tests_units DOUBLE,
 stringency_index DOUBLE,
 population DOUBLE,
 population_density DOUBLE,
 median_age DOUBLE, 
 aged_65_older DOUBLE,
 aged_70_older DOUBLE,
 gdp_per_capita DOUBLE,
 extreme_poverty DOUBLE,
 cardiovasc_death_rate DOUBLE,
 diabetes_prevalence DOUBLE,
 female_smokers DOUBLE,
 male_smokers DOUBLE,
 handwashing_facilities DOUBLE,
 hospital_beds_per_thousand DOUBLE,
 life_expectancy DOUBLE,
 human_development_index DOUBLE) 
row format delimited fields terminated by ',' lines terminated by '\n' tblproperties("skip.header.line.count"="1");


-- Load Data into Table from Hive
LOAD DATA INPATH 'hdfs://localhost:9000/countryWiseDaily' OVERWRITE INTO TABLE covid_data;


-- Total Number of deaths country wise
select country, sum(new_deaths) from covid_data group by country;

-- Total Number of deaths per continent
select continent, sum(new_deaths) from covid_data group by continent;

--- Total Number of deaths Day of the Week Wise
select dayofweek(case_date),sum(new_cases) from covid_data group by dayofweek(case_date);


-- Medial age country, continent wise in Descending sorted manner
select distinct continent, country, median_age from covid_data sort by median_age desc, country, continent;

-- Create a rank function from jar using UDF Library
CREATE FUNCTION rankFunction AS 'com.function.Rank' USING JAR 'hdfs://localhost:9000/jarLib/rank-1.jar';

-- Using the rankFunction ( UDF ) defiled above, find the top 5 max deaths per day per country
SELECT * FROM (SELECT *, rankFunction(country) as row_number FROM ( SELECT country, new_deaths FROM covid_data WHERE country is NOT NULL DISTRIBUTE BY country SORT BY country, new_deaths desc) A) B WHERE row_number < 5 ORDER BY country, row_number ;

