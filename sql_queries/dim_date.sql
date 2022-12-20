SELECT
    fips,
    date_parse(CAST(date as varchar),'%Y%c%d') AS date,
    year( date_parse(CAST(date as varchar),'%Y%c%d') ) AS year,
    month( date_parse(CAST(date as varchar),'%Y%c%d') ) AS month,
    day( date_parse(CAST(date as varchar),'%Y%c%d') ) AS day_of_week
FROM
    covid_19.rearc_covid_19_testing_data_states_daily_states_daily