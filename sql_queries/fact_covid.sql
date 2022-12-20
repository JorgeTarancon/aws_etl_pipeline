SELECT
    e.fips,
    e.province_state,
    e.country_region,
    e.confirmed,
    e.deaths,
    e.recovered,
    e.active,
    r.date,
    r.positive,
    r.negative,
    r.hospitalizedcurrently,
    r.hospitalized,
    r.hospitalizeddischarged
FROM
    covid_19.enigma_jhu e
        INNER JOIN covid_19.rearc_covid_19_testing_data_states_daily_states_daily r
        ON e.fips = r.fips