SELECT
    e.fips,
    e.province_state,
    e.country_region,
    e.latitude,
    e.longitude,
    n.county,
    n.state
FROM
    covid_19.enigma_jhu e
        INNER JOIN covid_19.nytimes_data_in_usa_us_county n
        ON e.fips = n.fips